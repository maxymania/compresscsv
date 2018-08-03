/*
Copyright (c) 2018 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


package compresscsv

import "encoding/csv"
import xdr "github.com/davecgh/go-xdr/xdr2"
import "bytes"
import "github.com/RoaringBitmap/roaring"
import "compress/flate"
import "github.com/andrew-d/lzma"
import "github.com/dsnet/compress/bzip2"
import "github.com/pierrec/lz4"
import "strconv"
import "io"

func strfnv(s string) (r uint64) {
	r = 14695981039346656037
	const p = 1099511628211
	for _,b := range []byte(s) {
		r ^= uint64(b)
		r *= p
	}
	return
}

type Format uint32
const (
	// Fast, good compression ratio.
	F_Bzip2 Format = iota
	
	// Slow, better compression ratio.
	F_Lzma
	
	// Fast, poor compression ratio.
	F_Flate
	
	F_Lz4
)

type Flags uint
const (
	MB_Nullable Flags = 1<<iota
	MB_Integer
	MB_Boolean
	MB_Single
)

type MatrixBuffer struct{
	Raw [][]string
	Flags []Flags
	MaxRows int
	Prefered Format
}
func (m *MatrixBuffer) Init(columns,rows int) {
	m.Raw = make([][]string,columns)
	m.Flags = make([]Flags,columns)
	m.MaxRows = rows
	for i := range m.Raw {
		m.Raw[i] = make([]string,0,rows)
	}
}
func (m *MatrixBuffer) Reset() {
	for i := range m.Raw {
		m.Raw[i] = m.Raw[i][:0]
		m.Flags[i] = 0
	}
}
func (m *MatrixBuffer) ReadCsv(r *csv.Reader) (err error) {
	rrbak := r.ReuseRecord
	r.ReuseRecord = true
	var row []string
	for {
		if len(m.Raw[0])>=m.MaxRows { break }
		row,err = r.Read()
		if err!=nil { break }
		for i,cell := range row {
			m.Raw[i] = append(m.Raw[i],cell)
		}
	}
	r.ReuseRecord = rrbak
	return
}
func (m *MatrixBuffer) analyzeColumn(col int) {
	const nHashes = 4
	var hashes [nHashes]uint64
	var strvss [nHashes]string
	allFlags := MB_Integer
	cnt,all := uint(0),uint(len(m.Raw[col]))
	for _,cell := range m.Raw[col] {
		if len(cell)==0 {
			cnt++
		}
		h := strfnv(cell)
		if h==0 { h++ }
		for i := range hashes {
			if hashes[i]==0 {
				hashes[i] = h
				break
			}
			if hashes[i]==h {
				/* Check for the case, when we have a hash collision. */
				if strvss[i]==cell { break }
			}
		}
		noCellFlags := Flags(0)
		for _,b := range []byte(cell) {
			if b<'0' || b>'9' { noCellFlags |= MB_Integer }
		}
		if len(cell)>1 {
			if cell[0]=='0' { noCellFlags |= MB_Integer }
		}
		if len(cell)!=0 {
			_,err := strconv.ParseUint(cell,10,64)
			if err!=nil { noCellFlags |= MB_Integer }
		}
		allFlags &= ^noCellFlags
	}
	if (cnt*2)>(all) { allFlags |= MB_Nullable }
	if hashes[2]==0 && hashes[1]!=0 { allFlags |= MB_Boolean }
	if hashes[1]==0 && hashes[0]!=0 { allFlags |= MB_Single }
	m.Flags[col] = allFlags
}
func (m *MatrixBuffer) Analyze() {
	for i := range m.Raw { m.analyzeColumn(i) }
}
func (m *MatrixBuffer) getBoolHead(col int) (r [2]string) {
	var c [2]uint
	j := 0
	for _,v := range m.Raw[col] {
		switch j {
		case 0:
			c[0]++
			r[0] = v
			j = 1
		case 1:
			if r[0]==v {
				c[0]++
			} else {
				c[1]++
				r[1] = v
				j = 2
			}
		case 2:
			switch {
			case r[0]==v: c[0]++
			case r[1]==v: c[1]++
			}
		}
	}
	if c[0] < c[1] {
		r[0],r[1] = r[1],r[0]
	}
	return
}
func (m *MatrixBuffer) columnEncode(buf *bytes.Buffer,col int,enc *xdr.Encoder) (err error) {
	f := m.Flags[col]
	_,err = enc.EncodeUint(uint32(f)) ; if err!=nil { return }
	if (f&MB_Boolean)!=0 {
		vals := m.getBoolHead(col)
		_,err = enc.EncodeString(vals[0]) ; if err!=nil { return }
		_,err = enc.EncodeString(vals[1]) ; if err!=nil { return }
		bm := roaring.New()
		for i,str := range m.Raw[col] {
			if str==vals[1] {
				bm.Add(uint32(i))
			}
		}
		bm.RunOptimize()
		var data []byte
		data,err = bm.ToBytes() ; if err!=nil { return }
		_,err = enc.EncodeOpaque(data) ; if err!=nil { return }
		return
	}
	var nmap *roaring.Bitmap
	if (f&MB_Nullable)!=0 {
		nmap = roaring.New()
		for i,str := range m.Raw[col] {
			if len(str)!=0 {
				nmap.Add(uint32(i))
			}
		}
		nmap.RunOptimize()
		var data []byte
		data,err = nmap.ToBytes() ; if err!=nil { return }
		_,err = enc.EncodeOpaque(data) ; if err!=nil { return }
	}
	format := m.Prefered
	_,err = enc.EncodeUint(uint32(format)) ; if err!=nil { return }
	
	buf.Reset()
	var wr io.WriteCloser
	
	switch format {
	case F_Bzip2: wr,_ = bzip2.NewWriter(buf,&bzip2.WriterConfig{Level:9})
	case F_Lzma : wr = lzma.NewWriterLevel(buf,9)
	case F_Flate: wr,_ = flate.NewWriter(buf, 9)
	case F_Lz4  :
		xwr := lz4.NewWriter(buf)
		xwr.CompressionLevel = 9
		wr = xwr
	default: panic("unknown format")
	}
	
	compenc := xdr.NewEncoder(wr)
	
	raw := m.Raw[col]
	
	if (f&MB_Integer)!=0 {
		var val uint64
		if nmap!=nil {
			iter := nmap.Iterator()
			for iter.HasNext() {
				i := iter.Next()
				val,err = strconv.ParseUint(raw[i],10,64) ; if err!=nil { return }
				//err = wr.FlushInner() ; if err!=nil { return }
				_,err = compenc.EncodeUhyper(val) ; if err!=nil { return }
			}
		} else {
			for _,cell := range raw {
				val,err = strconv.ParseUint(cell,10,32) ; if err!=nil { return }
				//err = wr.FlushInner() ; if err!=nil { return }
				_,err = compenc.EncodeUhyper(val) ; if err!=nil { return }
			}
		}
	} else {
		if nmap!=nil {
			iter := nmap.Iterator()
			for iter.HasNext() {
				i := iter.Next()
				//err = wr.FlushInner() ; if err!=nil { return }
				_,err = compenc.EncodeString(raw[i]) ; if err!=nil { return }
			}
		} else {
			for _,cell := range raw {
				//err = wr.FlushInner() ; if err!=nil { return }
				_,err = compenc.EncodeString(cell) ; if err!=nil { return }
			}
		}
	}
	err = wr.Close() ; if err!=nil { return }
	
	_,err = enc.EncodeOpaque(buf.Bytes()) ; if err!=nil { return }
	
	return
}
func (m *MatrixBuffer) Encode(enc *xdr.Encoder) (err error) {
	buf := new(bytes.Buffer)
	for i := range m.Raw {
		err = m.columnEncode(buf,i,enc)
		if err!=nil { return }
	}
	return
}


