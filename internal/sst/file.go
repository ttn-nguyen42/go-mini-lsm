package sst

import (
	"os"
)

type FileObject struct {
	f *os.File
	p string
	n int
}

func Write(data []byte, path string) (*FileObject, error) {
	fo := FileObject{p: path}

	// Use 0644 for normal file creation permissions instead of os.ModeAppend
	if err := os.WriteFile(fo.p, data, 0744); err != nil {
		return nil, err
	}

	f, err := os.Open(fo.p)
	if err != nil {
		return nil, err
	}
	fo.n = len(data)
	fo.f = f

	return &fo, nil
}

func Read(path string) (*FileObject, error) {
	fo := FileObject{}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	stats, err := f.Stat()
	if err != nil {
		return nil, err
	}

	fo.f = f
	fo.p = path
	fo.n = int(stats.Size())

	return &fo, nil
}

func (o *FileObject) Size() int {
	return o.n
}

func (o *FileObject) Close() error {
	return o.f.Close()
}

func (o *FileObject) Read(data []byte) (int, error) {
	return o.f.Read(data)
}

func (o *FileObject) ReadAt(buf []byte, offset int64) (int, error) {
	return o.f.ReadAt(buf, offset)
}
