package s3

import "sync"

//Writer  represents a bytes writer at
type Writer struct {
	mutex    *sync.Mutex
	bytes    []byte
	position int
	size     int
}

//Reset resets writer
func (w *Writer) Reset() {
	w.size = 0
	w.position = 0
}

func (w *Writer) Bytes() []byte {
	return w.bytes[:w.size]
}

//WriteAt returns number of written bytes or error
func (w *Writer) WriteAt(p []byte, offset int64) (n int, err error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	if int(offset)+len(p) > w.size {
		w.size = int(offset) + len(p)
	}
	newAlloc := w.size - len(w.bytes)
	if newAlloc > 0 {
		w.bytes = append(w.bytes, make([]byte, newAlloc)...)
	}
	return copy(w.bytes[offset:int(offset)+len(p)], p), nil
}

//NewWriter returns a writer
func NewWriter(initSize int) *Writer {
	return &Writer{
		mutex: &sync.Mutex{},
		bytes: make([]byte, initSize),
	}
}
