package assets

import "time"

// ListResponse represents OpenAI list response
type ListResponse struct {
	Object string `json:"object"`
	Data   []File `json:"data"`
}

// File represents OpenAI file object
type File struct {
	ID        string `json:"id"`
	Object    string `json:"object"`
	Bytes     int64  `json:"bytes"`
	CreatedAt int64  `json:"created_at"`
	Filename  string `json:"filename"`
	Purpose   string `json:"purpose"`
}

func (f File) ModTime() time.Time {
	if f.CreatedAt == 0 {
		return time.Now()
	}
	return time.Unix(f.CreatedAt, 0)
}

// DeleteResponse represents OpenAI delete response
type DeleteResponse struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Deleted bool   `json:"deleted"`
}
