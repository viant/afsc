package assets

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
)

func (s *Storager) newRequest(ctx context.Context, method, p string, body io.Reader) (*http.Request, error) {
	u, err := url.Parse(s.baseURL)
	if err != nil {
		return nil, err
	}
	u.Path = path.Join(u.Path, p)
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	if s.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+s.apiKey)
	}
	if s.organization != "" {
		req.Header.Set("OpenAI-Organization", s.organization)
	}
	return req, nil
}

func (s *Storager) doJSON(ctx context.Context, method, p string, body io.Reader, v interface{}) error {
	req, err := s.newRequest(ctx, method, p, body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("openai api error: %s: %s", resp.Status, bytes.TrimSpace(data))
	}
	if v == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

func (s *Storager) doMultipart(ctx context.Context, p string, fields map[string]string, fileField, filename string, content []byte) error {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for k, v := range fields {
		_ = mw.WriteField(k, v)
	}
	fw, err := mw.CreateFormFile(fileField, filename)
	if err != nil {
		return err
	}
	if _, err = fw.Write(content); err != nil {
		return err
	}
	if err = mw.Close(); err != nil {
		return err
	}
	req, err := s.newRequest(ctx, http.MethodPost, p, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("openai api error: %s: %s", resp.Status, bytes.TrimSpace(data))
	}
	return nil
}

func (s *Storager) doRaw(ctx context.Context, method, p string, body io.Reader) ([]byte, error) {
	req, err := s.newRequest(ctx, method, p, body)
	if err != nil {
		return nil, err
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("openai api error: %s: %s", resp.Status, bytes.TrimSpace(data))
	}
	return data, nil
}
