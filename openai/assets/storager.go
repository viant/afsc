package assets

import (
	"context"
	"net/http"
	"os"

	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	afsurl "github.com/viant/afs/url"
)

const (
	apiKeyEnv   = "OPENAI_API_KEY"
	defaultBase = "https://api.openai.com/v1"
)

// Config holds OpenAI access configuration.
type Config struct {
	APIKey       string
	BaseURL      string
	Organization string
}

// NewConfig creates a config option.
func NewConfig(apiKey string, opts ...func(*Config)) *Config {
	c := &Config{APIKey: apiKey}
	for _, o := range opts {
		o(c)
	}
	return c
}

// WithBaseURL sets custom API base URL.
func WithBaseURL(base string) func(*Config) { return func(c *Config) { c.BaseURL = base } }

// WithOrganization sets OpenAI organization header.
func WithOrganization(org string) func(*Config) { return func(c *Config) { c.Organization = org } }

type Storager struct {
	httpClient   *http.Client
	apiKey       string
	baseURL      string
	organization string
	basePath     string
}

// Close closes storager
func (s *Storager) Close() error { return nil }

// FilterAuthOptions filters auth options
func (s *Storager) FilterAuthOptions(options []storage.Option) []storage.Option {
	var authOptions = make([]storage.Option, 0)
	if cfg, _ := filterAuthOption(options); cfg != nil {
		authOptions = append(authOptions, cfg)
	}
	return authOptions
}

func filterAuthOption(options []storage.Option) (*Config, error) {
	cfg := &Config{}
	if _, ok := option.Assign(options, &cfg); ok {
		return cfg, nil
	}
	// Fallback to env var
	if key := os.Getenv(apiKeyEnv); key != "" {
		return &Config{APIKey: key}, nil
	}
	return nil, nil
}

// IsAuthChanged returns true if auth has changes
func (s *Storager) IsAuthChanged(authOptions []storage.Option) bool {
	if len(authOptions) == 0 {
		return false
	}
	cfg, _ := filterAuthOption(authOptions)
	if cfg == nil {
		return false
	}
	if s.apiKey != cfg.APIKey {
		return true
	}
	if cfg.BaseURL != "" && s.baseURL != cfg.BaseURL {
		return true
	}
	if cfg.Organization != "" && s.organization != cfg.Organization {
		return true
	}
	return false
}

// NewStorager creates OpenAI Assets storager
func NewStorager(ctx context.Context, baseURL string, options ...storage.Option) (*Storager, error) {
	cfg, _ := filterAuthOption(options)
	result := &Storager{
		httpClient: http.DefaultClient,
		baseURL:    defaultBase,
		basePath:   afsurl.Path(baseURL),
	}
	if cfg != nil {
		result.apiKey = cfg.APIKey
		if cfg.BaseURL != "" {
			result.baseURL = cfg.BaseURL
		}
		result.organization = cfg.Organization
	} else {
		// Try env var
		result.apiKey = os.Getenv(apiKeyEnv)
	}
	return result, nil
}
