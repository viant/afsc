package gs

import (
	"context"
	"net/http"

	"github.com/viant/afs/storage"
	"golang.org/x/oauth2/google"
	goptions "google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
)

// Provider returns a google storage manager
func Provider(options ...storage.Option) (storage.Manager, error) {
	return New(options...), nil
}

func getDefaultHTTPClient(ctx context.Context, scopes []string) (*http.Client, error) {
	o := []goptions.ClientOption{
		goptions.WithScopes(scopes...),
		goptions.WithUserAgent(UserAgent),
	}
	o = append(DefaultOptions, o...)
	httpClient, _, err := htransport.NewClient(ctx, o...)
	return httpClient, err
}

func getDefaultProject(ctx context.Context, scopes []string) (string, error) {
	credentials, err := google.FindDefaultCredentials(ctx, scopes...)
	if err != nil {
		return "", err
	}
	return credentials.ProjectID, nil
}

// SetOptions set global default options
func SetOptions(options ...goptions.ClientOption) {
	DefaultOptions = options
}

// DefaultOptions represents default client option
var DefaultOptions []goptions.ClientOption

// DefaultHTTPClientProvider defaultHTTP client
var DefaultHTTPClientProvider = getDefaultHTTPClient

// DefaultProjectProvider default projectid provider
var DefaultProjectProvider = getDefaultProject
