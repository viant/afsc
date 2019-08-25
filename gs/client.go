package gs

import (
	"context"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	goptions "google.golang.org/api/option"
	gstorage "google.golang.org/api/storage/v1"
	htransport "google.golang.org/api/transport/http"
	"net/http"
)

type client struct {
	projectID string
	ctx       context.Context
	*http.Client
}

func defaultHTTPClient(ctx context.Context, scopes Scopes) (*http.Client, error) {
	o := []goptions.ClientOption{
		goptions.WithScopes(scopes...),
		goptions.WithUserAgent(UserAgent),
	}
	httpClient, _, err := htransport.NewClient(ctx, o...)
	return httpClient, err
}

func newClient(ctx context.Context, options []storage.Option) (*client, error) {
	var jwTProvider JWTProvider
	var scopes = make(Scopes, 0)
	option.Assign(options, &jwTProvider, &scopes)
	if len(scopes) == 0 {
		scopes = NewScopes(gstorage.CloudPlatformScope, gstorage.DevstorageFullControlScope)
	}
	var err error
	result := &client{
		ctx: ctx,
	}
	if jwTProvider != nil {
		config, projectID, err := jwTProvider.JWTConfig(scopes...)
		if err != nil {
			return nil, err
		}
		result.projectID = projectID
		result.Client = oauth2.NewClient(ctx, config.TokenSource(ctx))
	} else {
		result.Client, err = defaultHTTPClient(ctx, scopes)
	}
	if result.projectID == "" {
		credentials, err := google.FindDefaultCredentials(ctx, scopes...)
		if err != nil {
			return nil, err
		}
		result.projectID = credentials.ProjectID
	}
	return result, err
}
