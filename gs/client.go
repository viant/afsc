package gs

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	goptions "google.golang.org/api/option"
	gstorage "google.golang.org/api/storage/v1"
	htransport "google.golang.org/api/transport/http"
	"net"
	"net/http"
	"net/url"
	"time"
)

type client struct {
	projectID string
	ctx       context.Context
	*http.Client

	useProxy         bool
	canProxyFallback bool
}

func (c *client) disableProxy() {
	if !c.useProxy {
		return
	}
	switch val := c.Transport.(type) {
	case *oauth2.Transport:
		val.Base = nil
	}
	c.useProxy = false
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
	proxy := &option.Proxy{}
	option.Assign(options, &jwTProvider, &scopes, &proxy)
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

	if proxy.URL != "" {
		result.useProxy = true
		result.canProxyFallback = proxy.Fallback
		proxyUrl, err := url.Parse(proxy.URL)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse proxy URL: %v", proxy.URL)
		}
		transport := &http.Transport{Proxy: http.ProxyURL(proxyUrl)}
		if proxy.TimeoutMs > 0 {
			timeout := time.Millisecond * time.Duration(proxy.TimeoutMs)
			dialContext := (&net.Dialer{
				Timeout:   timeout,
				DualStack: true,
			}).DialContext
			transport.DialContext = dialContext
		}
		switch val := result.Transport.(type) {
		case *oauth2.Transport:
			val.Base = transport
		default:
			fmt.Printf("setting proxy %T\n", result.Transport)
			result.Transport = transport
		}
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
