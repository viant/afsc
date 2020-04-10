package gs

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"golang.org/x/oauth2"
	gstorage "google.golang.org/api/storage/v1"
	"net"
	"net/http"
	"net/url"
	"time"
)

type client struct {
	projectID string
	region    string
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
	region := &option.Region{}
	if _, ok := option.Assign(options, &region); ok {
		result.region = region.Name
	}
	if jwTProvider != nil {
		config, projectID, err := jwTProvider.JWTConfig(scopes...)
		if err != nil {
			return nil, err
		}
		result.projectID = projectID
		result.Client = oauth2.NewClient(ctx, config.TokenSource(ctx))
	} else {
		result.Client, err = DefaultHTTPClientProvider(ctx, scopes)
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
			result.Transport = transport
		}
	}
	if result.projectID == "" {
		projectID, err := DefaultProjectProvider(ctx, scopes)
		if err != nil {
			return nil, err
		}
		result.projectID = projectID
	}
	return result, err
}
