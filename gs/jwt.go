package gs

import (
	"bytes"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"io/ioutil"
	"os"
)

//JWTProvider represetns JWT based auth provider
type JWTProvider interface {
	JWTConfig(scopes ...string) (config *jwt.Config, projectID string, err error)
}

//JwtConfig represents google service account secrets
type JwtConfig struct {
	//google cloud credential
	ClientEmail             string `json:"client_email,omitempty"`
	TokenURL                string `json:"token_uri,omitempty"`
	PrivateKey              string `json:"private_key,omitempty"`
	PrivateKeyID            string `json:"private_key_id,omitempty"`
	ProjectID               string `json:"project_id,omitempty"`
	TokenURI                string `json:"token_uri,omitempty"`
	Type                    string `json:"type,omitempty"`
	ClientX509CertURL       string `json:"client_x509_cert_url,omitempty"`
	AuthProviderX509CertURL string `json:"auth_provider_x509_cert_url,omitempty"`
	jwtClientConfig         *jwt.Config
}

//JWTConfig returns new JWT config for supplied scopes
func (c *JwtConfig) JWTConfig(scopes ...string) (config *jwt.Config, projectID string, err error) {
	if c.jwtClientConfig != nil {
		return c.jwtClientConfig, c.ProjectID, nil
	}
	var result = &jwt.Config{
		Email:        c.ClientEmail,
		Subject:      c.ClientEmail,
		PrivateKey:   []byte(c.PrivateKey),
		PrivateKeyID: c.PrivateKeyID,
		Scopes:       scopes,
		TokenURL:     c.TokenURL,
	}
	if result.TokenURL == "" {
		result.TokenURL = google.JWTTokenURL
	}
	c.jwtClientConfig = result
	return result, c.ProjectID, nil
}

//NewJwtConfig returns new secrets from location
func NewJwtConfig(options ...storage.Option) (*JwtConfig, error) {
	location := &option.Location{}
	var JSONPayload = make([]byte, 0)
	option.Assign(options, &location, &JSONPayload)
	option.Assign(options, &location)
	if location.Path == "" && len(JSONPayload) == 0 {
		return nil, errors.New("auth location was empty")
	}
	if location.Path != "" {
		file, err := os.Open(location.Path)
		if err != nil {
			return nil, errors.Wrap(err, "failed to open auth config")
		}
		defer func() { _ = file.Close() }()
		if JSONPayload, err = ioutil.ReadAll(file); err != nil {
			return nil, err
		}
	}
	config := &JwtConfig{}
	err := json.NewDecoder(bytes.NewReader(JSONPayload)).Decode(config)
	return config, err
}
