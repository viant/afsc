package op

import (
	"fmt"
	"strings"

	"github.com/viant/afs/url"
)

func reference(baseURL, resourceID string) (string, error) {
	resourceID = strings.TrimSpace(resourceID)
	if resourceID == "" {
		return "", fmt.Errorf("secret path was empty")
	}
	if strings.HasPrefix(resourceID, Scheme+"://") {
		return resourceID, nil
	}
	vault := url.Host(baseURL)
	if vault == "" {
		return "", fmt.Errorf("vault was empty in %v", baseURL)
	}
	path := strings.TrimLeft(resourceID, "/")
	if path == "" {
		return "", fmt.Errorf("secret path was empty")
	}
	return fmt.Sprintf("%s://%s/%s", Scheme, vault, path), nil
}
