package gs

import (
	"fmt"

	"google.golang.org/api/option"
)

var authOptions = map[string]bool{
	fmt.Sprintf("%T", option.WithTokenSource(nil)):     true,
	fmt.Sprintf("%T", option.WithCredentialsJSON(nil)): true,
	fmt.Sprintf("%T", option.WithCredentialsFile("")):  true,
}

func HasAuthOption(options []option.ClientOption) bool {
	for _, authOption := range options {
		if authOption == nil {
			continue
		}
		if _, ok := authOptions[fmt.Sprintf("%T", authOption)]; ok {
			return true
		}
	}
	return false
}

func Options(base, options []option.ClientOption) []option.ClientOption {
	var result = append([]option.ClientOption{}, options...)
	hasAuth := HasAuthOption(options)
	if hasAuth {
		for _, authOption := range base {
			if _, ok := authOptions[fmt.Sprintf("%T", authOption)]; ok {
				continue
			}
			result = append(result, authOption)
		}

	} else {
		result = append(result, base...)
	}
	return result
}
