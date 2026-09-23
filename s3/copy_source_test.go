package s3

import "testing"

func TestEncodeCopySource(t *testing.T) {
	testCases := []struct {
		name     string
		key      string
		expected string
	}{
		{
			name:     "nested path",
			key:      "folder/file.csv",
			expected: "example-bucket/folder/file.csv",
		},
		{
			name:     "space",
			key:      "file name.csv",
			expected: "example-bucket/file%20name.csv",
		},
		{
			name:     "literal plus",
			key:      "Donor+Sustainer.csv",
			expected: "example-bucket/Donor%2BSustainer.csv",
		},
		{
			name:     "literal percent sequence",
			key:      "literal%20test.csv",
			expected: "example-bucket/literal%2520test.csv",
		},
		{
			name:     "reserved characters",
			key:      "file&name#part?.csv",
			expected: "example-bucket/file%26name%23part%3F.csv",
		},
		{
			name:     "unreserved characters remain unchanged",
			key:      "AZaz09-._~.csv",
			expected: "example-bucket/AZaz09-._~.csv",
		},
		{
			name:     "parentheses and comma",
			key:      "report (final), v2.csv",
			expected: "example-bucket/report%20%28final%29%2C%20v2.csv",
		},
		{
			name:     "URI reserved punctuation",
			key:      "name!$&'()*+,;=:@.csv",
			expected: "example-bucket/name%21%24%26%27%28%29%2A%2B%2C%3B%3D%3A%40.csv",
		},
		{
			name:     "brackets and braces",
			key:      "name[1]{draft}.csv",
			expected: "example-bucket/name%5B1%5D%7Bdraft%7D.csv",
		},
		{
			name:     "quotes slash-like and other punctuation",
			key:      "quote\"backslash\\pipe|caret^backtick`.csv",
			expected: "example-bucket/quote%22backslash%5Cpipe%7Ccaret%5Ebacktick%60.csv",
		},
		{
			name:     "angle brackets",
			key:      "less<greater>.csv",
			expected: "example-bucket/less%3Cgreater%3E.csv",
		},
		{
			name:     "leading trailing and repeated spaces",
			key:      "  file name.csv ",
			expected: "example-bucket/%20%20file%20name.csv%20",
		},
		{
			name:     "repeated path separators",
			key:      "folder//nested///file.csv",
			expected: "example-bucket/folder//nested///file.csv",
		},
		{
			name:     "percent looking literals",
			key:      "literal%2B%2F%zz.csv",
			expected: "example-bucket/literal%252B%252F%25zz.csv",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual := encodeCopySource("example-bucket", testCase.key)
			if actual != testCase.expected {
				t.Fatalf("expected %q, got %q", testCase.expected, actual)
			}
		})
	}
}
