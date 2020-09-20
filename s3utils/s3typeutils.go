package s3utils

import (
	"errors"
	"strings"
)

// TODO is this really needed?  Can't we just to s3.New(awsSession) inline?

// TODO this is a convenience function TBD if still needed
func SplitS3Url(url string) (string, string, error) {
	tokens := strings.Split(url, "//")
	if len(tokens) != 2 {
		return "", "", errors.New("invalid S3 URL: must be of format s3://bucket-name/key-name")
	}

	tokens = strings.Split(tokens[1], "/")
	if len(tokens) < 2 {
		return "", "", errors.New("invalid S3 URL: must be of format s3://bucket-name/key-name")
	}

	return tokens[0], strings.Join(tokens[1:], "/"), nil
}
