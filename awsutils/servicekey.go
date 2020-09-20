package awsutils

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"strings"
)

func NewAWSSession(serviceKey string) (*session.Session, error) {
	if serviceKey == "" {
		return nil, errors.New("service key cannot be empty")
	}
	tokens := strings.Split(serviceKey, ":")
	if len(tokens) != 3 {
		return nil, errors.New("invalid service key format")
	}

	region := tokens[0]
	keyId := tokens[1]
	keySecret := tokens[2]

	return session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config: aws.Config{
			Credentials: credentials.NewStaticCredentials(keyId, keySecret, ""),
			Region:      aws.String(region),
		},
	})), nil
}
