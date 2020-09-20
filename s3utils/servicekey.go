package s3utils

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/tnyidea/awsutils-go/awsutils"
)

// TODO make a version that can be passed as an encrypted hash
func NewS3Session(serviceKey string) (*s3.S3, error) {
	awsSession, err := awsutils.NewAWSSession(serviceKey)
	if err != nil {
		return nil, err
	}
	return s3.New(awsSession), nil
}
