package s3utils

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/tnyidea/awsutils-go/awsutils"
	"strings"
)

// TODO make a version that can be passed as an encrypted hash
func NewS3Session(serviceKey string) (*s3.S3, error) {
	awsSession, err := awsutils.NewAWSSession(serviceKey)
	if err != nil {
		return nil, err
	}
	return s3.New(awsSession), nil
}

func getBucketRegion(bucket string, serviceKey string) (string, error) {
	s3Session := session.Must(session.NewSession())
	region, err := s3manager.GetBucketRegion(aws.BackgroundContext(), s3Session, bucket, strings.Split(serviceKey, ":")[0])
	if err != nil {
		return "", err
	}

	return region, nil
}
