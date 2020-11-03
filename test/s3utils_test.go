package test

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/tnyidea/awsutils-go/s3utils"
	"log"
	"net/url"
	"strings"
	"testing"
)

// TODO Current set of testcases is incomplete

// serviceKey is a colon delimited string defined as follows:
// <AWS Region>:<Access Key ID>:<Access Key Secret>
const serviceKey = ""

// sourceBucket is a bucket used for source operations testing (ex. List, Copy)
const sourceBucket = ""

// sourceObjectKey is an object key name used for source operations testing (ex. List, Copy)
const sourceObjectKey = ""

// sourceObjectPrefix is an object key prefix name used for source prefix operations testing (ex. GetSize)
const sourceObjectPrefix = ""

// targetBucket is a bucket used for destination operations testing (ex. Copy)
const targetBucket = ""

// targetObjectKey is an object key name used for destination operations testing (ex. Copy)
const targetObjectKey = ""

// testS3Url is an S3 url to an object in S3 for testing
const testS3Url = ""

func TestS3Copy(t *testing.T) {
	s3Object, err := s3utils.NewS3ObjectFromS3Url(testS3Url, serviceKey)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	targetS3Object := s3utils.NewS3Object(targetBucket, targetObjectKey, serviceKey)

	err = s3Object.Copy(targetS3Object)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
}

func TestS3ListObjects(t *testing.T) {
	s3Object := s3utils.NewS3Object(sourceObjectKey, sourceObjectKey, serviceKey)

	objectList, err := s3Object.ListObjects()
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	log.Println(aws.String("") == nil)

	log.Println(len(objectList))
	log.Println(objectList)
}

func TestRename(t *testing.T) {
	testObjectKey := "SAMPLESPACE+FILE.txt"
	//decodedObjectKey := strings.ReplaceAll(testObjectKey, "+", " ")
	decodedObjectKey, _ := url.QueryUnescape(testObjectKey)
	log.Println(decodedObjectKey)

	s3Object := s3utils.NewS3Object(sourceBucket, decodedObjectKey, serviceKey)
	log.Println(s3Object)

	newName := strings.ReplaceAll(decodedObjectKey, " ", "_")
	log.Println(newName)
	err := s3Object.Rename(newName)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
}

func TestGetObject(t *testing.T) {
	s3Object := s3utils.NewS3Object(sourceBucket, sourceObjectKey, serviceKey)

	//_, _ = s3Object.GetObject()
	targetS3Object := s3utils.NewS3Object(targetBucket, targetObjectKey, serviceKey)
	err := s3Object.MultipartCopy(targetS3Object)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
}

func TestGetTotalSize(t *testing.T) {
	prefix, err := s3utils.NewS3ObjectPrefixFromS3Url(sourceObjectPrefix, serviceKey)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	count, totalSize, err := prefix.GetTotalSize()
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	log.Println(count)
	log.Println(totalSize)
}
