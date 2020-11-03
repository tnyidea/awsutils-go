package s3utils

import (
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"strings"
	"time"
)

type S3ObjectPrefix struct {
	ServiceKey string `json:"-"` // Should be private for output
	Bucket     string `json:"bucket"`
	Prefix     string `json:"prefix"`
}

func NewS3ObjectPrefix(bucket string, prefix string, serviceKey string) (S3ObjectPrefix, error) {
	return S3ObjectPrefix{
		ServiceKey: serviceKey,
		Bucket:     bucket,
		Prefix:     prefix,
	}, nil
}

func NewS3ObjectPrefixFromS3Url(url string, serviceKey string) (S3ObjectPrefix, error) {
	tokens := strings.Split(url, "//")
	if tokens[0] != "s3:" {
		return S3ObjectPrefix{}, errors.New("invalid S3 URL: invalid protocol '" + tokens[0] +
			"'. S3 URL Must be in the form of s3://bucket_name/object_prefix")
	}

	tokens = strings.Split(tokens[1], "/")
	if len(tokens) == 1 {
		return S3ObjectPrefix{}, errors.New("invalid S3 URL: missing object prefix or bucket. S3 URL Must be in the form of s3://bucket_name/object_prefix")
	}

	return S3ObjectPrefix{
		ServiceKey: serviceKey,
		Bucket:     tokens[0],
		Prefix:     strings.Join(tokens[1:], "/"),
	}, nil
}

func (s *S3ObjectPrefix) Bytes() []byte {
	b, _ := json.MarshalIndent(s, "", "    ")
	return b
}

func (s *S3ObjectPrefix) String() string {
	b, _ := json.MarshalIndent(s, "", "    ")
	return string(b)
}

func (s *S3ObjectPrefix) S3Url() (string, error) {
	if s.Bucket == "" || s.Prefix == "" {
		return "", errors.New("invalid S3 URL: must specify both Bucket and Object Prefix")
	}

	return "s3://" + s.Bucket + "/" + s.Prefix, nil
}

func (s *S3ObjectPrefix) GetTotalSize() (int64, int64, error) {
	s3Session, err := NewS3Session(s.ServiceKey)
	if err != nil {
		return 0, 0, err
	}

	var count int64 = 0
	var totalSize int64 = 0
	var startAfter string
	var isTruncated bool

	for {
		err := s3Session.ListObjectsV2Pages(&s3.ListObjectsV2Input{
			Bucket:     aws.String(s.Bucket),
			Prefix:     aws.String(s.Prefix),
			StartAfter: aws.String(startAfter),
		}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			if *page.KeyCount == 0 {
				return true
			}
			for i := range page.Contents {
				count++
				totalSize += *page.Contents[i].Size
			}
			startAfter = *page.Contents[len(page.Contents)-1].Key
			isTruncated = *page.IsTruncated

			return false
		})
		if err != nil {
			return 0, 0, err
		}

		if !isTruncated {
			break
		}
	}

	return count, totalSize, nil
}

func (s *S3ObjectPrefix) ListObjects() ([]S3Object, error) {
	s3Session, err := NewS3Session(s.ServiceKey)
	if err != nil {
		return nil, err
	}

	var objectList []S3Object
	var startAfter string
	var isTruncated bool

	for {
		err := s3Session.ListObjectsV2Pages(&s3.ListObjectsV2Input{
			Bucket:     aws.String(s.Bucket),
			Prefix:     aws.String(s.Prefix),
			FetchOwner: aws.Bool(true),
			StartAfter: aws.String(startAfter),
		}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			if *page.KeyCount == 0 {
				return true
			}
			for i := range page.Contents {
				pageContents := page.Contents[i]
				objectList = append(objectList, S3Object{
					ServiceKey:   "",
					Bucket:       s.Bucket,
					ObjectKey:    *pageContents.Key,
					ETag:         strings.ReplaceAll(*page.Contents[i].ETag, "\"", ""),
					Size:         *pageContents.Size,
					StorageClass: *pageContents.StorageClass,
					LastModified: *pageContents.LastModified,
				})
			}
			startAfter = *page.Contents[len(page.Contents)-1].Key
			isTruncated = *page.IsTruncated

			return false
		})
		if err != nil {
			return nil, err
		}

		if !isTruncated {
			break
		}
	}

	return objectList, nil
}

func (s *S3ObjectPrefix) ListObjectsAfterTime(afterTime time.Time) ([]S3Object, error) {
	s3Session, err := NewS3Session(s.ServiceKey)
	if err != nil {
		return nil, err
	}

	var objectList []S3Object
	var startAfter string
	var isTruncated bool

	for {
		err := s3Session.ListObjectsV2Pages(&s3.ListObjectsV2Input{
			Bucket:     aws.String(s.Bucket),
			Prefix:     aws.String(s.Prefix),
			FetchOwner: aws.Bool(true),
			StartAfter: aws.String(startAfter),
		}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			if *page.KeyCount == 0 {
				return true
			}
			for i := range page.Contents {
				if (*page.Contents[i].LastModified).After(afterTime) {
					pageContents := page.Contents[i]
					objectList = append(objectList, S3Object{
						ServiceKey:   "",
						Bucket:       s.Bucket,
						ObjectKey:    *pageContents.Key,
						ETag:         strings.ReplaceAll(*page.Contents[i].ETag, "\"", ""),
						Size:         *pageContents.Size,
						StorageClass: *pageContents.StorageClass,
						LastModified: *pageContents.LastModified,
					})
				}
			}
			if len(page.Contents) == 0 {
				return true
			}

			startAfter = *page.Contents[len(page.Contents)-1].Key
			isTruncated = *page.IsTruncated

			return false
		})
		if err != nil {
			return nil, err
		}

		if !isTruncated {
			break
		}
	}

	return objectList, nil
}

func (s *S3ObjectPrefix) ListObjectsBeforeTime(beforeTime time.Time) ([]S3Object, error) {
	s3Session, err := NewS3Session(s.ServiceKey)
	if err != nil {
		return nil, err
	}

	var objectList []S3Object
	var startAfter string
	var isTruncated bool

	for {
		err := s3Session.ListObjectsV2Pages(&s3.ListObjectsV2Input{
			Bucket:     aws.String(s.Bucket),
			Prefix:     aws.String(s.Prefix),
			FetchOwner: aws.Bool(true),
			StartAfter: aws.String(startAfter),
		}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for i := range page.Contents {
				if (*page.Contents[i].LastModified).Before(beforeTime) {
					pageContents := page.Contents[i]
					objectList = append(objectList, S3Object{
						ServiceKey:   "",
						Bucket:       s.Bucket,
						ObjectKey:    *pageContents.Key,
						ETag:         strings.ReplaceAll(*page.Contents[i].ETag, "\"", ""),
						Size:         *pageContents.Size,
						StorageClass: *pageContents.StorageClass,
						LastModified: *pageContents.LastModified,
					})
				}
			}
			startAfter = *page.Contents[len(page.Contents)-1].Key
			isTruncated = *page.IsTruncated

			return false
		})
		if err != nil {
			return nil, err
		}

		if !isTruncated {
			break
		}
	}

	return objectList, nil
}

func (s *S3ObjectPrefix) ListObjectsBetweenTimes(afterTime time.Time, beforeTime time.Time) ([]S3Object, error) {
	s3Session, err := NewS3Session(s.ServiceKey)
	if err != nil {
		return nil, err
	}

	var objectList []S3Object
	var startAfter string
	var isTruncated bool

	for {
		err := s3Session.ListObjectsV2Pages(&s3.ListObjectsV2Input{
			Bucket:     aws.String(s.Bucket),
			Prefix:     aws.String(s.Prefix),
			FetchOwner: aws.Bool(true),
			StartAfter: aws.String(startAfter),
		}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			if *page.KeyCount == 0 {
				return true
			}
			for i := range page.Contents {
				if (*page.Contents[i].LastModified).After(afterTime) && (*page.Contents[i].LastModified).Before(beforeTime) {
					pageContents := page.Contents[i]
					objectList = append(objectList, S3Object{
						ServiceKey:   "",
						Bucket:       s.Bucket,
						ObjectKey:    *pageContents.Key,
						ETag:         strings.ReplaceAll(*page.Contents[i].ETag, "\"", ""),
						Size:         *pageContents.Size,
						StorageClass: *pageContents.StorageClass,
						LastModified: *pageContents.LastModified,
					})
				}
			}
			if len(page.Contents) == 0 {
				return true
			}

			startAfter = *page.Contents[len(page.Contents)-1].Key
			isTruncated = *page.IsTruncated

			return false
		})
		if err != nil {
			return nil, err
		}

		if !isTruncated {
			break
		}
	}

	return objectList, nil
}

func (s *S3ObjectPrefix) DeleteObjects() error {
	s3ObjectList, err := s.ListObjects()
	if err != nil {
		log.Println("List Error")
		return err
	}

	for i := range s3ObjectList {
		s3ObjectList[i].ServiceKey = s.ServiceKey
		err := s3ObjectList[i].Delete()
		if err != nil {
			log.Println("Delete Error")
			return err
		}
	}

	return nil
}
