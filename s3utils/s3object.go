package s3utils

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/tnyidea/awsutils-go/awsutils"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type S3Object struct {
	ServiceKey   string    `json:"-"` // Should be private for output
	Bucket       string    `json:"bucket"`
	ObjectKey    string    `json:"objectKey"`
	ETag         string    `json:"etag"`
	Size         int64     `json:"size"`
	Owner        string    `json:"owner"`
	StorageClass string    `json:"storageClass"`
	LastModified time.Time `json:"lastModified"`
}

func NewS3Object(bucket string, objectKey string, serviceKey string) S3Object {
	return S3Object{
		ServiceKey: serviceKey,
		Bucket:     bucket,
		ObjectKey:  objectKey,
	}
}

func NewS3ObjectFromS3Url(url string, serviceKey string) (S3Object, error) {
	tokens := strings.Split(serviceKey, ":")
	if len(tokens) != 3 {
		return S3Object{}, errors.New("invalid service key format")
	}

	tokens = strings.Split(url, "//")
	if tokens[0] != "s3:" {
		return S3Object{}, errors.New("invalid S3 URL: invalid protocol '" + tokens[0] +
			"'. S3 URL Must be in the form of s3://bucket_name/object_key")
	}

	tokens = strings.Split(tokens[1], "/")
	if len(tokens) == 1 {
		return S3Object{}, errors.New("invalid S3 URL: missing object key or bucket. S3 URL Must be in the form of s3://bucket_name/object_key")
	}

	return S3Object{
		ServiceKey: serviceKey,
		Bucket:     tokens[0],
		ObjectKey:  strings.Join(tokens[1:], "/"),
	}, nil
}

func (s *S3Object) Bytes() []byte {
	b, _ := json.MarshalIndent(s, "", "    ")
	return b
}

func (s *S3Object) String() string {
	b, _ := json.MarshalIndent(s, "", "    ")
	return string(b)
}

func (s *S3Object) Filename() string {
	tokens := strings.Split(s.ObjectKey, "/")
	return tokens[len(tokens)-1]
}

func (s *S3Object) S3Url() (string, error) {
	if s.Bucket == "" || s.ObjectKey == "" {
		return "", errors.New("invalid S3 URL: must specify both Bucket and Object Key")
	}

	return "s3://" + s.Bucket + "/" + s.ObjectKey, nil
}

// S3 Operations
func (s *S3Object) Copy(targetBucket string, targetObjectKey string) error {
	s3Session, err := NewS3Session(s.ServiceKey)
	if err != nil {
		return err
	}
	_, err = s3Session.CopyObject(&s3.CopyObjectInput{
		CopySource: aws.String("/" + s.Bucket + "/" + s.ObjectKey),
		Bucket:     aws.String(targetBucket),
		Key:        aws.String(targetObjectKey),
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *S3Object) MultipartCopy(targetBucket string, targetObjectKey string) error {
	s3Session, err := NewS3Session(s.ServiceKey)
	if err != nil {
		return err
	}

	headObjectResult, err := s3Session.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s.ObjectKey),
	})
	if err != nil {
		return err
	}

	upload, err := s3Session.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(targetBucket),
		Key:    aws.String(targetObjectKey),
	})
	if err != nil {
		return err
	}

	sourceObjectSize := *headObjectResult.ContentLength
	partSize := int64(math.Pow(1024, 3)) // 1 GB
	partNumber := int64(1)
	var completedParts []*s3.CompletedPart

	log.Println("==Starting Multipart Copy==")
	log.Println("Source File Size:", sourceObjectSize)
	log.Println("Part Size:", partSize)

	for bytePosition := int64(0); bytePosition < sourceObjectSize; bytePosition += partSize {
		lastByte := int64(math.Min(float64(bytePosition+partSize-1), float64(sourceObjectSize-1)))
		byteRangeString := "bytes=" + strconv.FormatInt(bytePosition, 10) + "-" + strconv.FormatInt(lastByte, 10)
		log.Println("Copying Part Number", partNumber, ": Byte Range:", byteRangeString)

		// Copy this part
		queryEscapeObjectKeyBug := url.QueryEscape(s.ObjectKey) // THERE IS A WEIRD BUG THAT UPLOAD PART COPY REQUIRES THIS
		partResult, err := s3Session.UploadPartCopy(&s3.UploadPartCopyInput{
			Bucket:          aws.String(targetBucket),
			CopySource:      aws.String("/" + s.Bucket + "/" + queryEscapeObjectKeyBug),
			CopySourceRange: aws.String(byteRangeString),
			Key:             aws.String(targetObjectKey),
			PartNumber:      aws.Int64(partNumber),
			UploadId:        upload.UploadId,
		})
		if err != nil {
			return err
		}

		completedParts = append(completedParts, &s3.CompletedPart{
			ETag:       partResult.CopyPartResult.ETag,
			PartNumber: aws.Int64(partNumber),
		})
		partNumber++
	}

	_, err = s3Session.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket: aws.String(targetBucket),
		Key:    aws.String(targetObjectKey),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
		UploadId: upload.UploadId,
	})
	if err != nil {
		return err
	}

	log.Println("==Multipart Copy Complete==")
	return nil
}

func (s *S3Object) Delete() error {
	s3Session, err := NewS3Session(s.ServiceKey)
	if err != nil {
		return err
	}

	_, err = s3Session.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s.ObjectKey),
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *S3Object) DownloadBytes() ([]byte, error) {
	awsSession, err := awsutils.NewAWSSession(s.ServiceKey)
	if err != nil {
		return nil, err
	}

	s3DownloadBuffer := aws.NewWriteAtBuffer([]byte{})
	s3Downloader := s3manager.NewDownloader(awsSession)
	_, err = s3Downloader.Download(s3DownloadBuffer,
		&s3.GetObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(s.ObjectKey),
		})
	if err != nil {
		return nil, err
	}

	return s3DownloadBuffer.Bytes(), nil
}

func (s *S3Object) DownloadReader() (io.ReadCloser, error) {
	awsSession, err := awsutils.NewAWSSession(s.ServiceKey)
	if err != nil {
		return nil, err
	}

	s3DownloadBuffer := aws.NewWriteAtBuffer([]byte{})
	s3Downloader := s3manager.NewDownloader(awsSession)
	_, err = s3Downloader.Download(s3DownloadBuffer,
		&s3.GetObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(s.ObjectKey),
		})
	if err != nil {
		return nil, err
	}

	return ioutil.NopCloser(bytes.NewReader(s3DownloadBuffer.Bytes())), nil
}

func (s *S3Object) Exists() (bool, error) {
	s3Session, err := NewS3Session(s.ServiceKey)
	if err != nil {
		return false, err
	}

	output, err := s3Session.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:     aws.String(s.Bucket),
		FetchOwner: aws.Bool(true),
		MaxKeys:    aws.Int64(1),
		Prefix:     aws.String(s.ObjectKey),
	})
	if err != nil {
		return false, err
	}

	if len(output.Contents) != 1 {
		return false, nil
	}
	object := output.Contents[0]

	s.ETag = strings.ReplaceAll(*object.ETag, "\"", "")
	s.Size = *object.Size
	s.Owner = *object.Owner.DisplayName
	s.StorageClass = *object.StorageClass
	s.LastModified = *object.LastModified

	return true, nil
}

func (s *S3Object) ListObject() error {
	exists, err := s.Exists()
	if err != nil {
		return err
	}

	if !exists {
		return errors.New("s3Object not found")
	}
	return nil
}

// Deprecated
// Use S3ObjectPrefix.ListObjects
func (s *S3Object) ListObjects() ([]S3Object, error) {
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
			Prefix:     aws.String( /*As prefix*/ s.ObjectKey),
			StartAfter: aws.String(startAfter),
		}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for i := range page.Contents {
				etag := strings.ReplaceAll(*page.Contents[i].ETag, "\"", "")
				pageContents := page.Contents[i]
				pageContents.ETag = &etag
				objectList = append(objectList, S3Object{
					ServiceKey:   "",
					Bucket:       s.Bucket,
					ObjectKey:    *pageContents.Key,
					ETag:         *pageContents.ETag,
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

// Deprecated
// Use S3ObjectPrefix.ListObjectsAfterTime
func (s *S3Object) ListObjectsAfterTime(afterTime time.Time) ([]S3Object, error) {
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
			Prefix:     aws.String( /*As prefix*/ s.ObjectKey),
			StartAfter: aws.String(startAfter),
		}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for i := range page.Contents {
				if (*page.Contents[i].LastModified).After(afterTime) {
					etag := strings.ReplaceAll(*page.Contents[i].ETag, "\"", "")
					pageContents := page.Contents[i]
					pageContents.ETag = &etag
					objectList = append(objectList, S3Object{
						ServiceKey:   "",
						Bucket:       s.Bucket,
						ObjectKey:    *pageContents.Key,
						ETag:         *pageContents.ETag,
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

// Deprecated
// Use S3ObjectPrefix.ListObjectsBeforeTime
func (s *S3Object) ListObjectsBeforeTime(beforeTime time.Time) ([]S3Object, error) {
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
			Prefix:     aws.String( /*As prefix*/ s.ObjectKey),
			StartAfter: aws.String(startAfter),
		}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for i := range page.Contents {
				if (*page.Contents[i].LastModified).Before(beforeTime) {
					etag := strings.ReplaceAll(*page.Contents[i].ETag, "\"", "")
					pageContents := page.Contents[i]
					pageContents.ETag = &etag
					objectList = append(objectList, S3Object{
						ServiceKey:   "",
						Bucket:       s.Bucket,
						ObjectKey:    *pageContents.Key,
						ETag:         *pageContents.ETag,
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

// Deprecated
// Use S3ObjectPrefix.ListObjectsBetweenTimes
func (s *S3Object) ListObjectsBetweenTimes(afterTime time.Time, beforeTime time.Time) ([]S3Object, error) {
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
			Prefix:     aws.String( /*As prefix*/ s.ObjectKey),
			StartAfter: aws.String(startAfter),
		}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for i := range page.Contents {
				if (*page.Contents[i].LastModified).After(afterTime) && (*page.Contents[i].LastModified).Before(beforeTime) {
					etag := strings.ReplaceAll(*page.Contents[i].ETag, "\"", "")
					pageContents := page.Contents[i]
					pageContents.ETag = &etag
					objectList = append(objectList, S3Object{
						ServiceKey:   "",
						Bucket:       s.Bucket,
						ObjectKey:    *pageContents.Key,
						ETag:         *pageContents.ETag,
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

func (s *S3Object) Rename(targetObjectKey string) error {
	err := s.MultipartCopy(s.Bucket, targetObjectKey)
	if err != nil {
		return err
	}
	return s.Delete()
}

func (s *S3Object) UploadBytes(uploadBytes []byte) error {
	awsSession, err := awsutils.NewAWSSession(s.ServiceKey)
	if err != nil {
		return err
	}

	s3Uploader := s3manager.NewUploader(awsSession)
	_, err = s3Uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s.ObjectKey),
		Body:   bytes.NewReader(uploadBytes),
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *S3Object) UploadReader(reader io.ReadCloser) error {
	awsSession, err := awsutils.NewAWSSession(s.ServiceKey)
	if err != nil {
		return err
	}

	s3Uploader := s3manager.NewUploader(awsSession)
	_, err = s3Uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s.ObjectKey),
		Body:   reader,
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *S3Object) WriteToHttpResponse(w http.ResponseWriter) error {
	downloadBytes, err := s.DownloadBytes()
	if err != nil {
		return err
	}

	_, err = w.Write(downloadBytes)
	if err != nil {
		return err
	}

	return nil
}
