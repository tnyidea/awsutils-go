package s3utils

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
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
	Region       string    `json:"region"`
	Bucket       string    `json:"bucket"`
	ObjectKey    string    `json:"objectKey"`
	Exists       bool      `json:"exists"`
	ETag         string    `json:"etag"`
	Size         int64     `json:"size"`
	StorageClass string    `json:"storageClass"`
	LastModified time.Time `json:"lastModified"`
}

func NewS3Object(bucket string, objectKey string, serviceKey string) (S3Object, error) {
	s3Object := S3Object{
		ServiceKey: serviceKey,
		Bucket:     bucket,
		ObjectKey:  objectKey,
		Exists:     true,
	}

	region, err := getBucketRegion(s3Object.Bucket, serviceKey)
	if err != nil {
		return S3Object{}, errors.New("error locating bucket region: " + err.Error())
	}
	s3Object.Region = region
	s3Object.localizeServiceKey()

	err = s3Object.listObjectV2()
	if err != nil {
		if awsError, defined := err.(awserr.Error); defined {
			code := awsError.Code()
			if code == s3.ErrCodeNoSuchKey {
				s3Object.Exists = false
				return s3Object, nil
			}
		}
		return S3Object{}, err
	}

	return s3Object, nil
}

func NewS3ObjectFromS3Url(url string, serviceKey string) (S3Object, error) {
	tokens := strings.Split(url, "//")
	if tokens[0] != "s3:" {
		return S3Object{}, errors.New("invalid S3 URL: invalid protocol '" + tokens[0] +
			"'. S3 URL Must be in the form of s3://bucket_name/object_key")
	}

	tokens = strings.Split(tokens[1], "/")
	if len(tokens) == 1 {
		return S3Object{}, errors.New("invalid S3 URL: missing object key or bucket. S3 URL Must be in the form of s3://bucket_name/object_key")
	}

	return NewS3Object(tokens[0], strings.Join(tokens[1:], "/"), serviceKey)
}

func (s *S3Object) Bytes() []byte {
	b, _ := json.Marshal(s)
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

func (s *S3Object) localizeServiceKey() {
	tokens := strings.Split(s.ServiceKey, ":")
	tokens[0] = s.Region
	s.ServiceKey = strings.Join(tokens, ":")
}

func (s *S3Object) listObjectV2() error {
	s3Session, err := NewS3Session(s.ServiceKey)
	if err != nil {
		return err
	}

	output, err := s3Session.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:  aws.String(s.Bucket),
		MaxKeys: aws.Int64(1),
		Prefix:  aws.String(s.ObjectKey),
	})
	if err != nil {
		return err
	}

	if *output.KeyCount != 1 {
		return awserr.New(s3.ErrCodeNoSuchKey, "No Such Key", errors.New("no such key found"))
	}
	object := output.Contents[0]

	s.ETag = strings.ReplaceAll(*object.ETag, "\"", "")
	s.Size = *object.Size
	// s.Owner = *object.Owner.DisplayName
	s.StorageClass = *object.StorageClass
	s.LastModified = *object.LastModified

	return nil
}

func (s *S3Object) Copy(target S3Object) error {
	s3Session, err := NewS3Session(s.ServiceKey)
	if err != nil {
		return err
	}
	_, err = s3Session.CopyObject(&s3.CopyObjectInput{
		CopySource: aws.String("/" + s.Bucket + "/" + s.ObjectKey),
		Bucket:     aws.String(target.Bucket),
		Key:        aws.String(target.ObjectKey),
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *S3Object) MultipartCopy(target S3Object) error {
	source := s
	if source.Region != target.Region {
		return s.crossRegionMultipartCopy(target)
	}

	s3Session, err := NewS3Session(s.ServiceKey)
	if err != nil {
		return err
	}

	sourceHeadObjectResult, err := s3Session.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(source.Bucket),
		Key:    aws.String(source.ObjectKey),
	})
	if err != nil {
		return err
	}

	sourceObjectSize := *sourceHeadObjectResult.ContentLength
	// partSize := int64(math.Pow(1024, 3)) // 1 GiB
	partSize := int64(math.Pow(1024, 2) * 100) // 100 MiB
	partNumber := int64(1)

	uploader, err := s3Session.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(target.Bucket),
		Key:    aws.String(target.ObjectKey),
	})
	if err != nil {
		return err
	}

	log.Println("==Starting Multipart Copy==")
	log.Println("Source File Size:", sourceObjectSize)
	log.Println("Part Size:", partSize)

	var completedParts []*s3.CompletedPart
	for bytePosition := int64(0); bytePosition < sourceObjectSize; bytePosition += partSize {
		lastByte := int64(math.Min(float64(bytePosition+partSize-1), float64(sourceObjectSize-1)))
		byteRangeString := "bytes=" + strconv.FormatInt(bytePosition, 10) + "-" + strconv.FormatInt(lastByte, 10)
		log.Println("Copying Part Number", partNumber, ": Byte Range:", byteRangeString)

		partResult, err := s3Session.UploadPartCopy(&s3.UploadPartCopyInput{
			Bucket:          aws.String(target.Bucket),
			CopySource:      aws.String(url.PathEscape("/" + source.Bucket + "/" + source.ObjectKey)),
			CopySourceRange: aws.String(byteRangeString),
			Key:             aws.String(target.ObjectKey),
			PartNumber:      aws.Int64(partNumber),
			UploadId:        uploader.UploadId,
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
		Bucket: aws.String(target.Bucket),
		Key:    aws.String(target.ObjectKey),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
		UploadId: uploader.UploadId,
	})
	if err != nil {
		return err
	}

	log.Println("==Multipart Copy Complete==")
	return nil
}

func (s *S3Object) crossRegionMultipartCopy(target S3Object) error {
	source := s

	sourceSession, err := NewS3Session(s.ServiceKey)
	if err != nil {
		return err
	}
	targetSession, err := NewS3Session(target.ServiceKey)
	if err != nil {
		return err
	}

	sourceHeadObjectResult, err := sourceSession.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(source.Bucket),
		Key:    aws.String(source.ObjectKey),
	})
	if err != nil {
		return err
	}

	sourceObjectSize := *sourceHeadObjectResult.ContentLength
	// partSize := int64(math.Pow(1024, 3)) // 1 GiB
	partSize := int64(math.Pow(1024, 2) * 100) // 100 MiB
	partNumber := int64(1)

	downloader := s3manager.NewDownloaderWithClient(sourceSession,
		func(d *s3manager.Downloader) {
			d.PartSize = partSize
		})

	uploader, err := targetSession.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(target.Bucket),
		Key:    aws.String(target.ObjectKey),
	})
	if err != nil {
		return err
	}

	log.Println("==Starting Multipart Copy==")
	log.Println("Source File Size:", sourceObjectSize)
	log.Println("Part Size:", partSize)

	var completedParts []*s3.CompletedPart
	var buffer []byte
	writeBuffer := aws.NewWriteAtBuffer(buffer)
	for bytePosition := int64(0); bytePosition < sourceObjectSize; bytePosition += partSize {
		lastByte := int64(math.Min(float64(bytePosition+partSize-1), float64(sourceObjectSize-1)))
		byteRangeString := "bytes=" + strconv.FormatInt(bytePosition, 10) + "-" + strconv.FormatInt(lastByte, 10)
		log.Println("Copying Part Number", partNumber, ": Byte Range:", byteRangeString)

		_, err := downloader.Download(writeBuffer, &s3.GetObjectInput{
			Bucket: aws.String(source.Bucket),
			Key:    aws.String(source.ObjectKey),
			Range:  aws.String(byteRangeString),
		})
		if err != nil {
			return err
		}

		partResult, err := targetSession.UploadPart(&s3.UploadPartInput{
			Body:          bytes.NewReader(writeBuffer.Bytes()),
			Bucket:        aws.String(target.Bucket),
			ContentLength: aws.Int64(partSize),
			Key:           aws.String(target.ObjectKey),
			PartNumber:    aws.Int64(partNumber),
			UploadId:      uploader.UploadId,
		})
		if err != nil {
			return err
		}

		completedParts = append(completedParts, &s3.CompletedPart{
			ETag:       partResult.ETag,
			PartNumber: aws.Int64(partNumber),
		})
		partNumber++
	}

	_, err = targetSession.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket: aws.String(target.Bucket),
		Key:    aws.String(target.ObjectKey),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
		UploadId: uploader.UploadId,
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

func (s *S3Object) Rename(targetObjectKey string) error {
	target := *s
	target.ObjectKey = targetObjectKey
	err := s.MultipartCopy(target)
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
