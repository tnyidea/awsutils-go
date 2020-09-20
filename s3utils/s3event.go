package s3utils

import (
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"strings"
	"time"
)

type S3Event struct {
	Id          int       `json:"id"`
	EventType   string    `json:"eventType"`
	ReferenceId string    `json:"referenceId"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
	S3Object
}

func NewS3EventFromLambdaS3EventRecord(record events.S3EventRecord) S3Event {
	return S3Event{
		S3Object: S3Object{
			Bucket:    record.S3.Bucket.Name,
			ObjectKey: record.S3.Object.Key,
			ETag:      strings.ReplaceAll(record.S3.Object.ETag, "\"", ""),
			Size:      record.S3.Object.Size,
		},
	}
}

func (s *S3Event) Bytes() []byte {
	b, _ := json.Marshal(s)
	return b
}

func (s *S3Event) String() string {
	b, _ := json.MarshalIndent(s, "", "    ")
	return string(b)
}

func (s *S3Event) NewS3Object() S3Object {
	return s.S3Object
}
