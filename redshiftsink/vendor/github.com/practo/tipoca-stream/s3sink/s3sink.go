package s3sink

import (
	"bytes"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// S3Sink is a library which can be used to upload data to s3
type S3Sink struct {
	// uploader client from aws which makes the API call to aws for upload
	uploader *s3manager.Uploader

	// bucket is the s3 bucket name to store data
	bucket string

	// bucketDir is the first level directory in
	// the bucket where the events would be stored
	bucketDir string
}

// NewS3Sink is the factory method constructing a new S3Sink
func NewS3Sink(
	awsAccessKeyID string,
	awsSecretAccessKey string,
	s3Region string,
	s3Bucket string,
	s3BucketDir string) (*S3Sink, error) {

	awsConfig := &aws.Config{
		Region:      aws.String(s3Region),
		Credentials: credentials.NewStaticCredentials(
			awsAccessKeyID, awsSecretAccessKey, ""),
	}

	awsConfig = awsConfig.WithCredentialsChainVerboseErrors(true)
	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, err
	}

	uploader := s3manager.NewUploader(sess)

	s := &S3Sink{
		uploader:       uploader,
		bucket:         s3Bucket,
		bucketDir:      s3BucketDir,
	}

	return s, nil
}

// upload uploads the data stored in buffer to s3 in the specified key
// and clears the buffer
func (s *S3Sink) upload(key string, bodyBuf *bytes.Buffer) error {
	_, err := s.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bodyBuf,
	})
	if err != nil {
		return err
	}
	return nil
}
