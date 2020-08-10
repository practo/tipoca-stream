package s3sink

package sinks

import (
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

	// outPutFormat is the format in which the data is stored in the s3 file
	outputFormat string
}

// NewS3Sink is the factory method constructing a new S3Sink
func NewS3Sink(
	awsAccessKeyID string,
	s3SinkSecretAccessKey string,
	s3SinkRegion string,
	s3SinkBucket string,
	s3SinkBucketDir string,
	outputFormat string) (*S3Sink, error) {

	awsConfig := &aws.Config{
		Region:      aws.String(s3SinkRegion),
		Credentials: credentials.NewStaticCredentials(
			awsAccessKeyID, s3SinkSecretAccessKey, ""),
	}

	awsConfig = awsConfig.WithCredentialsChainVerboseErrors(true)
	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, err
	}

	uploader := s3manager.NewUploader(sess)

	s := &S3Sink{
		uploader:       uploader,
		bucket:         s3SinkBucket,
		bucketDir:      s3SinkBucketDir,
		outputFormat:   outputFormat,
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
