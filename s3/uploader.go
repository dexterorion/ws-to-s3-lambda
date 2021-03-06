package s3

import (
	"bytes"
	"fmt"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	S3_REGION  = os.Getenv("BUCKET_REGION")
	AWS_KEY    = os.Getenv("ACCESS_KEY")
	AWS_SECRET = os.Getenv("SECRET_ACCESS")
)

func Upload(bucket string, filename string, buffer []byte) error {
	s, err := session.NewSession(&aws.Config{
		Region:      aws.String(S3_REGION),
		Credentials: credentials.NewStaticCredentials(AWS_KEY, AWS_SECRET, ""),
	})
	if err != nil {
		return fmt.Errorf("Error creating aws session: %s", err.Error())
	}

	_, err = s3.New(s).PutObject(&s3.PutObjectInput{
		Bucket:               aws.String(bucket),
		Key:                  aws.String(filename),
		ACL:                  aws.String("private"),
		Body:                 bytes.NewReader(buffer),
		ContentType:          aws.String(http.DetectContentType(buffer)),
		ContentDisposition:   aws.String("attachment"),
		ServerSideEncryption: aws.String("AES256"),
	})

	if err != nil {
		return fmt.Errorf("Error putting file on bucket: %s", err.Error())
	}

	return nil
}
