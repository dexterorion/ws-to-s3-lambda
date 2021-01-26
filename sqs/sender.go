package sqs

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"k8s.io/apimachinery/pkg/util/json"
)

var (
	SQS_REGION = os.Getenv("SQS_REGION")
	SQS_URL    = os.Getenv("SQS_URL")
	AWS_KEY    = os.Getenv("ACCESS_KEY")
	AWS_SECRET = os.Getenv("SECRET_ACCESS")
)

type UploadedFileMessage struct {
	Bucket      string `json:"bucket"`
	Filename    string `json:"filename"`
	Type        string `json:"type"`
	ParkingID   string `json:"parkingID"`
	ParkingName string `json:"parkingName"`
	ParkingSlug string `json:"parkingSlug"`
}

func (fm *UploadedFileMessage) Send() error {
	s, err := session.NewSession(&aws.Config{
		Region:      aws.String(SQS_REGION),
		Credentials: credentials.NewStaticCredentials(AWS_KEY, AWS_SECRET, ""),
	})
	if err != nil {
		return fmt.Errorf("Error creating aws session: %s", err.Error())
	}

	data, err := json.Marshal(fm)
	if err != nil {
		return fmt.Errorf("Error parsing data: %s", err.Error())
	}

	_, err = sqs.New(s).SendMessage(&sqs.SendMessageInput{
		MessageBody:            aws.String(string(data)),
		MessageGroupId:         aws.String(fm.ParkingSlug),
		MessageDeduplicationId: aws.String(fm.ParkingSlug),
		QueueUrl:               aws.String(SQS_URL),
	})

	if err != nil {
		return fmt.Errorf("Error sending message to queue: %s", err.Error())
	}

	return nil
}
