package jobqueue

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var accountID string

var queueName string
var queueURL string

var sqsSvc *sqs.SQS

func Init(sess client.ConfigProvider, targetQueue string) {
	fmt.Println("Jobqueue initialised")
	queueName = targetQueue
	sqsSvc = sqs.New(sess)

	resultURL, err := sqsSvc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})

	if err != nil {
		fmt.Println("Error jobqueue Init", err)
	}

	queueURL = *resultURL.QueueUrl

	// Init the rest of the work queue
}

func Start() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			res, err := sqsSvc.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueUrl: aws.String(queueURL),
				AttributeNames: aws.StringSlice([]string{
					"SentTimeStamp",
				}),
				MaxNumberOfMessages: aws.Int64(1),
				MessageAttributeNames: aws.StringSlice([]string{
					"All",
				}),
			})
			if err != nil {
				fmt.Println("Error Start:", err)
				continue
			}

			fmt.Printf("Received %d messages.\n", len(res.Messages))

			if len(res.Messages) > 0 {
				fmt.Println(res.Messages)
				// Delete the message
				resDel, err := sqsSvc.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      aws.String(queueURL),
					ReceiptHandle: res.Messages[0].ReceiptHandle,
				})
				if err != nil {
					fmt.Println("Error Start:", err)
					continue
				}
				fmt.Println("Message Deleted:", resDel)
			}
		}
	}
}
