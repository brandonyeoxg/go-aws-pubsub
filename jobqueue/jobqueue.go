package jobqueue

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/spf13/viper"
)

var accountID string

var queueName string
var queueURL string

var sqsSvc *sqs.SQS

var localDispatcher dispatcher

type jobqueueConfig struct {
	WorkerNum        int   `mapstructure:"worker_num"`
	PollingDelay     int   `mapstructure:"queue_polling_delay"`
	MaxJobPerRequest int64 `mapstructure:"max_job_per_request"`
}

var config jobqueueConfig

type MessageAttributeValue struct {
	DataType string `json:"Type"`
	Value    string `json:"Value"`
}

type MessageBodyField struct {
	Type           string    `json:"Notification"`
	MessageID      string    `json:"MessageID"`
	TopicARN       string    `json:"TopicArn"`
	Message        string    `json:"Message"`
	Timestamp      time.Time `json:"Timestamp"`
	UnsubscribeURL string    `json:"UnsubscribeURL"`

	// SNS published MessageAttributes resides inthe body of the message
	// https://stackoverflow.com/a/44243054
	MessageAttributes map[string]MessageAttributeValue `json:"MessageAttributes"`
}

func Init(sess client.ConfigProvider, targetQueue string) {
	if err := loadConfig(); err != nil {
		fmt.Println("Error unable to load config")
		return
	}
	queueName = viper.GetString("dispatcher.aws_sqs_queue_name")
	sqsSvc = sqs.New(sess)

	resultURL, err := sqsSvc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})

	if err != nil {
		fmt.Println("Error jobqueue Init", err)
	}

	queueURL = *resultURL.QueueUrl

	// Init the rest of the work queue
	localDispatcher = newDispatcher(config.WorkerNum)
	fmt.Println("Jobqueue initialised")
}

func Start() {
	ticker := time.NewTicker(time.Duration(config.PollingDelay) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			handleJob()
		}
	}
}

func loadConfig() error {
	if err := viper.UnmarshalKey("jobqueue", &config); err != nil {
		fmt.Println("Error loadConfig:", err)
		return err
	}
	return nil
}

func handleJob() {
	// If there is no job queueing then we dispatch to local worker
	// If there is a hungry worker, we try to receive from SQS
	// Once consumed, we will just delete from the SQS
	res, err := sqsSvc.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl: aws.String(queueURL),
		AttributeNames: aws.StringSlice([]string{
			"SentTimeStamp",
		}),
		MaxNumberOfMessages: aws.Int64(config.MaxJobPerRequest),
		MessageAttributeNames: aws.StringSlice([]string{
			"All",
		}),
	})
	if err != nil {
		fmt.Println("Error Start:", err)
		return
	}

	fmt.Printf("Received %d messages.\n", len(res.Messages))

	if len(res.Messages) > 0 {
		// Checks if the job is one that start
		var msgfield MessageBodyField
		json.Unmarshal([]byte(*res.Messages[0].Body), &msgfield)

		fmt.Println("Msgfield", msgfield)
		switch msgfield.Message {
		case "start":
			// Start transcription
			startTranscription(msgfield.MessageAttributes)
		default:
			fmt.Println("Error handleJob: invalid message found")
		}
		// Push into the localDispatcher to be processed
		localDispatcher.Work <- &transcriptionWork{JobID: *res.Messages[0].Body}

		// Delete the message
		resDel, err := sqsSvc.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      aws.String(queueURL),
			ReceiptHandle: res.Messages[0].ReceiptHandle,
		})
		if err != nil {
			fmt.Println("Error Start:", err)
			return
		}
		fmt.Println("Message Deleted:", resDel)
	}
}

func startTranscription(msgAttrs map[string]MessageAttributeValue) {
	// fmt.Println("Dispatching with transcriptionJob", msgAttrs["jobID"].Value)
	localDispatcher.Work <- &transcriptionWork{JobID: msgAttrs["jobID"].Value}
}
