package dispatcher

import (
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sts"

	"github.com/spf13/viper"
)

var snsSvc *sns.SNS
var sqsSvc *sqs.SQS
var stsSvc *sts.STS

var accountID string

var topicArn string
var queueArn string

var Config dispatcherConfig

type dispatcherConfig struct {
	Region     string `mapstructure:"aws_region" validate:"true"`
	SqsTimeout int    `mapstructure:"aws_sqs_timeout" validate:"true"`
	TopicName  string `mapstructure:"aws_sns_topic_name" validate:"true"`
	QueueName  string `mapstructure:"aws_sqs_queue_name" validate:"true"`
}

type Messager interface {
	Message() (map[string]*sns.MessageAttributeValue, string)
}

type StartJobAttribute struct {
	JobID     string `json:"jobID"`
	EngineID  string `json:"engineID"`
	ProjectID string `json:"projectID"`
}

type DefaultStartJobMessage struct {
	StartJobAttribute
}

func (msg *DefaultStartJobMessage) Message() (map[string]*sns.MessageAttributeValue, string) {
	msgAttributes := make(map[string]*sns.MessageAttributeValue)
	msgAttributes["jobID"] = &sns.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(msg.JobID)}
	msgAttributes["engineID"] = &sns.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(msg.EngineID)}
	msgAttributes["projectID"] = &sns.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(msg.ProjectID)}
	return msgAttributes, "start"
}

// Init handles the startup of required infrastructure for the pub sub to work
// If AWS sns is not setup, it will create it.
func Init(sess client.ConfigProvider) {

	if err := loadConfig(); err != nil {
		fmt.Println("Unable to loadConfig:", err)
		return
	}

	snsSvc = sns.New(sess)
	sqsSvc = sqs.New(sess)
	stsSvc = sts.New(sess)

	if err := getCallerIdentity(); err != nil {
		fmt.Println("Unable to getCallerIdentity:", err)
		return
	}

	if err := initTopic(Config.TopicName); err != nil {
		fmt.Println("Unable to initalise Topic", Config.TopicName)
		return
	}

	subscriptions := getSubscriptions()
	if err := initSubscription(subscriptions); err != nil {
		fmt.Println("Unable to initialise Subscription", Config.QueueName)
		return
	}

	fmt.Println("Dispatcher initialised")
}

func Dispatch(msg Messager) {
	msgAttrs, rawmsg := msg.Message()
	pubRes, err := snsSvc.Publish(&sns.PublishInput{
		MessageAttributes: msgAttrs,
		Message:           aws.String(rawmsg),
		TopicArn:          aws.String(topicArn),
	})

	if err != nil {
		fmt.Println("Error Dispatch:", err)
	}

	fmt.Println("Dispatched message", *pubRes.MessageId)
}

func RunDemo() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case t := <-ticker.C:
			jobMsg := DefaultStartJobMessage{
				StartJobAttribute{EngineID: "default", ProjectID: "default", JobID: fmt.Sprintf("Text Message %d", t.Unix())},
			}
			Dispatch(&jobMsg)
		}
	}
}

func loadConfig() error {
	if err := viper.UnmarshalKey("dispatcher", &Config); err != nil {
		return err
	}
	fmt.Println("Config", Config)
	return nil
}

func getCallerIdentity() error {
	result, err := stsSvc.GetCallerIdentity(&sts.GetCallerIdentityInput{})
	if err != nil {
		fmt.Println("Error getCalleridentity:", err)
		return err
	}

	accountID = *result.Account
	return nil
}

func createQueue(queueName string, input sns.SubscribeInput) (string, error) {
	queueArn = fmt.Sprintf("arn:aws:sqs:%s:%s:%s", Config.Region, accountID, queueName)
	res, err := sqsSvc.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(queueName),

		Attributes: map[string]*string{
			"DelaySeconds":                  aws.String("60"),
			"MessageRetentionPeriod":        aws.String("86400"),
			"ReceiveMessageWaitTimeSeconds": aws.String(strconv.Itoa(Config.SqsTimeout)),
			"Policy": aws.String(fmt.Sprintf(`{
				"Version": "2012-10-17",
				"Statement": [
						{
								"Sid": "SNSTopicSendMessage",
								"Effect": "Allow",
								"Principal": "*",
								"Action": "SQS:SendMessage",
								"Resource": "%s",
								"Condition": {
										"ArnEquals": {
												"aws:SourceArn": "%s"
										}
								}
						}
				]
			}`, queueArn, topicArn)),
		},
	})
	if err != nil {
		fmt.Println("Error createQueue:", err)
		return "", err
	}

	fmt.Println("SQS created")

	return *res.QueueUrl, nil
}

func isResourceExist(arn string, wantName string) (bool, error) {
	name, err := getResourceFromARN(arn)
	if err != nil {
		return false, err
	}
	if name == wantName {
		return true, nil
	}
	return false, nil
}

func getResourceFromARN(rawArn string) (string, error) {
	arnObj, err := arn.Parse(rawArn)
	return arnObj.Resource, err
}
