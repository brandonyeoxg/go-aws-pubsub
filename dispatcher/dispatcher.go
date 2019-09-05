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
)

var snsSvc *sns.SNS
var sqsSvc *sqs.SQS
var stsSvc *sts.STS

var accountID string
var region string = "ap-southeast-1"

var topicArn string
var queueArn string

var timeout int

const TopicName string = "sns-dispatch"
const QueueName string = "sqs-jobqueue"

// Init handles the startup of required infrastructure for the pub sub to work
// If AWS sns is not setup, it will create it.
func Init(sess client.ConfigProvider) {
	snsSvc = sns.New(sess)
	sqsSvc = sqs.New(sess)
	stsSvc = sts.New(sess)

	timeout = 20

	if err := getCallerIdentity(); err != nil {
		fmt.Println("Unable to getCallerIdentity:", err)
		return
	}

	if err := initTopic(TopicName); err != nil {
		fmt.Println("Unable to initalise Topic", TopicName)
		return
	}

	subscriptions := getSubscriptions()
	if err := initSubscription(subscriptions); err != nil {
		fmt.Println("Unable to initialise Subscription", QueueName)
		return
	}

	fmt.Println("Dispatcher initialised")
}

func Dispatch(msg string) {
	pubRes, err := snsSvc.Publish(&sns.PublishInput{
		Message:  aws.String(msg),
		TopicArn: aws.String(topicArn),
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
			Dispatch(fmt.Sprintf("Test Message %d", t.Unix()))
		}
	}
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
	queueArn = fmt.Sprintf("arn:aws:sqs:%s:%s:%s", region, accountID, queueName)
	res, err := sqsSvc.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(queueName),

		Attributes: map[string]*string{
			"DelaySeconds":                  aws.String("60"),
			"MessageRetentionPeriod":        aws.String("86400"),
			"ReceiveMessageWaitTimeSeconds": aws.String(strconv.Itoa(timeout)),
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
