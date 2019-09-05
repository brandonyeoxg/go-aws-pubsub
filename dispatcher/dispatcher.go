package dispatcher

import (
	"fmt"

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

const TopicName string = "sns-dispatch"
const QueueName string = "sqs-jobqueue"

// Init handles the startup of required infrastructure for the pub sub to work
// If AWS sns is not setup, it will create it.
func Init(sess client.ConfigProvider) {
	snsSvc = sns.New(sess)
	sqsSvc = sqs.New(sess)
	stsSvc = sts.New(sess)

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
	snsSvc.Publish(&sns.PublishInput{
		Message: aws.String(msg),
	})

	fmt.Println("Dispatched message")
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

func initSubscription(subscriptions map[string]sns.SubscribeInput) error {
	res, err := snsSvc.ListSubscriptionsByTopic(
		&sns.ListSubscriptionsByTopicInput{
			TopicArn: aws.String(topicArn),
		},
	)
	if err != nil {
		fmt.Println("Error initSubscription:", err)
		return err
	}

	for _, subscription := range res.Subscriptions {
		endpointName, err := getResourceFromARN(*subscription.Endpoint)
		// nameIdx := strings.LastIndex(subscriptionName, ":")
		// if nameIdx != -1 {
		// 	subscriptionName = subscriptionName[:nameIdx]
		// }
		fmt.Println("Endpoint name is:", endpointName)
		if err != nil {
			return err
		}

		_, ok := subscriptions[endpointName]

		// Check if subscription is online
		switch *subscription.Protocol {
		case "sqs":
			lres, err := sqsSvc.ListQueues(
				&sqs.ListQueuesInput{
					QueueNamePrefix: aws.String(endpointName),
				},
			)
			if err != nil {
				fmt.Println("Error initSubscription: sqs.ListQueues", err)
				return err
			}
			if len(lres.QueueUrls) == 0 {
				// Remove the subscription, could be an old subscription
				snsSvc.Unsubscribe(&sns.UnsubscribeInput{
					SubscriptionArn: subscription.SubscriptionArn,
				})
				ok = false
				fmt.Println("Unsubscribing from topic with SubscriptionArn:", *subscription.SubscriptionArn)
			}
		}

		if ok {
			delete(subscriptions, endpointName)
			fmt.Println("Skipping creation of subscription", endpointName)
		}
	}

	// Create subscriptions
	for k, v := range subscriptions {
		switch *v.Protocol {
		case "sqs":
			_, err := createQueue(k, v)
			if err != nil {
				return err
			}
			subscribeToTopic(v, queueArn)
		}
	}

	return nil
}

func subscribeToTopic(input sns.SubscribeInput, resourceArn string) error {
	input.Endpoint = aws.String(resourceArn)
	res, err := snsSvc.Subscribe(&input)
	if err != nil {
		fmt.Println("Error subscribeToTopic:", err)
		return err
	}
	fmt.Println("Subscribed Resource ARN:", resourceArn, "to Topic ARN:", *input.TopicArn, "with Subscription ARN:", *res.SubscriptionArn)
	return nil
}

func createQueue(queueName string, input sns.SubscribeInput) (string, error) {
	queueArn = fmt.Sprintf("arn:aws:sqs:%s:%s:%s", region, accountID, queueName)
	res, err := sqsSvc.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		Attributes: map[string]*string{
			"DelaySeconds":           aws.String("60"),
			"MessageRetentionPeriod": aws.String("86400"),
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

func getSubscriptions() map[string]sns.SubscribeInput {
	return map[string]sns.SubscribeInput{
		QueueName: sns.SubscribeInput{Protocol: aws.String("sqs"), TopicArn: aws.String(topicArn)},
	}
}

func initTopic(name string) error {
	res, err := snsSvc.ListTopics(nil)

	if err != nil {
		topicArn, err = createTopic(name)
		if err != nil {
			fmt.Println("Error initTopic:", err)
			return err
		}
	}

	for _, t := range res.Topics {
		hasTopic, err := isResourceExist(*t.TopicArn, TopicName)
		if err != nil {
			return err
		}
		if hasTopic {
			topicArn = *t.TopicArn
			fmt.Println("Skipping creation of Topic", TopicName)
			return nil
		}
	}

	// if we cant find the topic name, we will have to create
	topicArn, err = createTopic(name)
	fmt.Println("New Topic", topicArn, "created!")
	if err != nil {
		return err
	}

	return nil
}

func createTopic(name string) (string, error) {
	res, err := snsSvc.CreateTopic(
		&sns.CreateTopicInput{
			Name: aws.String(name),
		},
	)
	if err != nil {
		fmt.Println("Error", err)
		return "", err
	}

	return *res.TopicArn, nil
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
