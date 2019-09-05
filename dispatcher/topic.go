package dispatcher

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
)

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
