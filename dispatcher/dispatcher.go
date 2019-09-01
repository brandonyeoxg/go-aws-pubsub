package dispatcher

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/sns"
)

var snsSvc *sns.SNS
var topicArn string

const TopicName string = "sns-dispatch"

// Init handles the startup of required infrastructure for the pub sub to work
// If AWS sns is not setup, it will create it.
func Init(sess client.ConfigProvider) {
	snsSvc = sns.New(sess)
	initTopic(TopicName)
	fmt.Println("Dispatcher initialised")
}

func initTopic(name string) error {
	res, err := snsSvc.ListTopics(nil)

	if err != nil {
		fmt.Println("Error", err)
		return err
	}

	for _, t := range res.Topics {
		hasTopic, err := isTopicExist(t, TopicName)
		if err != nil {
			return err
		}
		if hasTopic {
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

func isTopicExist(topic *sns.Topic, wantName string) (bool, error) {
	name, err := getTopicName(*topic.TopicArn)
	if err != nil {
		return false, err
	}
	if name == wantName {
		return true, nil
	}
	return false, nil
}

func getTopicName(topicArn string) (string, error) {
	idx := strings.LastIndex(topicArn, TopicName)
	if idx == -1 {
		return "", fmt.Errorf("error topic name not found")
	}
	return topicArn[idx:], nil
}
