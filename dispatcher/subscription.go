package dispatcher

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func getSubscriptions() map[string]sns.SubscribeInput {
	return map[string]sns.SubscribeInput{
		Config.QueueName: sns.SubscribeInput{Protocol: aws.String("sqs"), TopicArn: aws.String(topicArn)},
	}
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
