package main

import (
	"flag"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/brandonyeoxg/go-aws-pubsub/dispatcher"
	"github.com/brandonyeoxg/go-aws-pubsub/jobqueue"
)

func main() {
	sess, err := initAwsSession()

	if err != nil {
		fmt.Println("Error", err)
		return
	}

	dispatcher.Init(sess)
	jobqueue.Init(sess, dispatcher.QueueName)

	isDispatcher := flag.Bool("d", false, "if the current program is a dispatcher")
	flag.Parse()

	if *isDispatcher == true {
		fmt.Println("Running Dispatcher")
		dispatcher.Dispatch("Test Message")
	} else {
		fmt.Println("Running Jobqueue")
		// Consume from the queue
		jobqueue.Start()
	}
}

func initAwsSession() (client.ConfigProvider, error) {
	sess, err := session.NewSession(
		&aws.Config{
			Region:      aws.String("ap-southeast-1"),
			Credentials: credentials.NewSharedCredentials("", "bran-dev"),
		},
	)
	return sess, err
}

func genFakeMessages(n int) []string {
	var fakeMsg []string

	for i := 0; i < n; i++ {
		genMsg := fmt.Sprintf("Generated Test Message %d\n", i)
		fakeMsg = append(fakeMsg, genMsg)
	}

	return fakeMsg
}
