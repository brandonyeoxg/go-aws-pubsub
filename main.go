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
	jobqueue.Init(sess)

	isDispatcher := flag.Bool("Is dispatcher", true, "if the current program is a dispatcher")
	flag.Parse()

	if *isDispatcher {
		dispatcher.Dispatch("Test Message")
	} else {

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
