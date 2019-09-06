package main

import (
	"flag"
	"fmt"

	"github.com/brandonyeoxg/go-aws-pubsub/dispatcher"
	"github.com/brandonyeoxg/go-aws-pubsub/jobqueue"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/spf13/viper"
)

func main() {
	if err := initConfig(); err != nil {
		return
	}

	sess, err := initAwsSession()

	if err != nil {
		fmt.Println("Error", err)
		return
	}

	isDispatcher := flag.Bool("d", false, "if the current program is a dispatcher")
	flag.Parse()

	if *isDispatcher == true {
		dispatcher.Init(sess)
		fmt.Println("Running Dispatcher")
		// dispatcher.Dispatch("Test Message")
		// dispatcher.RunDemo()
	} else {
		jobqueue.Init(sess, dispatcher.Config.QueueName)
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

func initConfig() error {
	viper.SetConfigName("config")
	viper.AddConfigPath("./")
	viper.SetConfigType("toml")

	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("Error initConfig:", err)
		return err
	}
	return nil
}
