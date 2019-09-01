package main

import (
	"github.com/brandonyeoxg/go-aws-pubsub/dispatcher"
	"github.com/brandonyeoxg/go-aws-pubsub/jobqueue"
)

func main() {
	dispatcher.Init()
	jobqueue.Init()
}
