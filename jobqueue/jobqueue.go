package jobqueue

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws/client"
)

func Init(sess client.ConfigProvider) {
	fmt.Println("Jobqueue initialised")
}
