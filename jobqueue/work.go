package jobqueue

import (
	"fmt"
	"time"
)

type transcriptionWork struct {
	JobID string
}

func (work *transcriptionWork) Process() error {
	fmt.Println("Do transcripton work on Job:", work.JobID)
	time.Sleep(5 * time.Second) // Arbitrary processing time
	return nil
}
