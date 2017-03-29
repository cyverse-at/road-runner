package main

import (
	"os"

	"github.com/cyverse-de/logcabin"
	"github.com/cyverse-de/messaging"
	"github.com/cyverse-de/model"
)

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		logcabin.Error.Printf("Couldn't get the hostname: %s", err.Error())
		return ""
	}
	return h
}

func fail(client *messaging.Client, job *model.Job, msg string) error {
	logcabin.Error.Print(msg)
	return client.PublishJobUpdate(&messaging.UpdateMessage{
		Job:     job,
		State:   messaging.FailedState,
		Message: msg,
		Sender:  hostname(),
	})
}

func success(client *messaging.Client, job *model.Job) error {
	logcabin.Info.Print("Job success")
	return client.PublishJobUpdate(&messaging.UpdateMessage{
		Job:    job,
		State:  messaging.SucceededState,
		Sender: hostname(),
	})
}

func running(client *messaging.Client, job *model.Job, msg string) {
	err := client.PublishJobUpdate(&messaging.UpdateMessage{
		Job:     job,
		State:   messaging.RunningState,
		Message: msg,
		Sender:  hostname(),
	})
	if err != nil {
		logcabin.Error.Print(err)
	}
	logcabin.Info.Print(msg)
}

func impendingCancellation(client *messaging.Client, job *model.Job, msg string) {
	err := client.PublishJobUpdate(&messaging.UpdateMessage{
		Job:     job,
		State:   messaging.ImpendingCancellationState,
		Message: msg,
		Sender:  hostname(),
	})
	if err != nil {
		logcabin.Error.Print(err)
	}
	logcabin.Info.Print(msg)
}
