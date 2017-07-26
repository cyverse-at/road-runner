package main

import (
	"errors"
	"testing"

	"github.com/cyverse-de/messaging"
	"github.com/cyverse-de/model"
)

type TestJobUpdatePublisher struct {
	fail    bool
	updates []*messaging.UpdateMessage
}

func NewTestJobUpdatePublisher(fail bool) *TestJobUpdatePublisher {
	return &TestJobUpdatePublisher{
		fail:    fail,
		updates: []*messaging.UpdateMessage{},
	}
}

func (j *TestJobUpdatePublisher) PublishJobUpdate(m *messaging.UpdateMessage) error {
	if j.fail {
		return errors.New("failed to publish job update")
	}
	j.updates = append(j.updates, m)
	return nil
}

func TestFail(t *testing.T) {
	j := NewTestJobUpdatePublisher(false)
	job := &model.Job{InvocationID: "test-id"}
	err := fail(j, job, "test message")
	if err != nil {
		t.Error(err)
	}
	expectedlen := 1
	actuallen := len(j.updates)
	if actuallen != expectedlen {
		t.Errorf("length of updates was %d instead of %d", actuallen, expectedlen)
	}
	actualid := j.updates[0].InvocationID
	expectedid := "test-id"
	if actualid != expectedid {
		t.Errorf("invocation id was %s instead of %s", actualid, expectedid)
	}
	actualstate := j.updates[0].State
	expectedstate := messaging.FailedState
	if actualstate != expectedstate {
		t.Errorf("state was %s instead of %s", actualstate, expectedstate)
	}
	actualmsg := j.updates[0].Message
	expectedmsg := "test message"
	if actualmsg != expectedmsg {
		t.Errorf("message was %s instead of %s", actualmsg, expectedmsg)
	}
}

func TestSuccess(t *testing.T) {
	j := NewTestJobUpdatePublisher(false)
	job := &model.Job{InvocationID: "test-id"}
	err := success(j, job)
	if err != nil {
		t.Error(err)
	}
	expectedlen := 1
	actuallen := len(j.updates)
	if actuallen != expectedlen {
		t.Errorf("length of updates was %d instead of %d", actuallen, expectedlen)
	}
	actualid := j.updates[0].InvocationID
	expectedid := "test-id"
	if actualid != expectedid {
		t.Errorf("invocation id was %s instead of %s", actualid, expectedid)
	}
	actualstate := j.updates[0].State
	expectedstate := messaging.SucceededState
	if actualstate != expectedstate {
		t.Errorf("state was %s instead of %s", actualstate, expectedstate)
	}
}

func TestRunning(t *testing.T) {
	j := NewTestJobUpdatePublisher(false)
	job := &model.Job{InvocationID: "test-id"}
	running(j, job, "test message")
	expectedlen := 1
	actuallen := len(j.updates)
	if actuallen != expectedlen {
		t.Errorf("length of updates was %d instead of %d", actuallen, expectedlen)
	}
	actualid := j.updates[0].InvocationID
	expectedid := "test-id"
	if actualid != expectedid {
		t.Errorf("invocation id was %s instead of %s", actualid, expectedid)
	}
	actualstate := j.updates[0].State
	expectedstate := messaging.RunningState
	if actualstate != expectedstate {
		t.Errorf("state was %s instead of %s", actualstate, expectedstate)
	}
	actualmsg := j.updates[0].Message
	expectedmsg := "test message"
	if actualmsg != expectedmsg {
		t.Errorf("message was %s instead of %s", actualmsg, expectedmsg)
	}
}
