package test

import (
	rocketmq "github.com/apache/rocketmq-client-go/core"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPullConsumerCreateConfigNil(t *testing.T) {
	_, err := rocketmq.NewPullConsumer(nil)
	if err == nil {
		t.Fail()
	} else {
		assert.Equal(t, ConfigNilInfo, err.Error())
	}
}

func TestPullConsumerCreateLackNameServer(t *testing.T) {
	pConfig := &rocketmq.PullConsumerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID: "group",
		},
	}
	_, err := rocketmq.NewPullConsumer(pConfig)
	if err == nil {
		t.Fail()
	} else {
		assert.Equal(t, NameserverEmptyInfo, err.Error())
	}
}

func TestPullConsumerLackGroupId(t *testing.T) {
	pConfig := &rocketmq.PullConsumerConfig{
		ClientConfig: rocketmq.ClientConfig{
			NameServer: "localhost:9876",
		},
	}
	_, err := rocketmq.NewPullConsumer(pConfig)
	if err == nil {
		t.Fail()
	} else {
		assert.Equal(t, GroupIDEmptyInfo, err.Error())
	}
}
