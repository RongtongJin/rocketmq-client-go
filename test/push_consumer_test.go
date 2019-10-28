package test

import (
	rocketmq "github.com/apache/rocketmq-client-go/core"
	"github.com/stretchr/testify/assert"
	"testing"
)

// config is nil
func TestPushConsumerCreateConfigNil(t *testing.T) {
	_, err := rocketmq.NewPushConsumer(nil)
	if err == nil {
		t.Fail()
	} else {
		assert.Equal(t, ConfigNilInfo, err.Error())
	}
}

// lack of nameserver
func TestPushConsumerCreateLackNameServer(t *testing.T) {
	pConfig := &rocketmq.PushConsumerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID: "123456",
		},
		Model:         rocketmq.Clustering,
		ConsumerModel: rocketmq.CoCurrently,
	}
	_, err := rocketmq.NewPushConsumer(pConfig)
	if err == nil {
		t.Fail()
	} else {
		assert.Equal(t, NameserverEmptyInfo, err.Error())
	}
}

// lack groupId
func TestPushConsumerLackGroupId(t *testing.T) {
	pConfig := &rocketmq.PushConsumerConfig{
		ClientConfig: rocketmq.ClientConfig{
			NameServer: "localhost:9876",
		},
		Model:         rocketmq.Clustering,
		ConsumerModel: rocketmq.CoCurrently,
	}
	_, err := rocketmq.NewPushConsumer(pConfig)
	if err == nil {
		t.Fail()
	} else {
		assert.Equal(t, GroupIDEmptyInfo, err.Error())
	}
}

// FIXME lack of model
func TestPushConsumerCreateLackModel(t *testing.T) {

	pConfig := &rocketmq.PushConsumerConfig{
		ClientConfig: rocketmq.ClientConfig{
			NameServer: "localhost:9876",
			GroupID:    "producer_group",
		},
		//Model:         rocketmq.Clustering,
		ConsumerModel: rocketmq.CoCurrently,
	}
	producer, err := rocketmq.NewPushConsumer(pConfig)
	if err == nil {
		t.Fail()
	}
	if producer != nil {
		err = producer.Start()
		if err != nil {
			t.Error(err)
		}
	}
}

// FIXME lack of ConsumerModel
func TestPushConsumerCreateLackConsumerModel(t *testing.T) {
	pConfig := &rocketmq.PushConsumerConfig{
		ClientConfig: rocketmq.ClientConfig{
			NameServer: "localhost:9876",
			GroupID:    "producer_group",
		},
		Model: rocketmq.Clustering,
		//ConsumerModel: rocketmq.CoCurrently,
	}
	producer, err := rocketmq.NewPushConsumer(pConfig)
	if err == nil {
		t.Fail()
	}
	if producer != nil {
		err = producer.Start()
		if err != nil {
			t.Error(err.Error())
		}
	}
}

func TestConsumerSubscribeFuncNil(t *testing.T) {
	consumer, err := createRocketMQPushConsumer()
	if err != nil {
		t.Fatal(err.Error())
	}
	err = consumer.Subscribe("test", "*", nil)
	if err == nil {
		t.Fail()
	} else {
		assert.Equal(t, ConsumeFuncNil, err.Error())
	}

	_ = consumer.Start()
	defer consumer.Shutdown()
}

func TestConsumerStartBeforeSubscribe(t *testing.T) {
	consumer, err := createRocketMQPushConsumer()
	if err != nil {
		t.Fatal(err.Error())
	}
	err = consumer.Start()
	defer consumer.Shutdown()
	if err == nil {
		t.Fail()
	} else {
		t.Log(err)
	}
}
