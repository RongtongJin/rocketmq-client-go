package test

import (
	rocketmq "github.com/apache/rocketmq-client-go/core"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConsumer(t *testing.T) {
	//lack of model
	{
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
				t.Error(err)
			}
		}
	}
}

func TestPushConsumerCreate(t *testing.T) {

	// config is nil
	{
		_, err := rocketmq.NewPushConsumer(nil)
		if err == nil {
			t.Fail()
		} else {
			assert.Equal(t, ConfigNilInfo, err.Error())
		}
	}

	// lack of nameserver
	{
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

	// lack of groupId
	{
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
			assert.Equal(t, GroupIdEmptyInfo, err.Error())
		}
	}

	// FIXME
	//lack of model
	{
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

	// FIXME
	// lack of consumer model
	{
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
				t.Error(err)
			}
		}
	}
}

//FIXME
func TestConsumerSubscribe(t *testing.T) {
	consumer := createRocketMQPushConsumer()
	//err := consumer.Subscribe("", "", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
	//	return rocketmq.ConsumeSuccess
	//})
	//if err == nil {
	//	t.Fail()
	//} else {
	//	t.Logf("err is %s", err)
	//}

	err := consumer.Subscribe("test", "*", nil)
	if err == nil {
		t.Fail()
	} else {
		assert.Equal(t, ConsumeFuncNil, err.Error())
	}

	_ = consumer.Start()
	defer consumer.Shutdown()
}

func TestConsumerStartBeforeSubscribe(t *testing.T) {
	consumer := createRocketMQPushConsumer()
	err := consumer.Start()
	defer consumer.Shutdown()
	if err == nil {
		t.Fail()
	} else {
		t.Log(err)
	}
}
