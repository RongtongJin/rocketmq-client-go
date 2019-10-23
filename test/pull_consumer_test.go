package test

import (
	rocketmq "github.com/apache/rocketmq-client-go/core"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPullConsumerCreate(t *testing.T) {

	// config is nil
	{
		_, err := rocketmq.NewPullConsumer(nil)
		if err == nil {
			t.Fail()
		} else {
			assert.Equal(t, ConfigNilInfo, err.Error())
		}
	}

	// lack of nameserver
	{
		pConfig := &rocketmq.PullConsumerConfig{
			ClientConfig: rocketmq.ClientConfig{
				GroupID: "123456",
			},
		}
		_, err := rocketmq.NewPullConsumer(pConfig)
		if err == nil {
			t.Fail()
		} else {
			assert.Equal(t, NameserverEmptyInfo, err.Error())
		}
	}

	// lack of groupId
	{
		pConfig := &rocketmq.PullConsumerConfig{
			ClientConfig: rocketmq.ClientConfig{
				NameServer: "localhost:9876",
			},
		}
		_, err := rocketmq.NewPullConsumer(pConfig)
		if err == nil {
			t.Fail()
		} else {
			assert.Equal(t, GroupIdEmptyInfo, err.Error())
		}
	}
}

//
////FIXME
//func TestFetchSubscriptionMessageQueues(t *testing.T) {
//	consumer := createRocketMQPullConsumer()
//	err := consumer.Start()
//	if err != nil {
//		t.Fatal(err)
//	}
//	messagequeues := consumer.FetchSubscriptionMessageQueues("TopicTest")
//	t.Log(messagequeues)
//}
//
////FIXME
//func TestPullInterface(t *testing.T) {
//	consumer := createRocketMQPullConsumer()
//	_ = consumer.Start()
//	messagequeues := consumer.FetchSubscriptionMessageQueues("TopicTest")
//	t.Log(messagequeues)
//}
//
//func createRocketMQPullConsumer() rocketmq.PullConsumer {
//	pConfig := &rocketmq.PullConsumerConfig{
//		ClientConfig: rocketmq.ClientConfig{
//			GroupID:    "consumerGroup",
//			NameServer: "localhost:9876",
//		},
//	}
//	consumer, _ := rocketmq.NewPullConsumer(pConfig)
//	return consumer
//}
