package test

import (
	"fmt"
	rocketmq "github.com/apache/rocketmq-client-go/core"
	"os"
	"sync"
)

const (
	//topic info
	NormalTopic         = "go_normal_test"
	OneWayTopic         = "go_oneway_test"
	RebalanceTopic      = "go_rebalance_test"
	BroadcastTopic      = "go_broadcast_test"
	TagFilterTopic      = "go_tag_filter_test"
	PartitionOrderTopic = "go_partition_order_test"
	GlobalOrderTopic    = "go_global_order_test"
	DelayTopic          = "go_delay_test"
	TransactionTopic    = "go_transaction_test"
	//error info
	ConfigNilInfo       = "config is nil"
	GroupIdEmptyInfo    = "GroupId is empty"
	NameserverEmptyInfo = "NameServer and NameServerDomain is empty"
	ConsumeFuncNil      = "consumeFunc is nil"
	//tag
	TagA = "tagA"
	TagC = "tagC"
)

var rocketmqNameserver string

func init() {
	rocketmqNameserver = os.Getenv("NAMESRV_ADDR")
	if rocketmqNameserver == "" {
		rocketmqNameserver = "localhost:9876"
	}
}

func createMessage(topic, body string) *rocketmq.Message {
	msg := fmt.Sprintf("%s", body)
	return &rocketmq.Message{Topic: topic, Body: msg}
}

func createTagMessage(topic, body, tag string) *rocketmq.Message {
	msg := fmt.Sprintf("%s", body)
	return &rocketmq.Message{Topic: topic, Body: msg, Tags: tag}
}

func createRocketMQProducer() (rocketmq.Producer, error) {
	pConfig := &rocketmq.ProducerConfig{
		ClientConfig: rocketmq.ClientConfig{
			NameServer: rocketmqNameserver,
			GroupID:    "producer_group",
		},
		//Set to Common Producer as default.
		ProducerModel: rocketmq.CommonProducer,
	}

	producer, err := rocketmq.NewProducer(pConfig)
	return producer, err
}

func createRocketMQOrderlyProducer() (rocketmq.Producer, error) {
	pConfig := &rocketmq.ProducerConfig{
		ClientConfig: rocketmq.ClientConfig{
			NameServer: rocketmqNameserver,
			GroupID:    "producer_group",
		},
		//Set to Common Producer as default.
		ProducerModel: rocketmq.OrderlyProducer,
	}

	producer, err := rocketmq.NewProducer(pConfig)
	return producer, err
}

func createRocketMQPushConsumer() (rocketmq.PushConsumer, error) {
	pConfig := &rocketmq.PushConsumerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:    "consumerGroup",
			NameServer: rocketmqNameserver,
		},
		Model:         rocketmq.Clustering,
		ConsumerModel: rocketmq.CoCurrently,
	}
	consumer, err := rocketmq.NewPushConsumer(pConfig)
	return consumer, err
}

func createRocketMQPullConsumer() (rocketmq.PullConsumer, error) {
	pConfig := &rocketmq.PullConsumerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:    "consumerGroup",
			NameServer: "localhost:9876",
		},
	}
	consumer, err := rocketmq.NewPullConsumer(pConfig)
	return consumer, err
}

func createRocketMQBroadcastConsumerByInstanceName(instanceName string) (rocketmq.PushConsumer, error) {
	pConfig := &rocketmq.PushConsumerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:      "consumerGroup",
			NameServer:   rocketmqNameserver,
			InstanceName: instanceName,
		},
		Model:         rocketmq.BroadCasting,
		ConsumerModel: rocketmq.CoCurrently,
	}
	consumer, err := rocketmq.NewPushConsumer(pConfig)
	return consumer, err
}

func createRocketMQPushConsumerByInstanceName(instanceName string) (rocketmq.PushConsumer, error) {
	pConfig := &rocketmq.PushConsumerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:      "consumerGroup",
			NameServer:   rocketmqNameserver,
			InstanceName: instanceName,
		},
		Model:         rocketmq.Clustering,
		ConsumerModel: rocketmq.CoCurrently,
	}
	consumer, err := rocketmq.NewPushConsumer(pConfig)
	return consumer, err
}

func createRocketMQTransactionProducer(listener rocketmq.TransactionLocalListener, arg interface{}) (rocketmq.TransactionProducer, error) {
	config := &rocketmq.ProducerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:    "transcation_group",
			NameServer: rocketmqNameserver,
		},
		ProducerModel: rocketmq.TransProducer,
	}
	producer, err := rocketmq.NewTransactionProducer(config, listener, arg)
	return producer, err
}

func createRocketMQPartitionOrderProducer() (rocketmq.Producer, error) {
	pConfig := &rocketmq.ProducerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:    "OnsGroupId",
			NameServer: rocketmqNameserver,
		},
		ProducerModel: rocketmq.OrderlyProducer,
	}
	producer, err := rocketmq.NewProducer(pConfig)
	return producer, err
}

func createRocketMQPartitionOrderPushConsumer() (rocketmq.PushConsumer, error) {
	pConfig := &rocketmq.PushConsumerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:    "OnsGroupId",
			NameServer: rocketmqNameserver,
		},
		Model:         rocketmq.Clustering,
		ConsumerModel: rocketmq.Orderly,
	}
	consumer, err := rocketmq.NewPushConsumer(pConfig)
	return consumer, err
}

func createRocketMQGlobalOrderProducer() (rocketmq.Producer, error) {
	pConfig := &rocketmq.ProducerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:    "OnsGroupId",
			NameServer: rocketmqNameserver,
		},
		ProducerModel: rocketmq.OrderlyProducer,
	}
	producer, err := rocketmq.NewProducer(pConfig)
	return producer, err
}

func createRocketMQGlobalOrderPushConsumer() (rocketmq.PushConsumer, error) {
	pConfig := &rocketmq.PushConsumerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:    "OnsGroupId",
			NameServer: rocketmqNameserver,
		},
		Model:         rocketmq.Clustering,
		ConsumerModel: rocketmq.Orderly,
	}
	consumer, err := rocketmq.NewPushConsumer(pConfig)
	return consumer, err
}

func checkMap(receiveMap sync.Map) bool {
	flag := true
	receiveMap.Range(func(k, v interface{}) bool {
		if v == false {
			flag = false
		}
		return true
	})
	return flag
}
