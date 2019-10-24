package test

import (
	"fmt"
	rocketmq "github.com/apache/rocketmq-client-go/core"
	"os"
	"sync"
)

const (
	//topic
	Topic            = "XXXX"
	OrderTopic       = "XXXX"
	GlobalOrderTopic = "XXXX"
	MsgBody          = "test"
	//config info
	OnsNameserver         = "XXXX"
	OnsGroupId            = "XXXX"
	OnsOrderGroupId       = "XXXX"
	OnsGlobalOrderGroupId = "XXXX"
	AccessKey             = "XXXX"
	SecretKey             = "XXXX"
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

func createONSProducer() rocketmq.Producer {
	pConfig := &rocketmq.ProducerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:    OnsGroupId,
			NameServer: OnsNameserver,
			Credentials: &rocketmq.SessionCredentials{
				AccessKey: AccessKey,
				SecretKey: SecretKey,
				Channel:   "ALIYUN",
			},
		},
		ProducerModel: rocketmq.CommonProducer,
	}
	producer, _ := rocketmq.NewProducer(pConfig)
	return producer
}

func createRocketMQProducer() rocketmq.Producer {
	pConfig := &rocketmq.ProducerConfig{
		ClientConfig: rocketmq.ClientConfig{
			NameServer: rocketmqNameserver,
			GroupID:    "producer_group",
		},
		//Set to Common Producer as default.
		ProducerModel: rocketmq.CommonProducer,
	}

	producer, _ := rocketmq.NewProducer(pConfig)
	return producer
}

func createONSPushConsumer() rocketmq.PushConsumer {
	pConfig := &rocketmq.PushConsumerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:    OnsGroupId,
			NameServer: OnsNameserver,
			Credentials: &rocketmq.SessionCredentials{
				AccessKey: AccessKey,
				SecretKey: SecretKey,
				Channel:   "ALIYUN",
			},
		},
		Model:         rocketmq.Clustering,
		ConsumerModel: rocketmq.CoCurrently,
	}
	consumer, _ := rocketmq.NewPushConsumer(pConfig)
	return consumer
}

func createRocketMQPushConsumer() rocketmq.PushConsumer {
	pConfig := &rocketmq.PushConsumerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:    "consumerGroup",
			NameServer: rocketmqNameserver,
		},
		Model:         rocketmq.Clustering,
		ConsumerModel: rocketmq.CoCurrently,
	}
	consumer, _ := rocketmq.NewPushConsumer(pConfig)
	return consumer
}

func createONSBroadcastConsumerByInstanceName(instanceName string) rocketmq.PushConsumer {
	pConfig := &rocketmq.PushConsumerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:    OnsGroupId,
			NameServer: OnsNameserver,
			Credentials: &rocketmq.SessionCredentials{
				AccessKey: AccessKey,
				SecretKey: SecretKey,
				Channel:   "ALIYUN",
			},
			InstanceName: instanceName,
		},
		Model:         rocketmq.BroadCasting,
		ConsumerModel: rocketmq.CoCurrently,
	}
	consumer, _ := rocketmq.NewPushConsumer(pConfig)
	return consumer
}

func createRocketMQBroadcastConsumerByInstanceName(instanceName string) rocketmq.PushConsumer {
	pConfig := &rocketmq.PushConsumerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:      "consumerGroup",
			NameServer:   rocketmqNameserver,
			InstanceName: instanceName,
		},
		Model:         rocketmq.BroadCasting,
		ConsumerModel: rocketmq.CoCurrently,
	}
	consumer, _ := rocketmq.NewPushConsumer(pConfig)
	return consumer
}

func createONSPushConsumerByInstanceName(instanceName string) rocketmq.PushConsumer {
	pConfig := &rocketmq.PushConsumerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:    OnsGroupId,
			NameServer: OnsNameserver,
			Credentials: &rocketmq.SessionCredentials{
				AccessKey: AccessKey,
				SecretKey: SecretKey,
				Channel:   "ALIYUN",
			},
			InstanceName: instanceName,
		},
		Model:         rocketmq.Clustering,
		ConsumerModel: rocketmq.CoCurrently,
	}
	consumer, _ := rocketmq.NewPushConsumer(pConfig)
	return consumer
}

func createRocketMQPushConsumerByInstanceName(instanceName string) rocketmq.PushConsumer {
	pConfig := &rocketmq.PushConsumerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:      "consumerGroup",
			NameServer:   rocketmqNameserver,
			InstanceName: instanceName,
		},
		Model:         rocketmq.Clustering,
		ConsumerModel: rocketmq.CoCurrently,
	}
	consumer, _ := rocketmq.NewPushConsumer(pConfig)
	return consumer
}

func createONSPartitionOrderProducer() rocketmq.Producer {
	pConfig := &rocketmq.ProducerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:    OnsOrderGroupId,
			NameServer: OnsNameserver,
			Credentials: &rocketmq.SessionCredentials{
				AccessKey: AccessKey,
				SecretKey: SecretKey,
				Channel:   "ALIYUN",
			},
		},
		ProducerModel: rocketmq.OrderlyProducer,
	}
	producer, _ := rocketmq.NewProducer(pConfig)
	return producer
}

func createONSPartitionOrderPushConsumer() rocketmq.PushConsumer {
	pConfig := &rocketmq.PushConsumerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:    OnsOrderGroupId,
			NameServer: OnsNameserver,
			Credentials: &rocketmq.SessionCredentials{
				AccessKey: AccessKey,
				SecretKey: SecretKey,
				Channel:   "ALIYUN",
			},
		},
		Model:         rocketmq.Clustering,
		ConsumerModel: rocketmq.Orderly,
	}
	consumer, _ := rocketmq.NewPushConsumer(pConfig)
	return consumer
}

func createONSGlobalOrderProducer() rocketmq.Producer {
	pConfig := &rocketmq.ProducerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:    OnsGlobalOrderGroupId,
			NameServer: OnsNameserver,
			Credentials: &rocketmq.SessionCredentials{
				AccessKey: AccessKey,
				SecretKey: SecretKey,
				Channel:   "ALIYUN",
			},
		},
		ProducerModel: rocketmq.OrderlyProducer,
	}
	producer, _ := rocketmq.NewProducer(pConfig)
	return producer
}

func createONSGlobalOrderPushConsumer() rocketmq.PushConsumer {
	pConfig := &rocketmq.PushConsumerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:    OnsGlobalOrderGroupId,
			NameServer: OnsNameserver,
			Credentials: &rocketmq.SessionCredentials{
				AccessKey: AccessKey,
				SecretKey: SecretKey,
				Channel:   "ALIYUN",
			},
		},
		Model:         rocketmq.Clustering,
		ConsumerModel: rocketmq.Orderly,
	}
	consumer, _ := rocketmq.NewPushConsumer(pConfig)
	return consumer
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