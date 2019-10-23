package test

import (
	rocketmq "github.com/apache/rocketmq-client-go/core"
	"testing"
)

func TestRocketMQSendSyncAndReceive(t *testing.T) {
	flag := false
	ch := make(chan interface{})
	producer := createRocketMQProducer()
	err := producer.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Shutdown()
	res, err := producer.SendMessageSync(createMessage("sendAndReceive", "sendAndReceive"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != rocketmq.SendOK {
		t.Fatalf("send message fail")
	}
	consumer := createRocketMQPushConsumer()
	consumer.Subscribe("sendAndReceive", "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Log(msg.Body)
		if msg.Body == "sendAndReceive" {
			flag = true
		}
		ch <- "done"
		return rocketmq.ConsumeSuccess
	})
	err = consumer.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Shutdown()
	<-ch
	if !flag {
		t.Errorf("send sync and receive test fail")
	}
}

func TestRocketMQSendOnewayAndReceive(t *testing.T) {
	flag := false
	ch := make(chan interface{})
	producer := createRocketMQProducer()
	err := producer.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Shutdown()
	err = producer.SendMessageOneway(createMessage("sendOnewayAndReceive", "sendOnewayAndReceive"))
	if err != nil {
		t.Fatal(err)
	}
	consumer := createRocketMQPushConsumer()
	consumer.Subscribe("sendOnewayAndReceive", "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Log(msg.Body)
		if msg.Body == "sendOnewayAndReceive" {
			flag = true
		}
		ch <- "done"
		return rocketmq.ConsumeSuccess
	})
	err = consumer.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Shutdown()

	<-ch
	if !flag {
		t.Errorf("send sync and receive test fail")
	}
}
