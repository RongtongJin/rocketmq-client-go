package test

import (
	rocketmq "github.com/apache/rocketmq-client-go/core"
	"testing"
	"time"
)

func TestRocketMQSendSyncAndReceive(t *testing.T) {
	flag := false
	ch := make(chan interface{})
	msgId := ""
	producer := createRocketMQProducer()
	err := producer.Start()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer producer.Shutdown()
	res, err := producer.SendMessageSync(createMessage("sendAndReceive", "sendAndReceive"))
	if err != nil {
		t.Fatal(err.Error())
	}
	if res.Status != rocketmq.SendOK {
		t.Fatalf("send message fail")
	} else {
		msgId = res.MsgId
	}
	consumer := createRocketMQPushConsumer()
	consumer.Subscribe("sendAndReceive", "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Log(msg.Body)
		if msg.MessageID == msgId {
			flag = true
		}
		ch <- "done"
		return rocketmq.ConsumeSuccess
	})
	err = consumer.Start()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer consumer.Shutdown()
	select {
	case <-time.After(time.Second * 40):
	case <-ch:
	}
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

	select {
	case <-time.After(time.Second * 40):
	case <-ch:
	}
	if !flag {
		t.Errorf("send sync and receive test fail")
	}
}
