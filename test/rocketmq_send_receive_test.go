package test

import (
	rocketmq "github.com/apache/rocketmq-client-go/core"
	"testing"
	"time"
)

func TestRocketMQSendSyncAndReceive(t *testing.T) {
	flag := false
	ch := make(chan interface{})
	msgID := ""
	producer, err := createRocketMQProducer()
	if err != nil {
		t.Fatal(err.Error())
	}
	err = producer.Start()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer producer.Shutdown()
	res, err := producer.SendMessageSync(createMessage(NormalTopic, "sendAndReceive"))
	if err != nil {
		t.Fatal(err.Error())
	}
	if res.Status != rocketmq.SendOK {
		t.Fatalf("send message fail")
	} else {
		msgID = res.MsgId
	}
	consumer, err := createRocketMQPushConsumer()
	if err != nil {
		t.Fatal(err.Error())
	}
	consumer.Subscribe(NormalTopic, "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Log(msg.Body)
		if msg.MessageID == msgID {
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

func TestRocketMQSendAndReceiveChinese(t *testing.T) {
	flag := false
	ch := make(chan interface{})
	producer, err := createRocketMQProducer()
	if err != nil {
		t.Fatal(err.Error())
	}
	err = producer.Start()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer producer.Shutdown()
	res, err := producer.SendMessageSync(createMessage(NormalTopic, "中文"))
	if err != nil {
		t.Fatal(err.Error())
	}
	if res.Status != rocketmq.SendOK {
		t.Fatalf("send message fail")
	}
	consumer, err := createRocketMQPushConsumer()
	if err != nil {
		t.Fatal(err.Error())
	}
	consumer.Subscribe(NormalTopic, "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Log(msg.Body)
		if msg.Body == "中文" {
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
	producer, err := createRocketMQProducer()
	if err != nil {
		t.Fatal(err.Error())
	}
	err = producer.Start()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer producer.Shutdown()
	err = producer.SendMessageOneway(createMessage(OneWayTopic, "sendOnewayAndReceive"))
	if err != nil {
		t.Fatal(err.Error())
	}
	consumer, err := createRocketMQPushConsumer()
	if err != nil {
		t.Fatal(err.Error())
	}
	consumer.Subscribe(OneWayTopic, "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Log(msg.Body)
		if msg.Body == "sendOnewayAndReceive" {
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
