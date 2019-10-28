package test

import (
	rocketmq "github.com/apache/rocketmq-client-go/core"
	"testing"
	"time"
)

func TestSend4MbMsg(t *testing.T) {
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
	res, err := producer.SendMessageSync(createMessage(TooLongTopic, getRandomString(4*1024*1024)))
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
	consumer.Subscribe(TooLongTopic, "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
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

func TestSend4MbPlusMsg(t *testing.T) {
	producer, err := createRocketMQProducer()
	if err != nil {
		t.Fatal(err.Error())
	}
	err = producer.Start()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer producer.Shutdown()
	_, err = producer.SendMessageSync(createMessage(TooLongTopic, getRandomString(4*1024*1024+1)))
	if err == nil {
		t.Errorf("send 4mb plus test fail")
	}
}
