package test

import (
	rocketmq "github.com/apache/rocketmq-client-go/core"
	"strconv"
	"testing"
	"time"
)

func TestDelayMessage(t *testing.T) {
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
	startDeliverTime := time.Now().Unix()*1000 + 30*1000
	t.Logf("start deliver time is %s\n", strconv.FormatInt(startDeliverTime, 10))
	res, err := producer.SendMessageSync(&rocketmq.Message{Topic: DelayTopic, Body: "delayMsg", DelayTimeLevel: 3})
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
	consumer.Subscribe(DelayTopic, "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Logf("receive time is %s\n", strconv.FormatInt(time.Now().Unix()*1000, 10))
		if msg.Body == "delayMsg" && time.Now().Unix()*1000 >= startDeliverTime {
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
