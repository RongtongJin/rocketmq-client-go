package test

import (
	rocketmq "github.com/apache/rocketmq-client-go/core"
	"testing"
	"time"
)

func TestRocketMQTagFilterNotMatch(t *testing.T) {
	flagA, flagC := false, false
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
	res, err := producer.SendMessageSync(createTagMessage(TagFilterTopic, "TagC", "TagC"))
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
	consumer.Subscribe(TagFilterTopic, "TagA", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Log(msg.Body)
		if msg.Body == "TagA" {
			flagA = true
		} else if msg.Body == "TagC" {
			flagC = true
		}
		return rocketmq.ConsumeSuccess
	})
	err = consumer.Start()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer consumer.Shutdown()
	select {
	case <-time.After(time.Second * 10):
	case <-ch:
	}
	if flagC == false && flagA == false {
		t.Logf("tag filter test success")
	} else {
		t.Errorf("tag filter test fail")
	}
}

func TestRocketMQTagFilterAll(t *testing.T) {
	flagA, flagC := false, false
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
	res, err := producer.SendMessageSync(createTagMessage(TagFilterTopic, "TagC", "TagC"))
	if err != nil {
		t.Fatal(err.Error())
	}

	if res.Status != rocketmq.SendOK {
		t.Fatalf("send message fail")
	}

	res, err = producer.SendMessageSync(createTagMessage(TagFilterTopic, "TagA", "TagA"))
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
	consumer.Subscribe(TagFilterTopic, "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Log(msg.Body)
		if msg.Body == "TagA" {
			flagA = true
		} else if msg.Body == "TagC" {
			flagC = true
		}
		return rocketmq.ConsumeSuccess
	})
	err = consumer.Start()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer consumer.Shutdown()
	select {
	case <-time.After(time.Second * 10):
	case <-ch:
	}
	if flagC == true && flagA == true {
		t.Logf("tag filter test success")
	} else {
		t.Errorf("tag filter test fail")
	}
}

func TestRocketMQTagFilter(t *testing.T) {
	flagA, flagB, flagC := false, false, false
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
	res, err := producer.SendMessageSync(createTagMessage(TagFilterTopic, "TagC", "TagC"))
	if err != nil {
		t.Fatal(err.Error())
	}

	if res.Status != rocketmq.SendOK {
		t.Fatalf("send message fail")
	}

	res, err = producer.SendMessageSync(createTagMessage(TagFilterTopic, "TagA", "TagA"))
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
	consumer.Subscribe(TagFilterTopic, "TagB||TagC", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Log(msg.Body)
		if msg.Body == "TagA" {
			flagA = true
		} else if msg.Body == "TagB" {
			flagB = true
		} else if msg.Body == "TagC" {
			flagC = true
		}
		return rocketmq.ConsumeSuccess
	})
	err = consumer.Start()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer consumer.Shutdown()
	select {
	case <-time.After(time.Second * 10):
	case <-ch:
	}
	if flagC == true && flagB == false && flagA == false {
		t.Logf("tag filter test success")
	} else {
		t.Errorf("tag filter test fail")
	}
}

func TestRocketMQTagFilterTwice(t *testing.T) {
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
	res, err := producer.SendMessageSync(createTagMessage(TagFilterTopic, "TagC", "TagC"))
	if err != nil {
		t.Fatal(err.Error())
	}

	if res.Status != rocketmq.SendOK {
		t.Fatalf("send message fail")
	}

	res, err = producer.SendMessageSync(createTagMessage(TagFilterTopic, "TagA", "TagA"))
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
	consumer.Subscribe(TagFilterTopic, "TagA", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Log(msg.Body)
		flag = false
		ch <- "done"
		return rocketmq.ConsumeSuccess
	})
	consumer.Subscribe(TagFilterTopic, "TagC", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Log(msg.Body)
		flag = true
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
	if flag {
		t.Logf("tag filter test success")
	} else {
		t.Errorf("tag filter test fail")
	}
}
