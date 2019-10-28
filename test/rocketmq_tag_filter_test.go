package test

import (
	rocketmq "github.com/apache/rocketmq-client-go/core"
	"testing"
	"time"
)

func TestRocketMQTagFilterNotMatch(t *testing.T) {
	flagA, flagB := false, false
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
	res, err := producer.SendMessageSync(createTagMessage(TagFilterTopic, "TagA", "TagA"))
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
	consumer.Subscribe(TagFilterTopic, "TagB", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Log(msg.Body)
		if msg.Body == "TagA" {
			flagA = true
		} else if msg.Body == "TagB" {
			flagB = true
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
	if flagA == false && flagB == false {
		t.Logf("tag filter test success")
	} else {
		t.Errorf("tag filter test fail")
	}
}

func TestRocketMQTagFilterAll(t *testing.T) {
	flagC, flagD := false, false
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

	res, err = producer.SendMessageSync(createTagMessage(TagFilterTopic, "TagD", "TagD"))
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
		if msg.Body == "TagC" {
			flagC = true
		} else if msg.Body == "TagD" {
			flagD = true
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
	if flagC == true && flagD == true {
		t.Logf("tag filter test success")
	} else {
		t.Errorf("tag filter test fail")
	}
}

func TestRocketMQTagFilterWithOr(t *testing.T) {
	flagE, flagF, flagG := false, false, false
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
	res, err := producer.SendMessageSync(createTagMessage(TagFilterTopic, "TagE", "TagE"))
	if err != nil {
		t.Fatal(err.Error())
	}

	if res.Status != rocketmq.SendOK {
		t.Fatalf("send message fail")
	}

	res, err = producer.SendMessageSync(createTagMessage(TagFilterTopic, "TagF", "TagF"))
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
	consumer.Subscribe(TagFilterTopic, "TagE||TagG", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Log(msg.Body)
		if msg.Body == "TagE" {
			flagE = true
		} else if msg.Body == "TagF" {
			flagF = true
		} else if msg.Body == "TagG" {
			flagG = true
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
	if flagE == true && flagF == false && flagG == false {
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
	res, err := producer.SendMessageSync(createTagMessage(TagFilterTopic, "TagH", "TagH"))
	if err != nil {
		t.Fatal(err.Error())
	}

	if res.Status != rocketmq.SendOK {
		t.Fatalf("send message fail")
	}

	res, err = producer.SendMessageSync(createTagMessage(TagFilterTopic, "TagI", "TagI"))
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
	consumer.Subscribe(TagFilterTopic, "TagH", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Log(msg.Body)
		flag = false
		ch <- "done"
		return rocketmq.ConsumeSuccess
	})
	consumer.Subscribe(TagFilterTopic, "TagI", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
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
