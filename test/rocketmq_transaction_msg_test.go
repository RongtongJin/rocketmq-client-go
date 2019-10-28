package test

import (
	"fmt"
	rocketmq "github.com/apache/rocketmq-client-go/core"
	"testing"
	"time"
)

var flagA = false
var flagB = false
var flagC = false
var flagD = false
var flagE = false
var flagF = false
var flagG = false
var flagH = false

type MyTransactionLocalListener struct {
}

type MyTransactionLocalContext struct {
}

func (l *MyTransactionLocalListener) Execute(m *rocketmq.Message, arg interface{}) rocketmq.TransactionStatus {
	fmt.Println("Execute")
	if m.Body == "excute:commit" {
		flagD = true
		return rocketmq.CommitTransaction
	}

	if m.Body == "excute:rollback" {
		return rocketmq.RollbackTransaction
	}

	if m.Body == "excute:unknow;check:commit" {
		flagA = true
		return rocketmq.UnknownTransaction
	}

	if m.Body == "excute:unknow;check:rollback" {
		flagF = true
		return rocketmq.UnknownTransaction
	}

	return rocketmq.UnknownTransaction
}
func (l *MyTransactionLocalListener) Check(m *rocketmq.MessageExt, arg interface{}) rocketmq.TransactionStatus {
	fmt.Println("Check")
	if m.Body == "excute:unknow;check:commit" {
		flagB = true
		return rocketmq.CommitTransaction
	}
	if m.Body == "excute:unknow;check:rollback" {
		flagG = true
		return rocketmq.RollbackTransaction
	}
	return rocketmq.CommitTransaction
}

func TestTransactionMsgExcuteCommit(t *testing.T) {

	var ch = make(chan interface{})
	listener := &MyTransactionLocalListener{}
	context := &MyTransactionLocalContext{}
	producer, err := createRocketMQTransactionProducer(listener, context)

	if err != nil {
		t.Fatal(err.Error())
	}

	err = producer.Start()
	if err != nil {
		t.Fatal(err.Error())
		return
	}
	defer producer.Shutdown()

	_, err = producer.SendMessageTransaction(&rocketmq.Message{Topic: TransactionTopic, Body: "excute:commit"}, context)
	if err != nil {
		t.Fatal(err.Error())
	}

	consumer, err := createRocketMQPushConsumer()
	if err != nil {
		t.Fatal(err.Error())
	}
	consumer.Subscribe(TransactionTopic, "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Log(msg.Body)
		if msg.Body == "excute:commit" {
			flagE = true
			ch <- "done"
		}
		return rocketmq.ConsumeSuccess
	})
	err = consumer.Start()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer consumer.Shutdown()

	select {
	case <-time.After(time.Second * 60):
	case <-ch:
	}
	if flagD && flagE {
		t.Logf("send transaction msg excute commit test success")
	} else {
		t.Errorf("send transaction msg excute commit test fail")
	}
}

func TestTransactionMsgExcuteRollback(t *testing.T) {

	listener := &MyTransactionLocalListener{}
	context := &MyTransactionLocalContext{}
	producer, err := createRocketMQTransactionProducer(listener, context)

	if err != nil {
		t.Fatal(err.Error())
	}

	err = producer.Start()
	if err != nil {
		t.Fatal(err.Error())
		return
	}
	defer producer.Shutdown()

	_, err = producer.SendMessageTransaction(&rocketmq.Message{Topic: TransactionTopic, Body: "excute:rollback"}, context)
	if err == nil {
		t.Fatal("test transaction msg excute rollback failed")
	}
}

func TestTransactionMsgUnkonwThenCommit(t *testing.T) {
	var ch = make(chan interface{})
	listener := &MyTransactionLocalListener{}
	context := &MyTransactionLocalContext{}
	producer, err := createRocketMQTransactionProducer(listener, context)

	if err != nil {
		t.Fatal(err.Error())
	}

	err = producer.Start()
	if err != nil {
		t.Fatal(err.Error())
		return
	}
	defer producer.Shutdown()

	_, err = producer.SendMessageTransaction(&rocketmq.Message{Topic: TransactionTopic, Body: "excute:unknow;check:commit"}, context)
	if err != nil {
		t.Fatal(err.Error())
	}

	consumer, err := createRocketMQPushConsumer()
	if err != nil {
		t.Fatal(err.Error())
	}
	consumer.Subscribe(TransactionTopic, "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Log(msg.Body)
		if msg.Body == "excute:unknow;check:commit" {
			flagC = true
			ch <- "done"
		}
		return rocketmq.ConsumeSuccess
	})
	err = consumer.Start()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer consumer.Shutdown()

	select {
	case <-time.After(time.Second * 60):
	case <-ch:
	}
	if flagA && flagB && flagC {
		t.Logf("send transaction msg unknow then commit test success")
	} else {
		t.Errorf("send transaction msg unknow then commit test fail")
	}
}

func TestTransactionMsgUnkonwThenRollback(t *testing.T) {
	var ch = make(chan interface{})
	listener := &MyTransactionLocalListener{}
	context := &MyTransactionLocalContext{}
	producer, err := createRocketMQTransactionProducer(listener, context)

	if err != nil {
		t.Fatal(err.Error())
	}

	err = producer.Start()
	if err != nil {
		t.Fatal(err.Error())
		return
	}
	defer producer.Shutdown()

	_, err = producer.SendMessageTransaction(&rocketmq.Message{Topic: TransactionTopic, Body: "excute:unknow;check:rollback"}, context)
	if err != nil {
		t.Fatal(err.Error())
	}

	consumer, err := createRocketMQPushConsumer()
	if err != nil {
		t.Fatal(err.Error())
	}
	consumer.Subscribe(TransactionTopic, "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Log(msg.Body)
		if msg.Body == "excute:unknow;check:rollback" {
			flagH = true
			ch <- "done"
		}
		return rocketmq.ConsumeSuccess
	})
	err = consumer.Start()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer consumer.Shutdown()

	select {
	case <-time.After(time.Second * 50):
	case <-ch:
	}
	if flagF && flagG && !flagH {
		t.Logf("send transaction msg test success")
	} else {
		t.Errorf("send transaction msg test fail")
	}
}
