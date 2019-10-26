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
var ch = make(chan interface{})

type MyTransactionLocalListener struct {
}

type MyTransactionLocalContext struct {
}

func (l *MyTransactionLocalListener) Execute(m *rocketmq.Message, arg interface{}) rocketmq.TransactionStatus {
	fmt.Println("Execute")
	if m.Body == "Transaction Message" {
		flagA = true
	}
	return rocketmq.UnknownTransaction
}
func (l *MyTransactionLocalListener) Check(m *rocketmq.MessageExt, arg interface{}) rocketmq.TransactionStatus {
	fmt.Println("Check")
	if m.Body == "Transaction Message" {
		flagB = true
	}
	return rocketmq.CommitTransaction
}

func TestTransactionMsg(t *testing.T) {
	config := &rocketmq.ProducerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:    "transcation_group",
			NameServer: rocketmqNameserver,
		},
		ProducerModel: rocketmq.TransProducer,
	}
	listener := &MyTransactionLocalListener{}
	context := &MyTransactionLocalContext{}
	producer, err := rocketmq.NewTransactionProducer(config, listener, context)

	if err != nil {
		t.Fatal(err)
	}

	err = producer.Start()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer producer.Shutdown()

	_, err = producer.SendMessageTransaction(&rocketmq.Message{Topic: "transaction-message", Body: "Transaction Message"}, context)
	if err != nil {
		t.Fatal(err)
	}

	consumer := createRocketMQPushConsumer()
	consumer.Subscribe("transaction-message", "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Log(msg.Body)
		if msg.Body == "Transaction Message" {
			flagC = true
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
	case <-time.After(time.Second * 80):
	case <-ch:
	}
	if flagA && flagB && flagC {
		t.Logf("send transaction msg test success")
	} else {
		t.Errorf("send transaction msg test fail")
	}

}
