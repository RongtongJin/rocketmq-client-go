package test

import (
	"fmt"
	rocketmq "github.com/apache/rocketmq-client-go/core"
	"sync/atomic"
	"testing"
	"time"
)

// only aliyun can success
//func TestGlobalOrder(t *testing.T) {
//	flag := false
//	ch := make(chan interface{})
//	consumer, err := createRocketMQGlobalOrderPushConsumer()
//	if err != nil {
//		t.Fatal(err.Error())
//	}
//	var count int32 = 0
//	consumer.Subscribe(GlobalOrderTopic, "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
//		t.Log(msg.Body)
//		content := fmt.Sprintf("%d", count)
//		if msg.Body != content {
//			flag = false
//			ch <- "done"
//		}
//		if atomic.AddInt32(&count, 1) == 20 {
//			flag = true
//			ch <- "done"
//		}
//		return rocketmq.ConsumeSuccess
//	})
//	err = consumer.Start()
//	if err != nil {
//		t.Fatal(err.Error())
//	}
//	defer consumer.Shutdown()
//
//	producer, err := createRocketMQGlobalOrderProducer()
//	if err != nil {
//		t.Fatal(err.Error())
//	}
//	err = producer.Start()
//	if err != nil {
//		t.Fatal(err.Error())
//	}
//	defer producer.Shutdown()
//
//	for i := 0; i < 20; i++ {
//		msgContent := fmt.Sprintf("%d", i)
//		//t.Logf("send %s",msgContent)
//		res, err := producer.SendMessageOrderlyByShardingKey(&rocketmq.Message{Topic: GlobalOrderTopic, Body: msgContent}, msgContent)
//		t.Logf("%s", res)
//		if err != nil {
//			t.Fatal(err)
//		}
//		if res.Status != rocketmq.SendOK {
//			t.Fatalf("send global order message fail")
//		}
//	}
//	select {
//	case <-time.After(time.Second * 40):
//	case <-ch:
//	}
//	if flag == false {
//		t.Errorf("global order test fail")
//	}
//}

func TestPartitionOrder(t *testing.T) {
	flag := false
	ch := make(chan interface{})
	consumer, err := createRocketMQPartitionOrderPushConsumer()
	if err != nil {
		t.Fatal(err.Error())
	}
	var count int32 = 0
	consumer.Subscribe(PartitionOrderTopic, "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Log(msg.Body)
		content := fmt.Sprintf("%d", count)
		if msg.Body != content {
			flag = false
			ch <- "done"
		}
		if atomic.AddInt32(&count, 1) == 20 {
			flag = true
			ch <- "done"
		}
		return rocketmq.ConsumeSuccess
	})
	err = consumer.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Shutdown()

	producer, err := createRocketMQPartitionOrderProducer()
	if err != nil {
		t.Fatal(err.Error())
	}
	err = producer.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Shutdown()

	for i := 0; i < 20; i++ {
		msgContent := fmt.Sprintf("%d", i)
		//t.Logf("send %s",msgContent)
		res, err := producer.SendMessageOrderlyByShardingKey(&rocketmq.Message{Topic: PartitionOrderTopic, Body: msgContent}, "shardingkey")
		t.Logf("%s", res)
		if err != nil {
			t.Fatal(err)
		}
		if res.Status != rocketmq.SendOK {
			t.Fatalf("send global order message fail")
		}
	}
	select {
	case <-time.After(time.Second * 40):
	case <-ch:
	}
	if flag == false {
		t.Errorf("global order test fail")
	}
}
