package test

import (
	rocketmq "github.com/apache/rocketmq-client-go/core"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRocketMQRebalance(t *testing.T) {
	ch := make(chan interface{})
	var count int32 = 0
	var receiveMap sync.Map
	producer := createRocketMQProducer()
	err := producer.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Shutdown()
	for i := 0; i < 100; i++ {
		res, err := producer.SendMessageSync(createMessage("rebalance", MsgBody))
		if err != nil {
			t.Fatal(err)
		}
		if res.Status == rocketmq.SendOK {
			t.Logf("send msg success, res = %s", res)
			receiveMap.Store(res.MsgId, false)
		}
	}
	consumerA := createRocketMQPushConsumerByInstanceName("consumerA")
	consumerB := createRocketMQPushConsumerByInstanceName("consumerB")
	consumerC := createRocketMQPushConsumerByInstanceName("consumerC")
	consumerA.Subscribe("rebalance", "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Logf("consumerA receive msg: %s", msg)
		receiveMap.Store(msg.MessageID, true)
		if atomic.AddInt32(&count, 1) == 100 {
			ch <- "done"
		}
		return rocketmq.ConsumeSuccess
	})
	consumerB.Subscribe("rebalance", "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Logf("consumerB receive msg: %s", msg)
		receiveMap.Store(msg.MessageID, true)
		if atomic.AddInt32(&count, 1) == 100 {
			ch <- "done"
		}
		return rocketmq.ConsumeSuccess
	})
	consumerC.Subscribe("rebalance", "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Logf("consumerC receive msg: %s", msg)
		receiveMap.Store(msg.MessageID, true)
		if atomic.AddInt32(&count, 1) == 100 {
			ch <- "done"
		}
		return rocketmq.ConsumeSuccess
	})
	err = consumerA.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer consumerA.Shutdown()
	err = consumerB.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer consumerB.Shutdown()
	err = consumerC.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer consumerC.Shutdown()
	<-ch
	time.Sleep(time.Duration(10) * time.Second)
	if !checkMap(receiveMap) {
		t.Errorf("rebalance test fail")
	}
}
