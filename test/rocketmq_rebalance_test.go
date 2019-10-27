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
	producer, err := createRocketMQProducer()
	if err != nil {
		t.Fatal(err.Error())
	}
	err = producer.Start()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer producer.Shutdown()
	for i := 0; i < 100; i++ {
		res, err := producer.SendMessageSync(createMessage(RebalanceTopic, "rebalance"))
		if err != nil {
			t.Fatal(err.Error())
		}
		if res.Status == rocketmq.SendOK {
			t.Logf("send msg success, res = %s", res)
			receiveMap.Store(res.MsgId, false)
		}
	}
	consumerA, err := createRocketMQPushConsumerByInstanceName("consumerA")
	if err != nil {
		t.Fatal(err.Error())
	}
	consumerB, err := createRocketMQPushConsumerByInstanceName("consumerB")
	if err != nil {
		t.Fatal(err.Error())
	}
	consumerC, err := createRocketMQPushConsumerByInstanceName("consumerC")
	if err != nil {
		t.Fatal(err.Error())
	}
	consumerA.Subscribe(RebalanceTopic, "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Logf("consumerA receive msg: %s", msg)
		receiveMap.Store(msg.MessageID, true)
		if atomic.AddInt32(&count, 1) == 100 {
			ch <- "done"
		}
		return rocketmq.ConsumeSuccess
	})
	consumerB.Subscribe(RebalanceTopic, "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Logf("consumerB receive msg: %s", msg)
		receiveMap.Store(msg.MessageID, true)
		if atomic.AddInt32(&count, 1) == 100 {
			ch <- "done"
		}
		return rocketmq.ConsumeSuccess
	})
	consumerC.Subscribe(RebalanceTopic, "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Logf("consumerC receive msg: %s", msg)
		receiveMap.Store(msg.MessageID, true)
		if atomic.AddInt32(&count, 1) == 100 {
			ch <- "done"
		}
		return rocketmq.ConsumeSuccess
	})
	err = consumerA.Start()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer consumerA.Shutdown()
	err = consumerB.Start()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer consumerB.Shutdown()
	err = consumerC.Start()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer consumerC.Shutdown()
	select {
	case <-time.After(time.Second * 40):
	case <-ch:
	}
	time.Sleep(time.Duration(10) * time.Second)
	if !checkMap(receiveMap) {
		t.Errorf("rebalance test fail")
	}
}
