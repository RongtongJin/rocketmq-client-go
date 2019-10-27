package test

import (
	"fmt"
	rocketmq "github.com/apache/rocketmq-client-go/core"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRocketMQBroadcast(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(3)
	flagA := false
	flagB := false
	flagC := false
	var countA int32 = 0
	var countB int32 = 0
	var countC int32 = 0

	consumerA, err := createRocketMQBroadcastConsumerByInstanceName("consumerA")
	if err != nil {
		t.Fatal(err.Error())
	}
	consumerB, err := createRocketMQBroadcastConsumerByInstanceName("consumerB")
	if err != nil {
		t.Fatal(err.Error())
	}
	consumerC, err := createRocketMQBroadcastConsumerByInstanceName("consumerC")
	if err != nil {
		t.Fatal(err.Error())
	}

	consumerA.Subscribe(BroadcastTopic, "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Logf("consumerA receive msg: %s", msg)
		fmt.Printf("consumerA receive msg: %s\n", msg)
		if atomic.AddInt32(&countA, 1) == 10 {
			flagA = true
			wg.Done()
		}
		return rocketmq.ConsumeSuccess
	})
	consumerB.Subscribe(BroadcastTopic, "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Logf("consumerB receive msg: %s", msg)
		fmt.Printf("consumerB receive msg: %s\n", msg)
		if atomic.AddInt32(&countB, 1) == 10 {
			flagB = true
			wg.Done()
		}
		return rocketmq.ConsumeSuccess
	})
	consumerC.Subscribe(BroadcastTopic, "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Logf("consumerC receive msg: %s", msg)
		fmt.Printf("consumerC receive msg: %s\n", msg)
		if atomic.AddInt32(&countC, 1) == 10 {
			flagC = true
			wg.Done()
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

	producer, err := createRocketMQProducer()

	if err != nil {
		t.Fatal(err.Error())
	}

	err = producer.Start()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer producer.Shutdown()

	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("test-%d", i)
		res, err := producer.SendMessageSync(&rocketmq.Message{Topic: BroadcastTopic, Body: msg})
		if err != nil {
			t.Fatal(err.Error())
		}
		if res.Status == rocketmq.SendOK {
			t.Logf("send msg success, res = %s", res)
			fmt.Printf("send success, res = %s\n", res)
		} else {
			t.Fatalf("send msg fail\n")
		}
	}
	fmt.Printf("send end\n")
	t.Logf("send end")

	waitTimeout(&wg, 40*time.Second)

	if flagA && flagB && flagC {
		t.Logf("broadcast test success")
	} else {
		t.Errorf("broadcast test fail")
	}
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
