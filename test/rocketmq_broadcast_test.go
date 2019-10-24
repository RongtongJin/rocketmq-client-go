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

	consumerA := createRocketMQBroadcastConsumerByInstanceName("consumerA")
	consumerB := createRocketMQBroadcastConsumerByInstanceName("consumerB")
	consumerC := createRocketMQBroadcastConsumerByInstanceName("consumerC")

	consumerA.Subscribe("broadcastTest", "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Logf("consumerA receive msg: %s", msg)
		fmt.Printf("consumerA receive msg: %s\n", msg)
		if atomic.AddInt32(&countA, 1) == 10 {
			flagA = true
			wg.Done()
		}
		return rocketmq.ConsumeSuccess
	})
	consumerB.Subscribe("broadcastTest", "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Logf("consumerB receive msg: %s", msg)
		fmt.Printf("consumerB receive msg: %s\n", msg)
		if atomic.AddInt32(&countB, 1) == 10 {
			flagB = true
			wg.Done()
		}
		return rocketmq.ConsumeSuccess
	})
	consumerC.Subscribe("broadcastTest", "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		t.Logf("consumerC receive msg: %s", msg)
		fmt.Printf("consumerC receive msg: %s\n", msg)
		if atomic.AddInt32(&countC, 1) == 10 {
			flagC = true
			wg.Done()
		}
		return rocketmq.ConsumeSuccess
	})

	err := consumerA.Start()
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

	producer := createRocketMQProducer()
	err = producer.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Shutdown()

	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("test-%d", i)
		res, err := producer.SendMessageSync(&rocketmq.Message{Topic: "broadcastTest", Body: msg})
		if err != nil {
			t.Fatal(err)
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

	waitTimeout(&wg, 100*time.Second)

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
