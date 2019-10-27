package test

import (
	"fmt"
	"github.com/apache/rocketmq-client-go/core"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestProducerCreateConfigNil(t *testing.T) {
	_, err := rocketmq.NewProducer(nil)
	if err == nil {
		t.Fail()
	} else {
		assert.Equal(t, ConfigNilInfo, err.Error())
	}
}

func TestProducerCreateLackNameServer(t *testing.T) {
	pConfig := &rocketmq.ProducerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID: "123456",
		},
		//Set to Common Producer as default.
		ProducerModel: rocketmq.CommonProducer,
	}
	_, err := rocketmq.NewProducer(pConfig)
	if err == nil {
		t.Fail()
	} else {
		assert.Equal(t, NameserverEmptyInfo, err.Error())
	}
}

func TestProducerCreateLackGroupId(t *testing.T) {
	pConfig := &rocketmq.ProducerConfig{
		ClientConfig: rocketmq.ClientConfig{
			NameServer: "localhost:9876",
		},
		//Set to Common Producer as default.
		ProducerModel: rocketmq.CommonProducer,
	}
	_, err := rocketmq.NewProducer(pConfig)
	if err == nil {
		t.Fail()
	} else {
		assert.Equal(t, GroupIdEmptyInfo, err.Error())
	}
}

func TestProducerCreateLackProducerModel(t *testing.T) {
	//FIXME lack of Producer Model
	{
		pConfig := &rocketmq.ProducerConfig{
			ClientConfig: rocketmq.ClientConfig{
				NameServer: "localhost:9876",
				GroupID:    "producer_group",
			},
		}

		_, err := rocketmq.NewProducer(pConfig)
		if err == nil {
			t.Fail()
		}
	}
}

func TestSendMessageBeforeStart(t *testing.T) {
	producer, err := createRocketMQProducer()
	if err != nil {
		t.Fatal(err.Error())
	}
	//send before start
	if producer != nil {
		_, err = producer.SendMessageSync(createMessage("Topic", "test"))
		if err == nil {
			t.Fail()
		} else {
			t.Logf("send before start, err is %s", err)
		}
	}
}

func TestMessageEmpty(t *testing.T) {
	producer, err := createRocketMQProducer()
	if err != nil {
		t.Fatal(err.Error())
	}
	if producer != nil {
		err = producer.Start()
		defer producer.Shutdown()
		msg := fmt.Sprintf("%s", "")

		_, err = producer.SendMessageSync(&rocketmq.Message{Topic: "test", Body: msg})
		if err == nil {
			t.Fail()
		} else {
			t.Logf("send message nil, err is %s", err)
		}
	}
}

func TestTopicEmpty(t *testing.T) {
	producer, err := createRocketMQProducer()
	if err != nil {
		t.Fatal(err.Error())
	}
	if producer != nil {
		err = producer.Start()
		defer producer.Shutdown()
		msg := fmt.Sprintf("%s", "test")
		_, err = producer.SendMessageSync(&rocketmq.Message{Topic: "", Body: msg})
		if err == nil {
			t.Fail()
		} else {
			t.Logf("Topic empty, err is %s", err)
		}
	}
}

func TestOrderlyMessageKeyEmpty(t *testing.T) {

	producer, err := createRocketMQOrderlyProducer()
	if err != nil {
		t.Fatal(err.Error())
	}
	if producer != nil {
		err = producer.Start()
		defer producer.Shutdown()
		msg := fmt.Sprintf("%s", "test")
		_, err = producer.SendMessageOrderlyByShardingKey(&rocketmq.Message{Topic: "rongtong_test", Body: msg}, "")
		if err == nil {
			t.Fail()
		} else {
			t.Logf("key empty, err is %s", err)
		}
	}
}
