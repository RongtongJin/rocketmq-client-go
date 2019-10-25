package test

import (
	"fmt"
	"github.com/apache/rocketmq-client-go/core"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestProducerCreate(t *testing.T) {

	// config is nil
	{
		_, err := rocketmq.NewProducer(nil)
		if err == nil {
			t.Fail()
		} else {
			assert.Equal(t, ConfigNilInfo, err.Error())
		}
	}

	// lack of nameserver
	{
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

	// lack of groupId
	{
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

	//FIXME
	// lack of Producer Model
	//{
	//	pConfig := &rocketmq.ProducerConfig{
	//		ClientConfig: rocketmq.ClientConfig{
	//			NameServer: "localhost:9876",
	//			GroupID:    "producer_group",
	//		},
	//	}
	//
	//	_, err := rocketmq.NewProducer(pConfig)
	//	if err == nil {
	//		t.Fail()
	//	}
	//}
}

func TestSendMessageBeforeStart(t *testing.T) {

	pConfig := &rocketmq.ProducerConfig{
		ClientConfig: rocketmq.ClientConfig{
			NameServer: "localhost:9876",
			GroupID:    "producer_group",
		},
		//Set to Common Producer as default.
		ProducerModel: rocketmq.CommonProducer,
	}

	producer, err := rocketmq.NewProducer(pConfig)
	if err != nil {
		t.Fatal(err)
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

	pConfig := &rocketmq.ProducerConfig{
		ClientConfig: rocketmq.ClientConfig{
			NameServer: "localhost:9876",
			GroupID:    "producer_group",
		},
		//Set to Common Producer as default.
		ProducerModel: rocketmq.CommonProducer,
	}

	producer, err := rocketmq.NewProducer(pConfig)
	if err != nil {
		t.Fatal(err)
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

	pConfig := &rocketmq.ProducerConfig{
		ClientConfig: rocketmq.ClientConfig{
			NameServer: "localhost:9876",
			GroupID:    "producer_group",
		},
		//Set to Common Producer as default.
		ProducerModel: rocketmq.CommonProducer,
	}

	producer, err := rocketmq.NewProducer(pConfig)
	if err != nil {
		t.Fatal(err)
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

	pConfig := &rocketmq.ProducerConfig{
		ClientConfig: rocketmq.ClientConfig{
			NameServer: "localhost:9876",
			GroupID:    "producer_group",
		},
		//Set to Common Producer as default.
		ProducerModel: rocketmq.CommonProducer,
	}

	producer, err := rocketmq.NewProducer(pConfig)
	if err != nil {
		t.Fatal(err)
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
