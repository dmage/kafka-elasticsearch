package main

import (
	"encoding/json"
	"flag"
	"math/rand"
	"os"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/naoina/toml"

	"github.com/dmage/kafka-elasticsearch/kafkahttp"
	"github.com/dmage/kafka-elasticsearch/restclient"
)

var configFileName = flag.String("config", "./foo.toml", "configuration file")

var cfg struct {
	KafkaHTTPProxyURL string
	ElasticURL        string
	ElasticIndex      string
	ElasticRetries    int
	Topics            []string
	Step              int64
}

func processMessage(topic string, partition int, offset int64, message kafkahttp.Message) error {
	var msg struct {
		Timestamp int64            `json:"@timestamp"`
		Topic     string           `json:"@topic"`
		Partition int              `json:"@partition"`
		Offset    int64            `json:"@offset"`
		ID        string           `json:"id"`
		Accepted  int64            `json:"accepted"`
		Sender    *json.RawMessage `json:"sender"`
		Data      *json.RawMessage `json:"data"`
	}

	msg.Timestamp = message.Accepted / 1000000
	msg.Topic = topic
	msg.Partition = partition
	msg.Offset = offset
	msg.ID = message.ID
	msg.Accepted = message.Accepted
	msg.Sender = &message.Sender // you may try to remove pointers
	msg.Data = &message.Data

	c := restclient.Client{
		URL: cfg.ElasticURL,
	}

	retryCounter := 0
	date := time.Unix(0, message.Accepted).UTC().Format("2006-01-02")
	for {
		var errResponse interface{}
		resp, err := c.Put("/"+cfg.ElasticIndex+"-"+date+"/"+topic+"/"+msg.ID, msg, nil, &errResponse)
		if err != nil {
			return err
		}
		if resp.StatusCode == 200 || resp.StatusCode == 201 {
			break
		}
		if resp.StatusCode == 429 {
			retryCounter++
			if cfg.ElasticRetries == -1 || retryCounter < cfg.ElasticRetries {
				time.Sleep(1*time.Second + time.Duration(rand.Intn(10000))*time.Millisecond)
				continue
			}
		}
		log.Fatal(resp.StatusCode, errResponse)
	}

	log.Infof("%d:%d: %s", partition, offset, message.ID)
	return nil
}

func worker(kafkaHTTPClient *kafkahttp.Client, topic string, partition int) {
	offsetStorage, err := kafkahttp.NewFileOffsetStorage(topic, partition)
	if err != nil {
		log.Fatal(err)
	}

	offsetFrom, err := offsetStorage.Get()
	if err != nil {
		log.Fatal(err)
	}

	offset := offsetFrom
	for {
		messages, err := kafkaHTTPClient.GetMessages(topic, partition, offset, cfg.Step)
		if oorErr, ok := err.(kafkahttp.OutOfRangeError); ok {
			if offset < oorErr.OffsetFrom {
				offset = oorErr.OffsetTo
				err = offsetStorage.Commit(offset - 1)
				if err != nil {
					log.Fatal(err)
				}
				continue
			}

			log.Warningf("topic=%s partition=%d err=%s", topic, partition, err)
			time.Sleep(1*time.Second + time.Duration(rand.Intn(2000))*time.Millisecond)
			continue
		}
		if err != nil {
			log.Errorf("topic=%s partition=%d err=%s", topic, partition, err)
			time.Sleep(1*time.Second + time.Duration(rand.Intn(2000))*time.Millisecond)
			continue
		}
		for idx, message := range messages {
			err = processMessage(topic, partition, offset+int64(idx), message)
			if err != nil {
				log.Fatalf("topic=%s partition=%d processMessage err=%s", topic, partition, err)
			}
			err = offsetStorage.Commit(offset + int64(idx))
			if err != nil {
				log.Fatal(err)
			}
		}
		offset += int64(len(messages))
	}
}

func main() {
	flag.Parse()

	f, err := os.Open(*configFileName)
	if err != nil {
		log.Fatal(err)
	}

	err = toml.NewDecoder(f).Decode(&cfg)
	if err != nil {
		log.Fatal(err)
	}

	f.Close()

	kafkaHTTPClient := &kafkahttp.Client{
		RESTClient: restclient.Client{
			URL: cfg.KafkaHTTPProxyURL + "/v1",
		},
	}

	for _, topic := range cfg.Topics {
		partitions, err := kafkaHTTPClient.GetPartitions(topic)
		if err != nil {
			log.Fatal(err)
		}
		for _, info := range partitions {
			go worker(kafkaHTTPClient, info.Topic, info.Partition)
		}
	}

	quit := make(chan struct{})
	<-quit
}
