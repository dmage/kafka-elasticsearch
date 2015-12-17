package kafkahttp

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"

	"github.com/dmage/kafka-elasticsearch/restclient"
)

type Client struct {
	RESTClient restclient.Client
}

type OffsetStorage interface {
	Get() (int64, error)
	Commit(int64) error
}

type PartitionInfo struct {
	Topic      string
	Partition  int
	OffsetFrom int64
	OffsetTo   int64
}

type Message struct {
	ID       string
	Accepted int64
	Sender   json.RawMessage
	Data     json.RawMessage
}

type OutOfRangeError struct {
	OffsetFrom int64
	OffsetTo   int64
}

func (e OutOfRangeError) Error() string {
	return fmt.Sprintf("offset out of range (%d, %d)", e.OffsetFrom, e.OffsetTo)
}

func (c *Client) GetPartitions(topic string) ([]PartitionInfo, error) {
	resp, err := c.RESTClient.Get("/info/topics/"+topic, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Close()

	if resp.StatusCode != 200 {
		// FIXME(dmage)
		panic(resp.StatusCode)
	}

	var response struct {
		Data []PartitionInfo
	}
	err = resp.Decode(&response)
	if err != nil {
		return nil, err
	}
	return response.Data, nil
}

func (c *Client) GetMessages(topic string, partition int, offset int64, limit int64) ([]Message, error) {
	resp, err := c.RESTClient.Get(
		fmt.Sprintf("/topics/%s/%d", topic, partition),
		url.Values{
			"offset": {strconv.FormatInt(offset, 10)},
			"limit":  {strconv.FormatInt(limit, 10)},
		},
	)
	if err != nil {
		return nil, err
	}
	defer resp.Close()

	if resp.StatusCode == 416 {
		var errResponse struct {
			Data OutOfRangeError
		}
		err = resp.Decode(&errResponse)
		if err != nil {
			return nil, err
		}
		return nil, errResponse.Data
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%s", resp.Status)
	}

	var response struct {
		Data struct {
			Messages []Message
		}
	}
	err = resp.Decode(&response)
	if err != nil {
		return nil, err
	}
	return response.Data.Messages, nil
}
