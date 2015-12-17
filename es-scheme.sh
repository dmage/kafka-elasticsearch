#!/bin/sh
curl -XPUT http://localhost:9200/_template/kafka -d '
{
  "template" : "kafka-*",
  "mappings": {
    "TOPIC_NAME": {
      "properties": {
        "@timestamp": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
        "@version": {"type": "string", "index": "not_analyzed"},
        "@topic": {"type": "string", "index": "not_analyzed"},
        "@partition": {"type": "long"},
        "@offset": {"type": "long"},
        "id": {"type": "string", "index": "not_analyzed"},
        "accepted": {"type": "long"},
        "data": {
          "type": "object",
          "dynamic": false
        }
      }
    }
  }
}
'
