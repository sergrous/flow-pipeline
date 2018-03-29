# flow-pipeline

This repository contains a set of tools and examples for [GoFlow](https://github.com/cloudflare/goflow),
a NetFlow/IPFIX/sFlow collector by [Cloudflare](https://www.cloudflare.com).

## Description

Code examples:
* consumer/consumer.go: displays the live samples in JSON 
* consumer/processor.go: adds the source/destination country based on the IP in the sample
* pb-ext/flowext.proto: extension of the base [protobuf](https://github.com/cloudflare/goflow/blob/master/pb/flow.proto) adding countries

## Start a flow pipeline

The demo directory contains a startup file for an example pipeline including:
* GoFlow: an sFlow collector
* Kafka/Zookeeper
* A processor: to add country information to the samples
* A consumer: to display the flows

In the future versions, an aggregator will be added. Hoping to have a display with Grafana.

It will listen on port 6343/UDP for sFlow (a generator of samples will be added at some point).

```



                   +------+         +-----+
     sFlow/NetFlow |goflow+--------->Kafka|
                   +------+         +-----+
                                       |
                                       +-------------+
                      Topic: flows     |             |
                                       |             |
                                 +-----v---+       +-v---------+
                     +countries  |processor|       |new service|
                                 +---------+       +-----------+
                                      |
                                      |
                                   +--v--+
                                   |Kafka|
                                   +-----+
                                      |
                Topic: flows-extended |
                                      |
                                  +---v----+
                                  |consumer|
                                  +--------+
                                       JSON


```

Clone the repository, then run the following:

```
cd demo
./init.sh # This will download the Maxmind GeoLite2-Countries database for the flow processor
docker-compose -d
```

Wait a few seconds for all the system to start.

Then visualize the live flows:

```
docker-compose run processor -c "./consumer -kafka.brokers kafka:9092 -kafka.topic flows-extended"
```

Change the topic from `flows-extended` to `flows` if you want to see the raw flows (without the country added).
