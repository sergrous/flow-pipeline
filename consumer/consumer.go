package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	log "github.com/Sirupsen/logrus"
	fmpb "github.com/cloudflare/goflow/pb"
	"github.com/golang/protobuf/jsonpb"
	proto "github.com/golang/protobuf/proto"
	sarama "gopkg.in/Shopify/sarama.v1"
	"net"
	"strconv"
	"strings"
	"sync"
)

var (
	n          = flag.Int("n", 10, "number of records to retrieve")
	KafkaTopic = flag.String("kafka.topic", "flows", "Kafka topic to consume from")
	KafkaSrv   = flag.String("kafka.srv", "", "SRV record containing a list of Kafka brokers (or use kafka.out.brokers)")
	KafkaBrk   = flag.String("kafka.brokers", "127.0.0.1:9092,[::1]:9092", "Kafka brokers list separated by commas")
	Json       = flag.Bool("json", true, "Render to json")
)

func fetchEvents(wg *sync.WaitGroup, topic string, consumer sarama.Consumer, messages chan<- *sarama.ConsumerMessage, partition int32, offset int64) {
	defer wg.Done()

	pconsumer, err := consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		log.Printf("Failed to create consumer on part: %d: %v", partition, err)
		return
	}
	defer pconsumer.Close()

	log.Printf("Fetching events from topic %s partiton %d offset %v", topic, partition, offset)

	mc := pconsumer.Messages()
	for {
		select {
		case m := <-mc:
			messages <- m
		}
	}
}

func GetServiceAddresses(srv string) (addrs []string, err error) {
	_, srvs, err := net.LookupSRV("", "", srv)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Service discovery: %v\n", err))
	}
	for _, srv := range srvs {
		addrs = append(addrs, net.JoinHostPort(srv.Target, strconv.Itoa(int(srv.Port))))
	}
	return addrs, nil
}

func DecodeIP(name string, value []byte) string {
	str := ""
	ipconv := net.IP{}
	if value != nil {
		invvalue := make([]byte, len(value))
		for i := range value {
			invvalue[len(value)-i-1] = value[i]
		}
		ipconv = value
		str += name + ipconv.String()
	}
	return str
}

func DecodeField(name string, value interface{}) string {
	str := ""
	switch valconv := value.(type) {
	case uint64, uint32, uint16:
		str += fmt.Sprintf("%v%v", name, valconv)
	}
	return str
}

func main() {
	flag.Parse()
	var err error
	addrs := make([]string, 0)
	if *KafkaSrv != "" {
		addrs, err = GetServiceAddresses(*KafkaSrv)
	} else {
		addrs = strings.Split(*KafkaBrk, ",")
	}
	if err != nil {
		log.Fatalf("Error ServiceAddrs: %v", err)
	}

	conf := sarama.NewConfig()
	offset := sarama.OffsetNewest

	consumer, err := sarama.NewConsumer(addrs, conf)
	if err != nil {
		log.Fatalf("Error NewConsumer: %v", err)
	}

	partitions, err := consumer.Partitions(*KafkaTopic)
	if err != nil {
		consumer.Close()
		log.Fatal("Error Partitions: %v", err)
	}

	messages := make(chan *sarama.ConsumerMessage, 15)
	wg := &sync.WaitGroup{}

	wg.Add(len(partitions))

	for _, partition := range partitions {
		go fetchEvents(wg, *KafkaTopic, consumer, messages, partition, offset)
	}

	go func() {
		wg.Wait()
		consumer.Close()
	}()

	for i, j := 0, 0; i < *n || *n == 0; {
		j++
		m := <-messages

		flowmsg := &fmpb.FlowMessage{}
		err = proto.Unmarshal(m.Value, flowmsg)
		if err != nil {
			log.Printf("unmarshaling error: ", err)
		} else {
			if *Json {
				bb := bytes.NewBuffer([]byte{})
				marshaler := jsonpb.Marshaler{}
				marshaler.Marshal(bb, flowmsg)

				fmt.Printf("%v\n", bb.String())
			} else {
				fmt.Printf("%v\n", flowmsg.String())
			}
		}

		if j%1e5 == 0 {
			ln := fmt.Sprintf("Topic: %s, Partition: %d, Offset: %d", *KafkaTopic, m.Partition, m.Offset)
			log.Print(ln)
		}
	}

}
