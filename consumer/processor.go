package main

import (
	"errors"
	"flag"
	"fmt"
	log "github.com/Sirupsen/logrus"
	fmpb "github.com/cloudflare/flow-pipeline/pb-ext"
	proto "github.com/golang/protobuf/proto"
	geoip2 "github.com/oschwald/geoip2-golang"
	sarama "gopkg.in/Shopify/sarama.v1"
	"net"
	"strconv"
	"strings"
	"sync"
)

var (
	n             = flag.Int("n", 10, "number of records to retrieve")
	GeoIPdb       = flag.String("geoip", "", "Path of the GeoIP database")
	KafkaTopic    = flag.String("kafka.topic", "flows", "Kafka topic to consume from")
	KafkaSrv      = flag.String("kafka.srv", "", "SRV record containing a list of Kafka brokers (or use kafka.out.brokers)")
	KafkaBrk      = flag.String("kafka.brokers", "127.0.0.1:9092,[::1]:9092", "Kafka brokers list separated by commas")
	KafkaOutTopic = flag.String("kafka.out.topic", "flows-extended", "Kafka topic to produce to")
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

	var db *geoip2.Reader
	if *GeoIPdb != "" {
		db, err = geoip2.Open(*GeoIPdb)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()
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

	var kafkaProducer sarama.AsyncProducer
	prodConf := sarama.NewConfig()
	prodConf.Producer.Return.Successes = false
	prodConf.Producer.Return.Errors = false
	kafkaProducer, err = sarama.NewAsyncProducer(addrs, prodConf)
	if err != nil {
		log.Fatal(err)
	}

	for i, j := 0, 0; i < *n || *n == 0; {
		j++
		m := <-messages

		flowmsg := &fmpb.FlowMessage{}
		err = proto.Unmarshal(m.Value, flowmsg)
		if err != nil {
			log.Printf("unmarshaling error: ", err)
		} else {
			flowmsg.SrcCountry = "O1"
			flowmsg.DstCountry = "O1"
			if db != nil {
				srccountry, _ := db.Country(flowmsg.SrcIP)
				dstcountry, _ := db.Country(flowmsg.DstIP)

				flowmsg.SrcCountry = srccountry.Country.IsoCode
				flowmsg.DstCountry = dstcountry.Country.IsoCode
			}
			b, _ := proto.Marshal(flowmsg)
			kafkaProducer.Input() <- &sarama.ProducerMessage{
				Topic: *KafkaOutTopic,
				Value: sarama.ByteEncoder(b),
			}
		}

		if j%1e5 == 0 {
			ln := fmt.Sprintf("Topic: %s, Partition: %d, Offset: %d", *KafkaTopic, m.Partition, m.Offset)
			log.Print(ln)
		}
	}

}
