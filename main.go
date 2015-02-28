package main

import "github.com/segmentio/go-stats"
import "github.com/docopt/docopt-go"
import "github.com/bitly/go-nsq"
import "encoding/json"
import "time"
import "log"
import "os"
import "io"

const Version = "1.0.1"

const Usage = `
  Usage:
    json_to_nsq --topic name [--nsqd-tcp-address addr]
    json_to_nsq -h | --help
    json_to_nsq --version

  Options:
    -a, --nsqd-tcp-address addr  destination nsqd tcp address [default: localhost:4150]
    -t, --topic name             destination topic name
    -h, --help                   output help information
    -v, --version                output version

`

var nl = []byte("\n")

func main() {
	args, err := docopt.Parse(Usage, nil, true, Version, false)
	if err != nil {
		log.Fatalf("error: %s", err)
	}

	// options
	addr := args["--nsqd-tcp-address"].(string)
	topic := args["--topic"].(string)

	// publisher
	config := nsq.NewConfig()
	pub, err := nsq.NewProducer(addr, config)
	if err != nil {
		log.Fatalf("error starting producer: %s", err)
	}
	defer pub.Stop()

	// stats
	s := stats.New()
	go s.TickEvery(5 * time.Second)

	// start
	d := json.NewDecoder(os.Stdin)

	for {
		var msg map[string]interface{}

		err = d.Decode(&msg)

		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatalf("error decode message: %s", err)
		}

		b, err := json.Marshal(msg)
		if err != nil {
			log.Printf("error marshal message: %s", err)
			continue
		}

		err = pub.Publish(topic, b)
		if err != nil {
			log.Printf("error publishing message: %s", err)
			continue
		}

		s.Incr("messages.published")
	}
}
