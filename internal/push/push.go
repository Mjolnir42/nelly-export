/*-
 * Copyright © 2016, Jörg Pernfuß <code.jpe@gmail.com>
 * Copyright © 2016, 1&1 Internet SE
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package push

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/olivere/elastic/v7"
	//	_ "github.com/sirupsen/logrus"
)

var Handlers map[string]map[int]Pusher

type Pusher struct {
	Num       int
	Input     chan *Transport
	Client    *elastic.Client
	Shutdown  chan struct{}
	Death     chan error
	topic     string
	topicSKey string
	topicENC  string
}

type Transport struct {
	Done    chan interface{}
	Message *sarama.ConsumerMessage
}

func (p *Pusher) Run() {
	if len(Handlers) == 0 {
		p.Death <- fmt.Errorf(`Incorrectly set handlers`)
		<-p.Shutdown
		return
	}

	p.topic = os.Getenv(`KAFKA_PRODUCER_TOPIC_DATA`)
	p.topicSKey = os.Getenv(`KAFKA_PRODUCER_TOPIC_SESSION`)
	p.topicENC = os.Getenv(`KAFKA_PRODUCER_TOPIC_ENCRYPTED`)

	errorlog := log.New(os.Stdout, "APP ", log.LstdFlags)

	var err error
	p.Client, err = elastic.NewClient(
		elastic.SetSniff(false),
		elastic.SetErrorLog(errorlog),
		elastic.SetURL(os.Getenv(`ELASTIC_SEARCH_API_URI`)),
	)
	if err != nil {
		p.Death <- err
		<-p.Shutdown
		return
	}
	_, esCode, err := p.Client.Ping(os.Getenv(`ELASTIC_SEARCH_API_URI`)).Do(context.Background())
	switch {
	case err != nil:
		p.Death <- err
		<-p.Shutdown
		return
	case esCode != 200:
		p.Death <- fmt.Errorf("Error: elasticsearch cluster responded with code %d\n", esCode)
		<-p.Shutdown
		return
	}

runloop:
	for {
		select {
		case <-p.Shutdown:
			break runloop
		case msg := <-p.Input:
			if msg == nil {
				continue runloop
			}
			p.process(msg)
		}
	}
}

func (p *Pusher) process(msg *Transport) {
	var index string
	switch msg.Message.Topic {
	case p.topic:
		index = `data-` + time.Now().UTC().Format(`2006-01-02`)
	case p.topicSKey:
		index = `session-key-` + time.Now().UTC().Format(`2006-01-02`)
	case p.topicENC:
		index = `encrypted-data-` + time.Now().UTC().Format(`2006-01-02`)
	}

	_, err := p.Client.Index().
		Index(index).
		BodyString(string(msg.Message.Value)).
		Do(context.Background())
	if err != nil {
		log.Println(err)
	}
	close(msg.Done)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
