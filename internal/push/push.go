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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
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
	Ready     chan struct{}
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

	// setup HTTPS client
	// load TLS CA certificate
	caCertPool := x509.NewCertPool()
	switch os.Getenv(`SSL_CERT_FILE`) {
	case ``:
	default:
		caCert, err := ioutil.ReadFile(os.Getenv(`SSL_CERT_FILE`))
		if err != nil {
			p.Death <- err
			<-p.Shutdown
			return
		}
		caCertPool.AppendCertsFromPEM(caCert)
	}

	var useTLS bool = false
	switch os.Getenv(`ELASTIC_USE_TLS`) {
	case `true`, `yes`, `1`:
		useTLS = true
	default:
		useTLS = false
	}

	var skipVerify bool = false
	switch os.Getenv(`ELASTIC_TLS_SKIPVERIFY`) {
	case `true`, `yes`, `1`:
		skipVerify = true
	default:
		skipVerify = false
	}

	tlsConfig := &tls.Config{
		RootCAs:            caCertPool,
		InsecureSkipVerify: skipVerify,
		ClientAuth:         0,
	}
	tlsConfig.BuildNameToCertificate()

	transport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout: 5 * time.Second,
		}).Dial,
	}
	if useTLS {
		transport.TLSClientConfig = tlsConfig
		transport.TLSHandshakeTimeout = 5 * time.Second
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   time.Second * 10,
	}

	var err error
	if p.Client, err = elastic.NewClient(
		elastic.SetHttpClient(httpClient),
		elastic.SetSniff(false),
		elastic.SetErrorLog(errorlog),
		elastic.SetURL(os.Getenv(`ELASTIC_SEARCH_API_URI`)),
	); err != nil {
		p.Death <- err
		<-p.Shutdown
		return
	}

	log.Println("Testing configured ES connection...")
connectloop:
	for {
		_, esCode, err := p.Client.Ping(os.Getenv(`ELASTIC_SEARCH_API_URI`)).Do(context.Background())
		switch {
		case err != nil:
			log.Println(err)
			time.Sleep(5 * time.Second)
		case esCode != 200:
			p.Death <- fmt.Errorf("Error: elasticsearch cluster responded with code %d\n", esCode)
			<-p.Shutdown
			return
		default:
			close(p.Ready)
			break connectloop
		}
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
