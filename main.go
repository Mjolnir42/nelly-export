/*-
 * Copyright © 2016, Jörg Pernfuß <code.jpe@gmail.com>
 * Copyright © 2016, 1&1 Internet SE
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	//"flag"
	//"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	//"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	//"time"

	"github.com/mjolnir42/nelly-export/internal/push"

	"github.com/Shopify/sarama"
	//"github.com/olivere/elastic/v7"
	"github.com/mjolnir42/delay"
	_ "github.com/sirupsen/logrus"
	//	"github.com/client9/reopen"
	//	"github.com/mjolnir42/cyclone/lib/cyclone"
	//	"github.com/mjolnir42/cyclone/lib/metric"
	//	"github.com/wvanbergen/kafka/consumergroup"
	//	"github.com/wvanbergen/kazoo-go"
)

var githash, shorthash, builddate, buildtime string

var (
	brokers  = ``
	version  = ``
	group    = ``
	topics   = ``
	assignor = ``
	oldest   = true
	verbose  = false
)

func main() {
	brokers = os.Getenv(`KAFKA_BROKER_PEERS`)
	topics = strings.Join(
		[]string{
			os.Getenv(`KAFKA_PRODUCER_TOPIC_DATA`),
			os.Getenv(`KAFKA_PRODUCER_TOPIC_SESSION`),
			os.Getenv(`KAFKA_PRODUCER_TOPIC_ENCRYPTED`),
			os.Getenv(`KAFKA_PRODUCER_TOPIC_INFLOW`),
		},
		`,`,
	)
	group = os.Getenv(`KAFKA_CONSUMER_GROUP_NAME`)

	var useTLS bool = false
	switch os.Getenv(`KAFKA_USE_TLS`) {
	case `true`, `yes`, `1`:
		useTLS = true
	default:
		useTLS = false
	}
	var skipVerify bool = true
	switch os.Getenv(`KAFKA_TLS_SKIPVERIFY`) {
	case `true`, `yes`, `1`:
		skipVerify = true
	default:
		skipVerify = false
	}

	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	if useTLS {
		caCertPool := x509.NewCertPool()
		switch os.Getenv(`SSL_CERT_FILE`) {
		case ``:
		default:
			caCert, err := ioutil.ReadFile(os.Getenv(`SSL_CERT_FILE`))
			if err != nil {
				log.Panicf("Error reading SSL_CERT_FILE: " + err.Error())
			}
			caCertPool.AppendCertsFromPEM(caCert)
		}

		config.Net.SASL.User = os.Getenv(`KAFKA_SASL_USER`)
		config.Net.SASL.Password = os.Getenv(`KAFKA_SASL_PASSWD`)
		config.Net.SASL.Handshake = true
		config.Net.SASL.Enable = true
		config.Net.TLS.Enable = true
		tlsConfig := &tls.Config{
			RootCAs:            caCertPool,
			InsecureSkipVerify: skipVerify,
			ClientAuth:         0,
		}
		tlsConfig.BuildNameToCertificate()
		config.Net.TLS.Config = tlsConfig
	}
	config.ClientID = `nelly-export`

	consumer := Consumer{
		ready:    make(chan bool),
		numCPU:   runtime.NumCPU(),
		handlers: make(map[string]map[int]push.Pusher),
	}
	push.Handlers = make(map[string]map[int]push.Pusher)

	// setup goroutine waiting policy
	waitdelay := delay.New()
	handlerDeath := make(chan error)
	shutdown := make(chan struct{})

	// start pusher
	for _, t := range strings.Split(topics, `,`) {
		consumer.handlers[t] = make(map[int]push.Pusher)
		push.Handlers[t] = make(map[int]push.Pusher)
		for i := 0; i < runtime.NumCPU(); i++ {
			datachan := make(chan *push.Transport, 64)
			readychan := make(chan struct{})
			ph := push.Pusher{
				Num:      i,
				Input:    datachan,
				Shutdown: shutdown,
				Death:    handlerDeath,
				Ready:    readychan,
			}
			consumer.handlers[t][i] = ph
			push.Handlers[t][i] = ph
			waitdelay.Use()
			go func() {
				defer waitdelay.Done()
				ph.Run()
			}()
			log.Printf("Waiting for pusher %s/%d connection to ES\n", t, i)
			<-ph.Ready
			log.Printf("Pusher %s/%d connection to ES ready\n", t, i)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(
		strings.Split(brokers, `,`),
		group,
		config,
	)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// Consumer should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will be
			// needed to be recreated to get the new claims
			if err := client.Consume(ctx,
				strings.Split(topics, `,`),
				&consumer,
			); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if the context was cancelled
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	log.Println("Waiting for Sarama consumer")
	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
runloop:
	for {
		select {
		case <-ctx.Done():
			log.Println("terminating: context cancelled")
			break runloop
		case <-sigterm:
			log.Println("terminating: via signal")
			break runloop
		case err := <-handlerDeath:
			if err != nil {
				log.Println(err)
			}
			break runloop
		}
	}
	close(shutdown)
	cancel()
	wg.Wait()
	waitdelay.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
