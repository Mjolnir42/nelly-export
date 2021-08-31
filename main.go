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
	//"flag"
	//"fmt"
	//"io/ioutil"
	"log"
	"os"
	"os/signal"
	//"path/filepath"
	//"runtime"
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
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky

	consumer := Consumer{
		ready:    make(chan bool),
		numCPU:   runtime.NumCPU(),
		handlers: make(map[string]map[int]push.Pusher),
	}

	// setup goroutine waiting policy
	waitdelay := delay.New()

	// start pusher
	for t := range strings.Split(topics, `,`) {
		consumer.handlers[t] = make(map[int]push.Pusher)
		for i := 0; i < runtime.NumCPU(); i++ {
			datachan := make(chan *push.Transport)
			ph := push.Pusher{
				Num:   i,
				Input: datachan,
			}
			consumer.handlers[t][i] = ph
			waitdelay.Use()
			go func() {
				defer waitdelay.Done()
				ph.Run()
			}()
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

	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	cancel()
	wg.Wait()
	waitdelay.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
