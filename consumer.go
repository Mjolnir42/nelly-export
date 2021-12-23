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
	//"context"
	//"flag"
	//"fmt"
	//"io/ioutil"
	//"log"
	//"os"
	//"os/signal"
	//"path/filepath"
	//"runtime"
	//"strings"
	"sync"
	//"syscall"
	//"time"

	"github.com/mjolnir42/nelly-export/internal/push"

	"github.com/Shopify/sarama"
	//"github.com/olivere/elastic/v7"
	_ "github.com/sirupsen/logrus"
	//	"github.com/client9/reopen"
	//	"github.com/mjolnir42/cyclone/lib/cyclone"
	//	"github.com/mjolnir42/cyclone/lib/metric"
	//	"github.com/wvanbergen/kafka/consumergroup"
	//	"github.com/wvanbergen/kazoo-go"
)

type Consumer struct {
	ready    chan bool
	numCPU   int
	handlers map[string]map[int]push.Pusher
	wg       *sync.WaitGroup
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	consumer.wg = &sync.WaitGroup{}
	// mark consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim
// goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	consumer.wg.Wait()
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's
// Messages()
func (consumer *Consumer) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine
	for message := range claim.Messages() {
		consumer.wg.Add(1)

		go func(msg *sarama.ConsumerMessage, s sarama.ConsumerGroupSession) {
			defer consumer.wg.Done()
			t := &push.Transport{
				Done:    make(chan interface{}),
				Message: msg,
			}
			select {
			case consumer.handlers[msg.Topic][int(msg.Offset)%consumer.numCPU].Input <- t:
			default:
				log.Printf("Dropped message from %s due to full channel\n", msg.Topic)
				close(t.Done)
			}
			<-t.Done
			s.MarkMessage(msg, ``)

		}(message, session)
	}

	return nil
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
