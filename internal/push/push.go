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
	"github.com/Shopify/sarama"
	"github.com/olivere/elastic/v7"
	//	_ "github.com/sirupsen/logrus"
)

type Pusher struct {
	Num    int
	Input  chan *Transport
	Client *elastic.Client
}

type Transport struct {
	Done    chan interface{}
	Message *sarama.ConsumerMessage
}

func (p *Pusher) Run() {
	errorlog := log.New(os.Stdout, "APP ", log.LstdFlags)

	var err error
	p.Client, err = elastic.NewClient(
		elastic.SetErrorLog(errorlog),
		elastic.SetURL(`http://192.168.2.10:9200`),
	)
	if err != nil {
		panic(err)
	}

}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
