package main

import (
	"log"
	"rabbit_mq_rpc/common"
	rabbitrpc "rabbit_mq_rpc/rabbit_rpc"
)

var server *rabbitrpc.RabbitClient

func main() {
	server = rabbitrpc.NewRPCServer(
		rabbitrpc.DefaultRabbitURL,
		"res",
		"req",
		"rpc",
		rabbitrpc.ExchangeKindDirect,
		"client",
		"server",
		func(raws rabbitrpc.Raws) {
			go onRequestReceived(server, raws)
		},
	)
	defer server.Publisher.Done()
	defer server.Subscriber.Done()

	select {
	case <-server.Publisher.CTX.Done():
		break
	case <-server.Subscriber.CTX.Done():
		break
	}
}

func onRequestReceived(server *rabbitrpc.RabbitClient, raws rabbitrpc.Raws) {
	data := common.TestData{}
	envelop, _ := rabbitrpc.FromBin(raws.Body, &data)
	log.Printf("pipipi!! %s\n", raws.CorrelationId)

	bin, _ := rabbitrpc.MakeBin(
		envelop.Method,
		rabbitrpc.StatusOK,
		envelop.TypeName,
		&data,
	)
	server.Publisher.Ch <- rabbitrpc.Raws{
		Body:          bin,
		CorrelationId: raws.CorrelationId,
	}
}
