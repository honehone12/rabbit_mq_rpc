package main

import (
	"log"
	rabbitrpc "rabbit_mq_rpc/rabbit-rpc"
	testcommon "rabbit_mq_rpc/test-common"
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
	data := testcommon.TestData{}
	envelop, _ := rabbitrpc.FromBin(raws.Body, &data)
	log.Printf(
		"********pipipi!!*******\n%s\n%s\n%d\n",
		raws.CorrelationId,
		envelop.DataTypeName,
		envelop.Method,
	)

	// slice ok!!
	// datas := make([]common.TestData, 10)
	// for i := 0; i < 10; i++ {
	// 	datas[i].Id = i
	// 	datas[i].CreatedAt = time.Now()
	// 	datas[i].User = data.User
	// }

	bin, _ := rabbitrpc.MakeBin(
		envelop.Method,
		rabbitrpc.StatusError,
		rabbitrpc.ErrorTypeName,
		rabbitrpc.ErrorTypeNotFound,
	)
	server.Publisher.Ch <- rabbitrpc.Raws{
		Body:          bin,
		CorrelationId: raws.CorrelationId,
	}
}
