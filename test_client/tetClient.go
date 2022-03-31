package main

import (
	"fmt"
	"log"
	"rabbit_mq_rpc/common"
	rabbitrpc "rabbit_mq_rpc/rabbit_rpc"
	"time"
)

func main() {
	callbackPool := make(rabbitrpc.CallbackPool)
	callbackCh := make(chan rabbitrpc.Raws)
	doneCh := make(chan string)

	client := rabbitrpc.NewRPCClient(
		rabbitrpc.DefaultRabbitURL,
		"req",
		"res",
		"rpc",
		rabbitrpc.ExchangeKindDirect,
		"server",
		"client",
		func(raws rabbitrpc.Raws) {
			callbackCh <- raws
		},
	)
	defer client.Publisher.Done()
	defer client.Subscriber.Done()

	i := 0
loop:
	for {
		select {
		case <-client.Publisher.CTX.Done():
			break loop
		case <-client.Subscriber.CTX.Done():
			break loop
		case raws := <-callbackCh:
			fn, ok := callbackPool[raws.CorrelationId]
			if ok {
				go fn(raws)
			} else {
				fmt.Println("received unknown response")
			}
		case id := <-doneCh:
			delete(callbackPool, id)
		default:
			data := common.TestData{
				Id:        i,
				User:      "RabbitTaro",
				CreatedAt: time.Now(),
			}
			bin, _ := rabbitrpc.MakeBin(
				rabbitrpc.MethodCodeGET,
				rabbitrpc.StatusNone,
				"TestData",
				&data,
			)

			corrId := rabbitrpc.GenerateCorrelationID()
			callbackPool[corrId] = func(raws rabbitrpc.Raws) {
				onResposeReceived(raws)
				doneCh <- raws.CorrelationId
			}
			client.Publisher.Ch <- rabbitrpc.Raws{
				Body:          bin,
				CorrelationId: corrId,
			}
			i++
			//time.Sleep(time.Millisecond * 10)
		}
	}
}

func onResposeReceived(raws rabbitrpc.Raws) {
	log.Println(raws.CorrelationId)
	data := common.TestData{}
	rabbitrpc.FromBin(
		raws.Body,
		&data,
	)
	log.Println(data)
}
