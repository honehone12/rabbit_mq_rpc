package main

import (
	"fmt"
	"log"
	rabbitrpc "rabbit_mq_rpc/rabbit-rpc"
	testcommon "rabbit_mq_rpc/test-common"
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
			data := testcommon.TestData{
				Id:        i,
				User:      "RabbitTaro",
				CreatedAt: time.Now(),
			}
			bin, _ := rabbitrpc.MakeBin(
				rabbitrpc.MethodCodeGET,
				rabbitrpc.StatusOK,
				"TestData",
				&data,
			)

			corrId := client.GenerateCorrelationID()
			callbackPool[corrId] = func(raws rabbitrpc.Raws) {
				onResposeReceived(raws)
				doneCh <- raws.CorrelationId
			}
			client.Publisher.Ch <- rabbitrpc.Raws{
				Body:          bin,
				CorrelationId: corrId,
			}
			i++
			time.Sleep(time.Millisecond * 1000)
		}
	}
}

func onResposeReceived(raws rabbitrpc.Raws) {
	log.Println(raws.CorrelationId)

	// slice ok!!
	// data := make([]common.TestData, 0)

	err := rabbitrpc.Error{}

	envelop, _ := rabbitrpc.FromBin(
		raws.Body,
		&err,
	)
	log.Printf(
		"*********response*********\n%v\n%s\n%v\n",
		envelop.Status,
		envelop.TypeName,
		err,
	)
}
