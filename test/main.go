package main

import (
	"fmt"
	rabbitrpc "rabbit_mq_rpc/rabbit_rpc"
)

type SomeData struct {
	Num   int     `json:"num"`
	Point float64 `json:"point"`
	Text  string  `json:"text"`
}

func main() {
	data := SomeData{
		Num:   1009,
		Point: 1.009,
		Text:  "1009",
	}

	bin, _ := rabbitrpc.MakeBin(
		rabbitrpc.MethodCodeGET,
		rabbitrpc.StatusNone,
		"SomeData",
		&data,
	)
	dataCopy := SomeData{}
	rabbitrpc.FromBin(bin, &dataCopy)

	fmt.Println(dataCopy)
}
