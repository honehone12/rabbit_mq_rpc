package rabbitrpc

import "encoding/json"

type MethodCode int
type StatusCode int

const (
	MethodNone MethodCode = iota
	MethodCodeGET
	MethodCodePOST
	MethodCodePUT
	MethodCodeDelete
)

const (
	StatusNone StatusCode = iota
	StatusOK
)

type Envelope struct {
	Method MethodCode `json:"method"`
	Status StatusCode `json:"status"`

	TypeName string `json:"type_name"`
	Body     []byte `json:"body"`
}

func MakeBin(
	method MethodCode,
	status StatusCode,
	dataTypeName string,
	dataPtr interface{},
) (binEnvelope []byte, err error) {
	binData, err := json.Marshal(dataPtr)
	if err != nil {
		return
	}
	envelop := Envelope{
		Method:   method,
		Status:   status,
		TypeName: dataTypeName,
		Body:     binData,
	}
	binEnvelope, err = json.Marshal(envelop)
	return

}

func FromBin(bin []byte, dataPtr interface{}) (envelop *Envelope, err error) {
	envelop = &Envelope{}
	err = json.Unmarshal(bin, envelop)
	if err != nil {
		return
	}
	err = json.Unmarshal(envelop.Body, dataPtr)
	return
}

type CallbackPool map[string]func(raws Raws)
