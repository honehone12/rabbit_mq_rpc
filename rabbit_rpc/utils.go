package rabbitrpc

import "encoding/json"

// req res classification

type MethodCode int
type StatusCode int

const (
	MethodCodeGET MethodCode = iota
	MethodCodePOST
	MethodCodePUT
	MethodCodeDelete
)

const (
	StatusOK StatusCode = iota
	StatusError
)

// general data wrapper

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
) (binEnvelope []byte, errorJSONMarshaling *Error) {
	binData, err := json.Marshal(dataPtr)
	if err != nil {
		errorJSONMarshaling = &Error{
			What: err.Error(),
		}
		return
	}
	envelop := Envelope{
		Method:   method,
		Status:   status,
		TypeName: dataTypeName,
		Body:     binData,
	}
	binEnvelope, err = json.Marshal(envelop)
	if err != nil {
		errorJSONMarshaling = &Error{
			What: err.Error(),
		}
	}
	return
}

func FromBin(
	bin []byte,
	dataPtr interface{},
) (envelop *Envelope, errorJSONUnmarshaling *Error) {
	envelop = &Envelope{}
	err := json.Unmarshal(bin, envelop)
	if err != nil {
		errorJSONUnmarshaling = &Error{
			What: err.Error(),
		}
		return
	}
	err = json.Unmarshal(envelop.Body, dataPtr)
	if err != nil {
		errorJSONUnmarshaling = &Error{
			What: err.Error(),
		}
	}
	return
}

// callback map for convenience of correlation id check

type CallbackPool map[string]func(raws Raws)

// error definitions

type Error struct {
	What string `json:"what"`
}

const ErrorTypeName = "Error"

func (err *Error) Error() string {
	return err.What
}

var ErrorTypeNotFound *Error = &Error{
	What: "type name is unknown",
}

var ErrorMethodCodeInvalid *Error = &Error{
	What: "method code is invalid",
}
