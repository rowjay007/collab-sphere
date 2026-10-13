package hub

import (
	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	"google.golang.org/protobuf/proto"
)

// EncodeError marshals a server ErrorMessage.
func EncodeError(code collabv1.ErrorMessage_Code, message string) []byte {
	return mustMarshal(&collabv1.ServerMessage{
		Payload: &collabv1.ServerMessage_Error{Error: &collabv1.ErrorMessage{
			Code:    code,
			Message: message,
		}},
	})
}

// EncodePong marshals a Pong with the given nonce.
func EncodePong(nonce uint64) []byte {
	return mustMarshal(&collabv1.ServerMessage{
		Payload: &collabv1.ServerMessage_Pong{Pong: &collabv1.Pong{Nonce: nonce}},
	})
}

func mustMarshal(msg *collabv1.ServerMessage) []byte {
	b, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return b
}
