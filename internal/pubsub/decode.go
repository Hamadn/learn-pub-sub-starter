package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
)

func jsonDecode[T any](b []byte) (T, error) {
	var t T
	return t, json.Unmarshal(b, &t)
}

func gobDecode[T any](b []byte) (T, error) {
	var t T
	return t, gob.NewDecoder(bytes.NewReader(b)).Decode(&t)
}
