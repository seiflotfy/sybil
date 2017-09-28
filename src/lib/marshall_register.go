package sybil

import "encoding/gob"

// this registration is used for saving and decoding cached per block query
// results
func registerTypes() {
	gob.Register(IntFilter{})
	gob.Register(StrFilter{})
	gob.Register(SetFilter{})
	gob.Register(&HistCompat{})
	gob.Register(&MultiHistCompat{})
}
