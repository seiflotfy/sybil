package sybil

import "encoding/gob"
import "reflect"

func (h *HistCompat) AssignableTo(u reflect.Type) bool {
	switch u.(type) {
	case Histogram:
		return true
	default:
		return true
	}
}

func (h *BasicHist) AssignableTo(u reflect.Type) bool {
	switch u.(type) {
	case Histogram:
		return true
	default:
		return true
	}
}
func (h *MultiHist) AssignableTo(u reflect.Type) bool {
	switch u.(type) {
	case Histogram:
		return true
	default:
		return true
	}
}
func registerTypes() {
	gob.Register(IntFilter{})
	gob.Register(StrFilter{})
	gob.Register(SetFilter{})
	gob.Register(&HistCompat{})
	gob.Register(&MultiHistCompat{})
}
