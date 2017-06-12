package storeserver

import (
	"testing"

	"github.com/ngaut/log"
)

func TestVolume(t *testing.T) {
	CreateVolume("test.vol", 1, 1<<20)

	v, err := OpenVolume("test.vol")
	log.Info(err)
	err = v.putBytes(123, 112, []byte("hello"))
	log.Info(err)

	n, b, err := v.read(123)
	log.Info(n, b, err)

}
