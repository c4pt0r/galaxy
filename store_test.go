package storeserver

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ngaut/log"
)

func TestCreateStore(t *testing.T) {
	err := CreateStore("hello", "hello", 10, 1<<21)
	fmt.Println(err)
}

func TestOpenStore(t *testing.T) {
	s, err := OpenStore("hello")
	log.Info(err)
	buf := bytes.NewBuffer([]byte("hello"))
	v := s.volumes[0]
	err = v.put(123, uint64(buf.Len()), 112, buf)
	log.Info(err)
}
