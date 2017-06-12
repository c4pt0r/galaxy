package storeserver

import (
	"encoding/binary"
	"io"
)

func writeUint32(w io.Writer, v uint32) error {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	_, err := w.Write(b)
	if err != nil {
		return err
	}
	return nil
}

func writeUint64(w io.Writer, v uint64) error {
	// write key
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	_, err := w.Write(b)
	if err != nil {
		return err
	}
	return nil
}

func readUint32(r io.Reader) (uint32, error) {
	b := make([]byte, 4)
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

func readUint64(r io.Reader) (uint64, error) {
	b := make([]byte, 8)
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}
