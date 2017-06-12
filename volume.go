package storeserver

import (
	"bytes"
	"errors"
	"io"
	"os"

	"github.com/ngaut/log"
)

var (
	NEEDLE_MAGIC     = []byte{'\xbe', '\xef'}
	VOLUME_MAGIC     = []byte{'\xef', '\xef'}
	ErroNoEnoughRoom = errors.New("no enough room")
)

type needle struct {
	v        *volume
	offset   uint64
	key      uint64
	flg      byte
	sz       uint64
	checksum uint32
}

func (n *needle) size() uint64 {
	// magic + key + flag + size + checksum + data
	return 2 + 8 + 1 + 8 + 4 + n.sz
}

func readNeedleMeta(r io.Reader) (*needle, error) {
	b := make([]byte, 2)
	io.ReadFull(r, b)
	// TODO check magic

	n := &needle{}

	key, err := readUint64(r)
	if err != nil {
		return nil, err
	}
	n.key = key

	// read flag
	b = make([]byte, 1)
	r.Read(b)
	n.flg = b[0]

	sz, err := readUint64(r)
	if err != nil {
		return nil, err
	}
	n.sz = sz

	cs, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	n.checksum = cs

	return n, nil
}

func (n *needle) writeMeta(w io.Writer) error {
	// write magic
	_, err := w.Write(NEEDLE_MAGIC)
	if err != nil {
		return err
	}

	// write key
	if err := writeUint64(w, n.key); err != nil {
		return err
	}

	// write flg
	_, err = w.Write([]byte{n.flg})
	if err != nil {
		return err
	}

	if err := writeUint64(w, n.sz); err != nil {
		return err
	}

	if err := writeUint32(w, n.checksum); err != nil {
		return err
	}

	return nil
}

// [header][needle1][needle2]...
type volume struct {
	filename string
	id       uint32
	maxSize  uint64
	cur      uint64
	writer   io.Writer
	index    map[uint64]uint64 // offset map, file id > needle

	rdr *os.File
	fp  *os.File
}

func CreateVolume(filename string, id uint32, maxSize uint64) error {
	if _, err := os.Stat(filename); !os.IsNotExist(err) {
		return errors.New("volume already exists, you cannot init twice")
	}

	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return err
	}
	defer fp.Close()
	// write volume header
	if _, err := fp.Write(VOLUME_MAGIC); err != nil {
		return err
	}
	if err := writeUint32(fp, id); err != nil {
		return err
	}
	if err := writeUint64(fp, maxSize); err != nil {
		return err
	}
	return fp.Sync()
}

func OpenVolume(filename string) (*volume, error) {
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	rdr, err := os.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}

	v := &volume{
		filename: filename,
		fp:       fp,
		rdr:      rdr,
		index:    make(map[uint64]uint64),
	}

	b := make([]byte, 2)
	_, err = io.ReadFull(v.fp, b)
	if err != nil {
		fp.Close()
		return nil, err
	}
	if v.id, err = readUint32(v.fp); err != nil {
		fp.Close()
		return nil, err
	}
	if v.maxSize, err = readUint64(v.fp); err != nil {
		fp.Close()
		return nil, err
	}
	// magic + id + max_size
	v.cur = 2 + 4 + 8
	return v, nil
}

func (v *volume) close() error {
	if err := v.fp.Sync(); err != nil {
		return err
	}
	return v.fp.Close()
}

func (v *volume) putBytes(key uint64, checksum uint32, b []byte) error {
	r := bytes.NewBuffer(b)
	return v.put(key, uint64(r.Len()), checksum, r)
}

func (v *volume) put(key, size uint64, checksum uint32, r io.Reader) error {
	n := &needle{
		offset:   v.cur,
		key:      key,
		flg:      0,
		sz:       size,
		checksum: checksum,
	}

	// check if volume still got enough room for data
	if v.cur+n.size() > v.maxSize {
		return ErroNoEnoughRoom
	}

	// write needle meta
	err := n.writeMeta(v.fp)
	if err != nil {
		return err
	}
	// write data
	_, err = io.CopyN(v.fp, r, int64(size))
	if err != nil {
		return err
	}
	// call fsync
	err = v.fp.Sync()
	if err != nil {
		return err
	}

	// update index
	v.index[key] = v.cur
	// update cur offset for next needle
	v.cur += n.size()
	return nil
}

func (v *volume) read(key uint64) (*needle, []byte, error) {
	offset, ok := v.index[key]
	log.Info(v.index, key)
	if !ok {
		return nil, nil, errors.New("no such key")
	}
	_, err := v.rdr.Seek(int64(offset), 0)
	if err != nil {
		return nil, nil, err
	}

	n, err := readNeedleMeta(v.rdr)
	if err != nil {
		return nil, nil, err
	}
	n.v = v

	buf := make([]byte, n.sz)
	_, err = io.ReadFull(v.rdr, buf)
	if err != nil {
		return nil, nil, err
	}

	return n, buf, nil
}
