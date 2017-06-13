package storeserver

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sync"
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

func needleMetaSize() int {
	return 2 + 8 + 1 + 8 + 4
}

func newNeedleFromBytes(b []byte) *needle {
	r := &needle{}
	rdr := bytes.NewBuffer(b)

	magic := make([]byte, 2)
	_, err := rdr.Read(magic)
	if err != nil {
		return nil
	}

	r.key, err = readUint64(rdr)
	if err != nil {
		return nil
	}

	r.flg, err = rdr.ReadByte()
	if err != nil {
		return nil
	}

	r.sz, err = readUint64(rdr)
	if err != nil {
		return nil
	}

	r.checksum, err = readUint32(rdr)
	if err != nil {
		return nil
	}

	return r
}

func (n *needle) size() uint64 {
	// magic + key + flag + size + checksum + data
	return uint64(needleMetaSize()) + n.sz
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

	lock   sync.Mutex
	rwlock sync.RWMutex
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
	// TODO load index
	return v, nil
}

func (v *volume) close() error {
	// TODO flush index
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

	// append blob
	func() {
		v.lock.Lock()
		defer v.lock.Unlock()
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
	}()

	// update index
	v.rwlock.Lock()
	v.index[key] = v.cur
	// update cur offset for next needle
	v.cur += n.size()
	v.rwlock.Unlock()

	return nil
}

func (v *volume) read(key uint64) (*needle, []byte, error) {
	v.rwlock.RLock()
	offset, ok := v.index[key]
	v.rwlock.RUnlock()
	if !ok {
		return nil, nil, errors.New("no such key")
	}

	// read needle meta
	buf := make([]byte, needleMetaSize())
	_, err := v.rdr.ReadAt(buf, int64(offset))
	if err != nil {
		return nil, nil, err
	}

	n := newNeedleFromBytes(buf)
	if n == nil {
		return nil, nil, errors.New("invalid needle")
	}

	buf = make([]byte, n.sz)
	_, err = v.rdr.ReadAt(buf, int64(offset+uint64(needleMetaSize())))
	if err != nil {
		return nil, nil, err
	}
	n.v = v
	n.offset = offset
	return n, buf, nil
}
