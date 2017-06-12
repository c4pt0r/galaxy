package storeserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
)

type store struct {
	name          string
	path          string
	volumeCnt     int
	maxVolumeSize uint64
	volumes       []*volume
}

func CreateStore(name, storePath string, volumeCnt int, maxVolumeSize uint64) error {
	if _, err := os.Stat(storePath); !os.IsNotExist(err) {
		return errors.New("store already exists, you cannot init twice")
	}

	// create dir
	if err := os.Mkdir(storePath, os.ModePerm); err != nil {
		return err
	}

	// create meta file
	b, _ := json.Marshal(map[string]string{
		"name":            name,
		"volume_cnt":      fmt.Sprintf("%d", volumeCnt),
		"max_volume_size": fmt.Sprintf("%d", maxVolumeSize),
	})

	if err := ioutil.WriteFile(path.Join(storePath, "META"), b, os.ModePerm); err != nil {
		return err
	}

	for i := 0; i < volumeCnt; i++ {
		err := CreateVolume(path.Join(storePath, fmt.Sprintf("vol-%d", i)), uint32(i), maxVolumeSize)
		if err != nil {
			// TODO do some cleanup?
			return err
		}
	}
	return nil
}

func OpenStore(storePath string) (*store, error) {
	metaFile := path.Join(storePath, "META")
	b, err := ioutil.ReadFile(metaFile)
	if err != nil {
		return nil, err
	}

	v := make(map[string]string)
	err = json.Unmarshal(b, &v)
	if err != nil {
		return nil, err
	}

	volCnt, err := strconv.ParseUint(v["volume_cnt"], 10, 64)
	if err != nil {
		return nil, err
	}

	maxVolSize, err := strconv.ParseUint(v["max_volume_size"], 10, 64)
	if err != nil {
		return nil, err
	}

	ret := &store{
		name:          v["name"],
		volumeCnt:     int(volCnt),
		maxVolumeSize: maxVolSize,
	}

	for i := 0; i < ret.volumeCnt; i++ {
		volFile := path.Join(storePath, fmt.Sprintf("vol-%d", i))
		v, err := OpenVolume(volFile)
		if err != nil {
			return nil, err
		}
		ret.volumes = append(ret.volumes, v)
	}

	return ret, nil
}
