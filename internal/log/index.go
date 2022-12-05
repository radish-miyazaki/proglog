package log

import (
	"github.com/tysonmote/gommap"
	"io"
	"os"
)

const (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())

	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	if idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED); err != nil {
		return nil, err
	}
	return idx, nil
}

func (i *index) Close() error {
	// メモリにマップされたファイルのデータを永続化されたファイルへ同期
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	// 永続化されたファイルの内容を安定したストレージに同期
	if err := i.file.Sync(); err != nil {
		return err
	}

	// 永続化されたファイルをその中にある実際のデータ量まで切り詰めて、ファイルを閉じる
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	// 0の場合は、最初の位置を返す
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	// -1の場合は、一番最後の位置を返す
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	// オフセット番号とストアファイルの位置をマッピング
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	if i.isMaxed() {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)
	return nil
}

func (i *index) isMaxed() bool {
	return uint64(len(i.mmap)) < i.size+entWidth
}

func (i *index) Name() string {
	return i.file.Name()
}
