package log

import (
	"fmt"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"

	api "github.com/radish-miyazaki/proglog/api/v1"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64 // 相対オフセットを計算するためbaseとnextと2つ有する
	config                 Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	// INFO: ストアファイルをオープンする。
	//  ファイルが存在しない場合はos.O_Createファイルモードフラグをos.OpenFileの引数として渡して、ファイルを作成する。
	//  ストアファイルを作成する際には、os.O_APPENDフラグを渡して、書き込み時にOSがファイルを追加するようにしている。
	storeFile, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	// INFO: インデックスファイルをオープンする。
	//  ストアファイル同様、ファイルが存在しない場合はファイルを作成する。
	indexFile, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	// 次のオフセットを設定して、次に追加されるレコードの準備をする。
	if off, _, err := s.index.Read(-1); err != nil {
		//  インデックスが空の場合、セグメントに追加される次のレコードが最初のレコードとなり、そのオフセットはセグメントのベースオフセットになる。
		s.nextOffset = baseOffset
	} else {
		//  空でない場合、次に書き込まれるレコードのオフセットはベースセグメントと相対オフセットの和に1を加算する
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur

	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	// ストアファイルにレコードを追加
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	// インデックスファイルに追加したレコードの相対オフセットと位置を追記
	if err = s.index.Write(
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}

	// 次のAppendの実行に備えて、nextOffsetを1加算
	s.nextOffset++
	return cur, nil
}

func (s *segment) Read(off uint64) (*api.Record, error) {
	// 相対オフセットをもとに、インデックスファイルからレコードの位置を取得
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}

	// 位置をもとに、ストアファイルからレコードを取得
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes ||
		s.index.isMaxed()
}

// Remove セグメントを閉じて、インデックスファイルとストアファイルを削除する
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}

	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}

	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}

	return nil
}

// Close インデックスファイルとストアファイルを閉じる
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}

	if err := s.store.Close(); err != nil {
		return err
	}

	return nil
}
