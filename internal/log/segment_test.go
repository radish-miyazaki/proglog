package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	api "github.com/radish-miyazaki/proglog/api/v1"
)

func TestSegment(t *testing.T) {
	dir, err := os.MkdirTemp("", "segment-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	// 新規のセグメントを構築
	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset)
	require.False(t, s.IsMaxed())

	// 上限値までレコードを追加
	for i := uint64(0); i < 3; i++ {
		// 追加したレコードの相対オフセットが想定値と同じか
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		// 相対オフセットをもとに呼び出した値が想定値と同じか
		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	// 上限値に達したセグメントにレコードを追加するとエラーが返ってくるか
	_, err = s.Append(want)
	require.Equal(t, io.EOF, err)
	require.True(t, s.IsMaxed())
	require.NoError(t, s.Close())

	// 既存のセグメントを再構築
	p, _ := proto.Marshal(want)
	c.Segment.MaxStoreBytes = uint64(len(p)+lenWidth) * 4
	c.Segment.MaxIndexBytes = 1024
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.True(t, s.IsMaxed()) // ストアが最大化どうかチェック

	// セグメントを削除した後、再構築した際に初期化されているか
	require.NoError(t, s.Remove())
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
	require.NoError(t, s.Close())
}
