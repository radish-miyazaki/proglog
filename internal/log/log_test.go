package log

import (
	api "github.com/radish-miyazaki/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"io"
	"os"
	"testing"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T, log *Log){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

// ログへの追加とログからの読み出しが正常に行えるか
func testAppendRead(t *testing.T, log *Log) {
	ap := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(ap)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, ap.Value, read.Value)
	require.NoError(t, log.Close())
}

// ログに保存されているオフセットの範囲外のオフセットを読み取ろうとするとエラーが返ってくるか
func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	require.Error(t, err)
	require.NoError(t, log.Close())
}

// ログを作成したときに、以前のログのインスタンスが保存したデータからログが再開するか
func testInitExisting(t *testing.T, log *Log) {
	ap := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(ap)
		require.NoError(t, err)
	}
	require.NoError(t, log.Close())
	// レコードが追加されているか
	off, err := log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = log.highestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	// 新しいログを作成しても保存されたデータが残っているか
	nlog, err := NewLog(log.Dir, log.Config)
	require.NoError(t, err)

	off, err = nlog.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = nlog.highestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
}

// ディスクに保存されているログを読み込めるか
func testReader(t *testing.T, log *Log) {
	ap := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(ap)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	// ログのio.Readerインスタンスを生成し、ディスクに保存されているデータを読み込む
	reader := log.Reader()
	b, err := io.ReadAll(reader)
	require.NoError(t, err)

	read := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], read)
	require.NoError(t, err)
	require.Equal(t, ap.Value, read.Value)
	require.NoError(t, log.Close())
}

// ログを切り詰めて、必要のない古いセグメントを削除する
func testTruncate(t *testing.T, log *Log) {
	ap := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(ap)
		require.NoError(t, err)
	}

	// 相対オフセット0のレコードを削除
	err := log.Truncate(1)
	require.NoError(t, err)
	_, err = log.Read(0)
	require.Error(t, err)
	require.NoError(t, log.Close())
}
