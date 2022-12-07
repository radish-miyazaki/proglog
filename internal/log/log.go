package log

import (
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/radish-miyazaki/proglog/api/v1"
)

type Log struct {
	// INFO: RWMutexではロックを獲得している書き込みがない場合、読み込みのアクセスは可能
	mu            sync.RWMutex
	Dir           string
	Config        Config
	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	// ストアファイル・インデックスファイルの上限値がConfigで指定されていない場合はデフォルト値をセット
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}

	l := &Log{
		Dir:    dir,
		Config: c,
	}
	return l, l.setup()
}

func (l *Log) setup() error {
	// ディスク上のセグメントの一覧を取得
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	// ファイル名からベースオフセットの値を求めてソート
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	// ディスク上に存在するセグメントを処理して設定
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// baseOffsetsは、インデックスファイルとストアファイルの2つの重複を含んでいるので、
		// 重複しているものをスキップする
		i++
	}

	// 既存のセグメントが存在しない場合、渡されたベースオフセットで最初のセグメントを作成
	if l.segments == nil {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	// 追加したセグメントを一番新しいものとみなし、アクティブセグメントとする
	l.activeSegment = s
	return nil
}

func (l *Log) highestOffset() (uint64, error) {
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.segments[0].baseOffset, nil
}

// Truncate 処理したデータ不要になった古いセグメントを削除するためのメソッド
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var segments []*segment
	for _, s := range l.segments {
		// 最大オフセットがlowestよりも小さいセグメントを削除
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}

		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}

func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	highestOffset, err := l.highestOffset()
	if err != nil {
		return 0, err
	}

	// アクティブセグメントが最大の場合は新しいアクティブセグメントを作成
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(highestOffset + 1)
		if err != nil {
			return 0, err
		}
	}

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	return off, nil
}

func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var s *segment
	for _, segment := range l.segments {
		// セグメントのベースセグメントはセグメント内の最小のオフセットなので、
		// ベースオフセットが探しているオフセット以下であり、かつnextOffsetが探しているオフセットより大きい最初のセグメントを探す
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}
	if s == nil {
		return nil, api.ErrOffsetOutOfRange{Offset: off}
	}
	return s.Read(off)
}

// Close セグメントをすべてクローズする
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Remove セグメントをすべてクローズし、データをすべて削除する
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}

	return os.RemoveAll(l.Dir)
}

// Reset データをすべて削除し、新しいログを作成する
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}

	return l.setup()
}

// Reader ログ全体を読み込むためのio.Readerを返す
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()

	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originalReader{segment.store, 0}
	}
	return io.MultiReader(readers...)
}

type originalReader struct {
	*store
	off int64
}

func (o *originalReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)

	return n, err
}
