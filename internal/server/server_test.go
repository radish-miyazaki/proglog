package server

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	api "github.com/radish-miyazaki/proglog/api/v1"
	"github.com/radish-miyazaki/proglog/internal/auth"
	"github.com/radish-miyazaki/proglog/internal/config"
	"github.com/radish-miyazaki/proglog/internal/log"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootClient api.LogClient, // 書き込みと読み出しを許可されたスーパーユーザ
		nobodyClient api.LogClient, // 何も許可されていないユーザ
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
		"unauthorized fails":                                 testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	rootClient api.LogClient,
	nobodyClient api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()

	// サーバが動作するローカルネットワークのアドレスに対してリスナーを作成
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	newClient := func(certPath, keyPath string) (
		*grpc.ClientConn,
		api.LogClient,
		[]grpc.DialOption,
	) {
		// INFO: クライアントのTLS認証情報に、RootCAとして、独自のCAを使うよう設定
		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CAFile:   config.CAFile,
			KeyFile:  keyPath,
			CertFile: certPath,
			Server:   false,
		})
		require.NoError(t, err)

		tlsCreds := credentials.NewTLS(tlsConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		conn, err := grpc.Dial(l.Addr().String(), opts...)
		require.NoError(t, err)

		client := api.NewLogClient(conn)
		return conn, client, opts
	}

	// サーバを呼び出すクライアント作成
	rootConn, rootClient, _ := newClient(
		config.RootClientCertFile,
		config.RootClientKeyFile,
	)
	nobodyConn, nobodyClient, _ := newClient(
		config.NobodyClientCertFile,
		config.NobodyClientKeyFile,
	)

	// INFO: サーバの証明書と鍵を解析し、それらを用いてサーバのTLS認証情報を設定
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)
	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer, err := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	require.NoError(t, err)
	cfg = &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}
	if fn != nil {
		fn(cfg)
	}

	// サーバ作成
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		// INFO: Serveは指定されたリスナーのAcceptメソッドが失敗しない限り呼び出し元に返ってこないので、
		//  以下のテストを実行するためにゴルーチンで呼び出す
		server.Serve(l)
	}()

	return rootClient, nobodyClient, cfg, func() {
		rootConn.Close()
		nobodyConn.Close()
		server.Stop()
		l.Close()
		clog.Remove()
	}
}

func testProduceConsume(t *testing.T, client, _ api.LogClient, _ *Config) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("hello world"),
	}

	// ログにレコードを書き込む
	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)
	want.Offset = produce.Offset

	// ログのレコードを読み出す
	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client, _ api.LogClient, _ *Config) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{Value: []byte("hello world")},
	})
	require.NoError(t, err)

	// ログの境界を超えて呼び出す
	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	if consume != nil {
		t.Errorf("consume not nil")
	}
	// 想定したエラーステータスコードが返ってくるかチェック
	got := status.Code(err)
	want := codes.OutOfRange
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}

func testProduceConsumeStream(t *testing.T, client, _ api.LogClient, _ *Config) {
	ctx := context.Background()

	records := []*api.Record{{
		Value:  []byte("first message"),
		Offset: 0,
	}, {
		Value:  []byte("second message"),
		Offset: 1,
	}}

	// ストリームの書き出しのテスト
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			require.NoError(t, err)

			res, err := stream.Recv()
			require.NoError(t, err)
			if res.Offset != uint64(offset) {
				t.Fatalf("got offset: %d, want: %d", res.Offset, offset)
			}
		}
	}

	// ストリームの読み出しのテスト
	{
		// ConsumeRequestには最初のオフセットをセット
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)

			require.Equal(t, res.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(i),
			})
		}
	}
}

func testUnauthorized(t *testing.T, _, client api.LogClient, config *Config) {
	ctx := context.Background()
	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	if produce != nil {
		t.Fatalf("produce response should be nil")
	}

	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want code: %d", gotCode, wantCode)
	}

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: 0,
	})
	if consume != nil {
		t.Fatalf("consume response should be nil")
	}
	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want code: %d", gotCode, wantCode)
	}
}