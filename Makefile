# 生成された証明書を置くパス
CONFIG_PATH=${HOME}/.proglog/

.PHONY: init
init:
	mkdir -p ${CONFIG_PATH}

.PHONE: gencert
gencert:
	cfssl gencert \
		-initca test/ca-csr.json | cfssljson -bare ca

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=server \
		test/server-csr.json | cfssljson -bare server

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="root" \
		test/client-csr.json | cfssljson -bare root-client

	# 異なるCommonNameを持つクライアントをテスト用に生成
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="nobody" \
		test/client-csr.json | cfssljson -bare nobody-client

	mv *.pem *.csr ${CONFIG_PATH}

# 異なるCAから発行された証明書を用いたときに弾かれることを確認する際に用いるコマンド
.PHONY: extra-gencert
extra-gencert:
	cfssl gencert \
		-initca test/other-ca-csr.json | cfssljson -bare other-ca

	cfssl gencert \
		-ca=other-ca.pem \
		-ca-key=other-ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		test/client-csr.json | cfssljson -bare other-client

.PHONY: compile
compile:
	protoc api/v1/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative
		--proto_path=.

# Casbinで用いるモデルファイルとポリシーファイルをCONFIG_PATHにコピーする
${CONFIG_PATH}/model.conf:
	cp test/model.conf ${CONFIG_PATH}/model.conf

${CONFIG_PATH}/policy.csv:
	cp test/policy.csv ${CONFIG_PATH}/policy.csv

.PHONY: test
test: ${CONFIG_PATH}/model.conf ${CONFIG_PATH}/policy.csv
	go test -race ./...
