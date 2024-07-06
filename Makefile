#Place holder for proto file 
FILE_NAME ?= message.proto

# target command
all : proto 

# to generate both .pb.go and _rpc.pb.go for the proto file 
proto: 
	protoc -I protos/ protos/$(FILE_NAME) --go_out=:. && protoc -I protos/ protos/$(FILE_NAME) --go-grpc_out=:.
