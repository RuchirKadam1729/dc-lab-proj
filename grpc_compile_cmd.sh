python -m grpc_tools.protoc --python_out=. --grpc_python_out=. -I .  ./proto/ChatServer.proto

python -m grpc_tools.protoc --python_out=protos --pyi_out=protos --grpc_python_out=protos protos/ChatServer.proto

python -m grpc_tools.protoc -I protos --python_out=protos --pyi_out=protos --grpc_python_out=protos protos/ChatServer.proto
