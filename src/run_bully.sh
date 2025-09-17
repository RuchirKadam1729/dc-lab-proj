# terminal A
python chat_server.py --ws_port 9001 --grpc_port 50051 --priority 9 --seqnum 1

# terminal B
python chat_server.py --ws_port 9002 --grpc_port 50052 --priority 5 --seqnum 1

# terminal C
python chat_server.py --ws_port 9003 --grpc_port 50053 --priority 1 --seqnum 1
