#!/bin/bash

PROTO_DIR=./protos

generate_proto() {
    local proto_file=$1
    echo "Generating for $proto_file..."
    python -m grpc_tools.protoc -I=$PROTO_DIR --python_out=$PROTO_DIR --grpc_python_out=$PROTO_DIR "$PROTO_DIR/$proto_file"

    # Patch *_pb2_grpc.py files to use relative imports
    for f in $PROTO_DIR/*_pb2_grpc.py; do
        sed -i 's/^import \(.*_pb2\) as \(.*\)$/from . import \1 as \2/' "$f"
    done

    echo "Done generating and patching $proto_file."
}

if [ "$#" -gt 0 ]; then
  for proto in "$@"; do
    generate_proto "$proto"
  done
else
  echo "Usage: $0 ProtoFile1.proto [ProtoFile2.proto ...]"
fi