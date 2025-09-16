SRC_DIR=('../protos')
DEST_DIR=('.')
python -m grpc_tools.protoc -I$SRC_DIR --python_out=$DEST_DIR --pyi_out=$DEST_DIR --grpc_python_out=$DEST_DIR $SRC_DIR/$1
