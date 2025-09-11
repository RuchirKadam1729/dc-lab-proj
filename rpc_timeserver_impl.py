from TimeServerProtoGens import TimeServer_pb2
from TimeServerProtoGens import TimeServer_pb2_grpc

class TimeServer(TimeServer_pb2_grpc.TimeServerServicer):
    
    def Sync(self, request : TimeServer_pb2.TimeMessage, context):
        pass
    
    