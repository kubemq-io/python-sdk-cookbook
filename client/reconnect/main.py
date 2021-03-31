import jwt
from kubemq.grpc import Empty
from kubemq.basic.grpc_client import GrpcClient

if __name__ == "__main__":
    tries = 3
    for i in range(tries):
        try:
            client = GrpcClient(None)
            client._kubemq_address = 'localhost:50010'
            ping_result = client.get_kubemq_client().Ping(Empty())
            print(ping_result)
            break
        except Exception as err:
            if i < tries - 1:  # i is zero indexed
                print('failed on err' + err.__str__())
                continue
            else:
                raise
