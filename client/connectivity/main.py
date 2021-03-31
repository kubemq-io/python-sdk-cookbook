from kubemq.grpc import Empty
from kubemq.basic.grpc_client import GrpcClient

if __name__ == "__main__":
    try:
        client = GrpcClient(None)
        client._kubemq_address = 'localhost:50000'
        ping_result = client.get_kubemq_client().Ping(Empty())
        print(ping_result)
    except Exception as err:
        print(
            "'error:'%s'" % (
                err
            )
        )
