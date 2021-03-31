

from kubemq.basic.grpc_client import GrpcClient
from kubemq.grpc import Empty

if __name__ == "__main__":

    client = GrpcClient('')
    client._kubemq_address = 'localhost:50000'
    try:
        ping_result = client.get_kubemq_client().Ping(Empty())
        print(ping_result)
    except Exception as err:
        print(
            "'error ping:'%s'" % (
                err
            )
        )
