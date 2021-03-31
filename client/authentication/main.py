
import jwt
from kubemq.grpc import Empty
from kubemq.basic.grpc_client import GrpcClient


if __name__ == "__main__":
    token = jwt.encode({}, algorithm="HS256", key="some-key")
    client = GrpcClient(token)
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
