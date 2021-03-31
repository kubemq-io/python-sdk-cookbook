import datetime
from time import sleep

from kubemq.commandquery import ChannelParameters, Channel, RequestType, Request, Responder
from kubemq.commandquery.response import Response
from kubemq.subscription import SubscribeType, EventsStoreType, SubscribeRequest
from kubemq.tools import ListenerCancellationToken


def create_request_channel_parameters(request_type):
    return ChannelParameters(
        channel_name="queries",
        client_id="python-sdk-cookbook-rpc-queries-client",
        timeout=1000,
        request_type=request_type,
        kubemq_address="localhost:50000"
    )


def send_query_request():
    request_channel_parameters = create_request_channel_parameters(RequestType.Query)
    request_channel = Channel(channel_parameters=request_channel_parameters)

    request = Request(
        metadata="some-metadata",
        body="hello kubemq - sending a query, please reply".encode('UTF-8'),
        tags=[
            ('key', 'value'),
            ('key2', 'value2'),
        ]
    )

    try:
        request_channel.send_request(request)
    except Exception as err:
        print('error, error:%s' % (
            err
        ))


def handle_incoming_request(request):
    client_id = "python-sdk-cookbook-rpc-queries-client-receiver"
    if request:
        print("Subscriber Received request: Metadata:'%s', Channel:'%s', Body:'%s' tags:%s" % (
            request.metadata,
            request.channel,
            request.body,
            request.tags
        ))
        response = Response(request)
        response.body = "OK".encode('UTF-8')
        response.cache_hit = False
        response.error = "None"
        response.client_id = client_id
        response.executed = True
        response.metadata = "OK"
        response.timestamp = datetime.datetime.now()
        return response


def handle_incoming_error(error_msg):
    print("received error:%s'" % (
        error_msg
    ))


if __name__ == "__main__":
    cancel_token = ListenerCancellationToken()

    try:
        responder = Responder("localhost:50000")
        subscribe_request = SubscribeRequest(
            channel="queries",
            client_id="python-sdk-cookbook-rpc-queries-client-receiver",
            events_store_type=EventsStoreType.Undefined,
            events_store_type_value=0,
            group="",
            subscribe_type=SubscribeType.Queries
        )
        responder.subscribe_to_requests(subscribe_request, handle_incoming_request, handle_incoming_error, cancel_token)
        # give some time to connect a receiver
        sleep(1)
        send_query_request()
        cancel_token.cancel()

    except Exception as err:
        print('error, error:%s' % (
            err
        ))
