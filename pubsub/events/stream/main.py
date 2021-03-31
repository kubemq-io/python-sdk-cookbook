from time import sleep

from kubemq.events import Sender, Event
from kubemq.tools.listener_cancellation_token import ListenerCancellationToken
from kubemq.events.subscriber import Subscriber
from kubemq.subscription.subscribe_type import SubscribeType
from kubemq.subscription.events_store_type import EventsStoreType
from kubemq.subscription.subscribe_request import SubscribeRequest


def create_subscribe_request(
        subscribe_type=SubscribeType.Events, client_id="",
        events_store_type=EventsStoreType.Undefined,
        events_store_type_value=0, channel_name='events'

):
    return SubscribeRequest(
        channel=channel_name,
        client_id=client_id,
        events_store_type=events_store_type,
        events_store_type_value=events_store_type_value,
        group="",
        subscribe_type=subscribe_type
    )


def handle_incoming_events_receiver_a(event):
    if event:
        print("Subscriber a Received Event: Metadata:'%s', Channel:'%s', Body:'%s tags:%s \n" % (
            event.metadata,
            event.channel,
            event.body,
            event.tags
        ))


def handle_incoming_error(error_msg):
    print("received error:%s'" % (
        error_msg
    ))


def async_streamer(channel_name):
    for counter in range(2):
        yield Event(
            metadata="some-metadata",
            body=("hello kubemq - sending event :{}".format(counter)).encode('UTF-8'),
            store=False,
            channel=channel_name,
            client_id="python-sdk-cookbook-pubsub-events-stream-sender",
        )


def result_handler(result):
    # Handle result
    return


if __name__ == "__main__":
    channel = 'events'
    cancel_token = ListenerCancellationToken()
    try:
        subscriber = Subscriber("localhost:50000")
        subscribe_request = create_subscribe_request(SubscribeType.Events,
                                                     'python-sdk-cookbook-pubsub-events-stream-receiver-a',
                                                     EventsStoreType.Undefined, 0, channel)
        subscriber.subscribe_to_events(subscribe_request, handle_incoming_events_receiver_a, handle_incoming_error,
                                       cancel_token)

    except Exception as err:
        print('error, error:%s' % (
            err
        ))
    sleep(1)

    sender = Sender("localhost:50000")
    try:
        sender.stream_event(async_streamer(channel), result_handler)
    except Exception as err:
        print('error:%s' % (
            err
        ))
    sleep(5)
    cancel_token.cancel()
