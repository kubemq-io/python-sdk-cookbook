import string
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
        events_store_type_value=0, channel_name='events', group=""

):
    return SubscribeRequest(
        channel=channel_name,
        client_id=client_id,
        events_store_type=events_store_type,
        events_store_type_value=events_store_type_value,
        group=group,
        subscribe_type=subscribe_type
    )


def handle_incoming_events_receiver_a(event_a):
    if event_a:
        print("Subscriber a Received Event: Metadata:'%s', Channel:'%s', Body:'%s tags:%s \n" % (
            event_a.metadata,
            event_a.channel,
            event_a.body,
            event_a.tags
        ))


def handle_incoming_events_receiver_b(event_b):
    if event_b:
        print("Subscriber b Received Event: Metadata:'%s', Channel:'%s', Body:'%s tags:%s \n" % (
            event_b.metadata,
            event_b.channel,
            event_b.body,
            event_b.tags
        ))


def handle_incoming_events_receiver_c(event_c):
    if event_c:
        print("Subscriber c Received Event: Metadata:'%s', Channel:'%s', Body:'%s tags:%s \n" % (
            event_c.metadata,
            event_c.channel,
            event_c.body,
            event_c.tags
        ))


def handle_incoming_error(error_msg):
    print("received error:%s'" % (
        error_msg
    ))


def async_streamer(channel_name):
    for counter in range(1):
        yield Event(
            metadata="some-metadata",
            body=("hello kubemq - sending event wildcards :{}".format(counter)).encode('UTF-8'),
            store=False,
            channel=channel_name+'.A',
            client_id="python-sdk-cookbook-pubsub-events-wildcards-sender",
        )
        yield Event(
            metadata="some-metadata",
            body=("hello kubemq - sending event wildcards :{}".format(counter)).encode('UTF-8'),
            store=False,
            channel=channel_name+'.B',
            client_id="python-sdk-cookbook-pubsub-events-wildcards-sender",
        )


def result_handler(result):
    # Handle result
    return


if __name__ == "__main__":
    lett_dig = string.ascii_letters + string.digits
    channel = "events"
    cancel_token = ListenerCancellationToken()
    try:

        subscriber_a = Subscriber("localhost:50000")
        subscribe_request_a = create_subscribe_request(SubscribeType.Events,
                                                       'python-sdk-cookbook-pubsub-events-wildcards-receiver-a',
                                                       EventsStoreType.Undefined, 0, channel + '.A')
        subscriber_a.subscribe_to_events(subscribe_request_a, handle_incoming_events_receiver_a, handle_incoming_error,
                                         cancel_token)

        subscriber_b = Subscriber("localhost:50000")
        subscribe_request_b = create_subscribe_request(SubscribeType.Events,
                                                       'python-sdk-cookbook-pubsub-events-wildcards-receiver-b',
                                                       EventsStoreType.Undefined, 0, channel + '.B')
        subscriber_b.subscribe_to_events(subscribe_request_b, handle_incoming_events_receiver_b, handle_incoming_error,
                                         cancel_token)

        subscriber_c = Subscriber("localhost:50000")
        subscribe_request_c = create_subscribe_request(SubscribeType.Events,
                                                       'python-sdk-cookbook-pubsub-events-wildcards-receiver-all',
                                                       EventsStoreType.Undefined, 0, channel + '.*')
        subscriber_c.subscribe_to_events(subscribe_request_c, handle_incoming_events_receiver_c, handle_incoming_error,
                                         cancel_token)

    except Exception as err:
        print('error, error:%s' % (
            err
        ))
    sleep(3)
    limit = 1
    sender = Sender("localhost:50000")
    try:
        sender.stream_event(async_streamer(channel), result_handler)
    except Exception as err:
        print('error:%s' % (
            err
        ))
    sleep(5)
    cancel_token.cancel()
