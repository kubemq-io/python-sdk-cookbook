import string
import random
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
        events_store_type_value=0, channel_name='events-store'

):
    return SubscribeRequest(
        channel=channel_name,
        client_id=client_id,
        events_store_type=events_store_type,
        events_store_type_value=events_store_type_value,
        group="",
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


def handle_incoming_events_receiver_c(event_b):
    if event_b:
        print("Subscriber c Received Event: Metadata:'%s', Channel:'%s', Body:'%s tags:%s \n" % (
            event_b.metadata,
            event_b.channel,
            event_b.body,
            event_b.tags
        ))


def handle_incoming_error(error_msg):
    print("received error:%s'" % (
        error_msg
    ))


if __name__ == "__main__":
    lett_dig = string.ascii_letters + string.digits
    channel = ''.join(random.choice(lett_dig) for i in range(5))
    sender = Sender("localhost:50000")
    for i in range(10):
        event = Event(
            metadata="some-metadata",
            body=("hello kubemq - sending event :{}".format(i)).encode('UTF-8'),
            store=True,
            channel=channel,
            client_id="python-sdk-cookbook-pubsub-events-store-offset-sender"
        )
        event.tags = [
            ('key', 'value'),
            ('key2', 'value2'),
        ]
        try:
            sender.send_event(event)
        except Exception as err:
            print('error:%s' % (
                err
            ))
    cancel_token = ListenerCancellationToken()
    try:

        # Subscribe to get all messages form start
        subscriber_a = Subscriber("localhost:50000")
        subscribe_request_a = create_subscribe_request(SubscribeType.EventsStore,
                                                       'python-sdk-cookbook-pubsub-events-store-offset-receiver-start-from-first',
                                                       EventsStoreType.StartFromFirst, 0, channel)
        subscriber_a.subscribe_to_events(subscribe_request_a, handle_incoming_events_receiver_a, handle_incoming_error,
                                         cancel_token)

        # Subscribe to get all messages from seq 5
        subscriber_b = Subscriber("localhost:50000")
        subscribe_request_b = create_subscribe_request(SubscribeType.EventsStore,
                                                       'python-sdk-cookbook-pubsub-events-store-offset-receiver-start-from-seq',
                                                       EventsStoreType.StartAtSequence, 5, channel)
        subscriber_b.subscribe_to_events(subscribe_request_b, handle_incoming_events_receiver_b, handle_incoming_error,
                                         cancel_token)

        # Subscribe to get all messages from 3 seconds ago
        subscriber_c = Subscriber("localhost:50000")
        subscribe_request_c = create_subscribe_request(SubscribeType.EventsStore,
                                                       'python-sdk-cookbook-pubsub-events-store-offset-receiver-start-from-time',
                                                       EventsStoreType.StartAtTimeDelta, 3, channel)
        subscriber_c.subscribe_to_events(subscribe_request_c, handle_incoming_events_receiver_c, handle_incoming_error,
                                         cancel_token)
    except Exception as err:
        print('error, error:%s' % (
            err
        ))
    sleep(3)
    event = Event(
        metadata="some-metadata",
        body="hello kubemq - sending last event".encode('UTF-8'),
        store=True,
        channel=channel,
        client_id="python-sdk-cookbook-pubsub-events-store-offset-sender"
    )
    event.tags = [
        ('key', 'value'),
        ('key2', 'value2'),
    ]
    try:
        sender.send_event(event)
    except Exception as err:
        print('error:%s' % (
            err
        ))
    sleep(1)
    cancel_token.cancel()
