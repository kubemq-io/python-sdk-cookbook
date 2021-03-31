from time import sleep

from kubemq.events import Subscriber
from kubemq.queue.message_queue import MessageQueue

from kubemq.queue.message import Message
from kubemq.subscription import SubscribeType, EventsStoreType, SubscribeRequest
from kubemq.tools import ListenerCancellationToken


def create_queue_message(meta_data, body, policy=None):
    message = Message()
    message.metadata = meta_data
    message.body = body
    message.tags = [
        ('key', 'value'),
        ('key2', 'value2'),
    ]
    message.attributes = None
    message.policy = policy
    return message


def handle_incoming_events_receiver(event_a):
    if event_a:
        print("Subscriber Received Event: Metadata:'%s', Channel:'%s', Body:'%s tags:%s \n" % (
            event_a.metadata,
            event_a.channel,
            event_a.body,
            event_a.tags
        ))


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


def handle_incoming_error(error_msg):
    print("received error:%s'" % (
        error_msg
    ))


if __name__ == "__main__":
    cancel_token = ListenerCancellationToken()
    try:

        subscriber_a = Subscriber("localhost:50000")
        subscribe_request_a = create_subscribe_request(SubscribeType.EventsStore,
                                                       'python-sdk-cookbook-event-store-multicast-mix-client-receiver',
                                                       EventsStoreType.StartFromFirst, 0, 'es1')
        subscriber_a.subscribe_to_events(subscribe_request_a, handle_incoming_events_receiver, handle_incoming_error,
                                         cancel_token)

    except Exception as err:
        print('error, error:%s' % (
            err
        ))

    queue = MessageQueue("q1;events_store:es1", "python-sdk-cookbook-event-store-multicast-mix-client",
                         "localhost:50000")
    message = create_queue_message("some-metadata",
                                   "some-simple-event-multicast-mix-message-1".encode('UTF-8'))

    try:
        sent = queue.send_queue_message(message)
        if sent.error:
            print('message enqueue error, error:' + sent.error)
        else:
            print('Send to Queue  at: %d' % (
                sent.sent_at
            ))
    except Exception as err:
        print('message enqueue error, error:%s' % (
            err
        ))

    queue_a = MessageQueue('q1', "python-sdk-cookbook-event-store-multicast-mix-client-receiver-A", "localhost:50000",
                           2, 1)
    try:
        res = queue_a.receive_queue_messages()
        if res.error:
            print(
                "'Error Received:'%s'" % (
                    res.error
                )
            )
        else:
            for message in res.messages:
                print(
                    "'Queue A Received :%s ,Body: sending:'%s'" % (
                        message.MessageID,
                        message.Body
                    )
                )
    except Exception as err:
        print(
            "'error sending:'%s'" % (
                err
            )
        )

    sleep(1)
    cancel_token.cancel()
