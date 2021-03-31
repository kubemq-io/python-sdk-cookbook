from time import sleep

from kubemq.grpc import QueueMessagePolicy
from kubemq.queue.message_queue import MessageQueue

from kubemq.queue.message import Message


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


if __name__ == "__main__":
    channel = "queues.expiration"
    queue_sender = MessageQueue(channel, "python-sdk-cookbook-queues-expiration-client", "localhost:50000")
    policy = QueueMessagePolicy()
    policy.ExpirationSeconds = 5
    queue_message = create_queue_message("queueName {}".format(channel),
                                         "some-simple-queue-expiration-message-1".encode('UTF-8'), policy)
    try:
        sent = queue_sender.send_queue_message(queue_message)
        if sent.is_error:
            print('message enqueue error, error:' + sent.error)
        else:
            print('Send to Queue')
    except Exception as err:
        print('message enqueue error, error:%s' % (
            err
        ))

    queue_receiver = MessageQueue(channel, "python-sdk-cookbook-queues-expiration-client-receiver", "localhost:50000",
                                  2, 1)
    try:
        res = queue_receiver.receive_queue_messages()
        if res.error:
            print(
                "'Received:'%s'" % (
                    res.error
                )
            )
        else:
            for message in res.messages:
                print(
                    "'Received :%s ,Body: sending:'%s'" % (
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

    try:
        sent = queue_sender.send_queue_message(queue_message)
        if sent.is_error:
            print('message enqueue error, error:' + sent.error)
        else:
            print('Send to Queue')
    except Exception as err:
        print('message enqueue error, error:%s' % (
            err
        ))

    sleep(5)

    try:
        res = queue_receiver.receive_queue_messages()
        if res.error:
            print(
                "'Received:'%s'" % (
                    res.error
                )
            )
        else:
            if len(res.messages) != 0:
                "'error found messages after expiration:'%s'" % (
                    err
                )
    except Exception as err:
        print(
            "'error sending:'%s'" % (
                err
            )
        )
