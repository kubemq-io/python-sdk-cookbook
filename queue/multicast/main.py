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
    channel = "queue.a;queue.b;queue.c"
    queue = MessageQueue(channel, "python-sdk-cookbook-queues-multicast-client", "localhost:50000")
    message = create_queue_message("queueName {}".format(channel),
                                   "some-simple-queue-multicast-message-1".encode('UTF-8'))
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

    queue_a = MessageQueue('queue.a', "python-sdk-cookbook-queues-multicast-client-receiver-A", "localhost:50000", 2, 1)
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

    queue_b = MessageQueue('queue.b', "python-sdk-cookbook-queues-multicast-client-receiver-B", "localhost:50000", 2, 1)
    try:
        res = queue_b.receive_queue_messages()
        if res.error:
            print(
                "'Error Received:'%s'" % (
                    res.error
                )
            )
        else:
            for message in res.messages:
                print(
                    "'Queue B Received :%s ,Body: sending:'%s'" % (
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

    queue_c = MessageQueue('queue.c', "python-sdk-cookbook-queues-multicast-client-receiver-C", "localhost:50000", 2, 1)
    try:
        res = queue_c.receive_queue_messages()
        if res.error:
            print(
                "'Error Received:'%s'" % (
                    res.error
                )
            )
        else:
            for message in res.messages:
                print(
                    "'Queue C Received :%s ,Body: sending:'%s'" % (
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
