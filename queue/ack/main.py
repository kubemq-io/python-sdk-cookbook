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
    channel = "queues.ack"
    queue = MessageQueue(channel, "python-sdk-cookbook-queues-ack-client", "localhost:50000")
    mm = []

    message = create_queue_message("queueName {}".format(channel),
                                   "some-simple-queue-ack-message-1".encode('UTF-8'))
    mm.append(message)

    message2 = create_queue_message("queueName {}".format(channel),
                                    "some-simple-queue-ack-message-2".encode('UTF-8'))
    mm.append(message2)
    try:
        sent = queue.send_queue_messages_batch(mm)
        if sent.have_errors:
            print('message enqueue error, error:' + sent.have_errors)
        else:
            print('Send to Queue')
    except Exception as err:
        print('message enqueue error, error:%s' % (
            err
        ))

    queue = MessageQueue(channel, "python-sdk-cookbook-queues-ack-client-receiver", "localhost:50000", 2, 1)
    try:
        res = queue.receive_queue_messages()
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
    mm2 = []

    message3 = create_queue_message("queueName {}".format(channel),
                                    "some-simple-queue-queue-ack-message-3".encode('UTF-8'))
    mm2.append(message3)

    message4 = create_queue_message("queueName {}".format(channel),
                                    "some-simple-queue-queue-ack-message-4".encode('UTF-8'))
    mm2.append(message4)
    try:
        sent = queue.send_queue_messages_batch(mm2)
        if sent.have_errors:
            print('message enqueue error, error:' + sent.have_errors)
        else:
            print('Sent to Queue')
    except Exception as err:
        print('message enqueue error, error:%s' % (
            err
        ))

    queue.ack_all_queue_messages()

    try:
        res = queue.receive_queue_messages()
        if res.error:
            print(
                "'Received:'%s'" % (
                    res.error
                )
            )
        else:
            if len(res.messages) != 0:
                "'error found messages after ack:'%s'" % (
                    err
                )
    except Exception as err:
        print(
            "'error sending:'%s'" % (
                err
            )
        )
