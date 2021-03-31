from kubemq.queue.message_queue import MessageQueue

from kubemq.queue.message import Message

if __name__ == "__main__":
    channel = "queues.peek"
    queue = MessageQueue(channel, "python-sdk-cookbook-queues-peek-client", "localhost:50000")
    message = Message()
    message.metadata = 'metadata'
    message.body = "some-simple-queue-message".encode('UTF-8')
    message.attributes = None
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

    queue = MessageQueue(channel, "python-sdk-cookbook-queues-peek-client-receiver", "localhost:50000", 2, 1)
    try:
        res = queue.peek_queue_message(1)
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