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
    channel = "queues.delayed"
    queue = MessageQueue(channel, "python-sdk-cookbook-queues-delayed-client", "localhost:50000")
    queuePolicy = QueueMessagePolicy(
        DelaySeconds=10
    )
    message = create_queue_message("queueName {}".format(channel),
                                   "some-simple-queue-delayed-message-1".encode('UTF-8'),queuePolicy)
    try:
        sent = queue.send_queue_message(message)
        if sent.is_error:
            print('message enqueue error, error:' + sent.error)
        else:
            print('Send to Queue')
    except Exception as err:
        print('message enqueue error, error:%s' % (
            err
        ))

    queue = MessageQueue(channel, "python-sdk-cookbook-queues-delayed-client-receiver", "localhost:50000", 2, 1)
    try:
        res = queue.receive_queue_messages()
        if res.is_error:
            print(
                "'Error:'%s'" % (
                    res.error
                )
            )
        else:
            if len(res.messages) != 0:
                print("error found messages before delayed")
    except Exception as err:
        print(
            "'error receiving:'%s'" % (
                err
            )
        )
    sleep(10)
    try:
        res = queue.receive_queue_messages()
        if res.is_error:
            print(
                "'Error:'%s'" % (
                    res.error
                )
            )
        else:
            if len(res.messages) == 0:
                print("error receiving messages after delayed")
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
            "'error receiving:'%s'" % (
                err
            )
        )
