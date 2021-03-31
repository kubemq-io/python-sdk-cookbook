import string
import random
from time import sleep

from kubemq.queue.message_queue import MessageQueue

from kubemq.queue.message import Message


def random_string(string_length=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(string_length))


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
    client_id = "python-sdk-cookbook-queues-stream-sender"
    kube_add = "localhost:50000"
    queue = MessageQueue("Get_Messages{}".format(random_string(10)), client_id, kube_add)

    message = create_queue_message("some-metadata", "hi there".encode('UTF-8'))
    queue.send_queue_message(message)

    tr = queue.create_transaction()
    stream = tr.receive(5, 10)
    print(stream.message)
    print("work for 1 seconds")
    sleep(1)
    print("need more time to process, extend visibility for more 3 seconds")
    tr.extend_visibility(3)
    print("approved. work for 2.5 seconds")
    sleep(2.5)
    tr.ack_message(stream.message.Attributes.Sequence)
    tr.close_stream()
