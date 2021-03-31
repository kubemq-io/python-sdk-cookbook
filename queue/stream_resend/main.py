import string
import random

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
    client_id = "python-sdk-cookbook-queues-resend-sender"
    kube_add = "localhost:50000"
    queue = MessageQueue("Get_Messages{}".format(random_string(10)), client_id, kube_add)
    mm = []

    message = create_queue_message("some-metadata", "hi there".encode('UTF-8'))
    mm.append(message)

    message2 = create_queue_message("some-metadata", "hi again".encode('UTF-8'))
    mm.append(message2)
    queue.send_queue_messages_batch(mm)

    tr = queue.create_transaction()
    for seq in range(3):
        stream = tr.receive(1, 5)
        print(stream.message)
        if seq == 0:
            tr.resend(queue.queue_name)
        else:
            tr.ack_message(stream.message.Attributes.Sequence)
        tr.close_stream()
