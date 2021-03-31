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
    channel = "queues.dead-letter"
    queue = MessageQueue(channel, "python-sdk-cookbook-queues-dead-letter-client", "localhost:50000")
    policy = QueueMessagePolicy()
    policy.MaxReceiveCount = 3
    policy.MaxReceiveQueue = "DeadLetterQueue"
    message = create_queue_message("some-metadata", "some-simple-queue-dead-letter-message".encode('UTF-8'), policy)
    queue_send_message_to_queue_with_deadletter_response = queue.send_queue_message(message)
    print("finished sending message to queue with dead-letter answer: {} ".format(
        queue_send_message_to_queue_with_deadletter_response))
