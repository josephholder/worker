#!/usr/bin/env python
import pika
import time


def method_pika():
    credentials = pika.PlainCredentials('username', 'password')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host='host',
            credentials=credentials,
            heartbeat=0,
            # heartbeat_interval=0,
            blocked_connection_timeout=0
            # socket_timeout=0,
        )
    )
    channel = connection.channel()
    return channel


def callback(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)
    ch.stop_consuming()
    task(body)


def task(body):
    print(" [x] Received %r" % body)
    time.sleep(100)
    print(" [x] Done")
    time.sleep(5)
    main()


def start(channel):
    channel.queue_declare(queue='task_queue', durable=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback, queue='task_queue')
    return channel.start_consuming()


def stop_consuming(channel_value):
    return channel_value.stop_consuming()


def main():
    channel = method_pika()
    start(channel)


if __name__ == "__main__":
    main()
