#!/usr/bin/python27

import time, json, pika, argparse

parser = argparse.ArgumentParser(description='RabbitMQ performance stresser')
parser.add_argument('--exchange', help='RabbitMQ exchange', required=False, default='perftest')
parser.add_argument('--host', help='RabbitMQ host', required=False, default='localhost')
parser.add_argument('--topic_count', help='Number of topics to publish on', type=int, required=False, default=10000)
parser.add_argument('--topic', help='Subscribe to only this topic', required=False)


params = parser.parse_args()

HOST = params.host
rabbit_connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
rabbit_channel = rabbit_connection.channel()
exchange_name = params.exchange


r = rabbit_channel.queue_declare(exclusive=True, auto_delete=True)
queue_name = r.method.queue

print('Queue name: {}'.format(queue_name))

topic = params.topic

if topic is None:
    COUNT = params.topic_count
    print('Binding to exchange {}, {} topics'.format(exchange_name, COUNT))

    print('Binding...')
    for i in xrange(COUNT):
        portfolio = str(i)
        topic = 'perftest.{}'.format(portfolio)
        rabbit_channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=topic)
    print('Done')
else:
    print('Binding to exchange {}, topic: {}'.format(exchange_name, topic))
    rabbit_channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=topic)


messages = 0
total_size = 0
start = time.time()

def on_msg(ch, method, props, body):
    global messages, total_size
    messages += 1
    total_size += len(body)

def on_timer():
    end = time.time()

    global messages, total_size, start
    msg_rate = messages/float(end-start)
    data_rate_kb = total_size/(1024*float(end-start))

    print('Received msg rate: {:.2f} msg/s, data rate: {:.2f} kB/s'.format(msg_rate, data_rate_kb))

    start = end
    messages = 0
    total_size  =0
    rabbit_connection.add_timeout(1, on_timer)

print('Running...')
rabbit_channel.basic_consume(on_msg, queue=queue_name, no_ack=True)

rabbit_connection.add_timeout(1, on_timer)
rabbit_channel.start_consuming()