#!/usr/bin/python27
import time, json, pika, argparse, sys


HOST = 'localhost'
EXCHANGE='lvc-test'

print('Connecting...')
rabbit_connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
rabbit_channel = rabbit_connection.channel()
message_properties = pika.BasicProperties(content_type='application/json')

rabbit_channel.exchange_declare(EXCHANGE, exchange_type='x-lvc')

# publish on 3 topics
MESSAGES = 3
for i in range(MESSAGES):
    topic = str(i)
    for j in range(i+1):
        msg = {'topic' : i, 'seq': j}
        body = json.dumps(msg)
        rabbit_channel.basic_publish(exchange=EXCHANGE, routing_key=topic, body=body, properties=message_properties)
        print("Published '{}' on topic '{}'".format(msg, topic))

# expect only the last to be received
r = rabbit_channel.queue_declare(exclusive=True, auto_delete=True)
queue_name = r.method.queue

for i in range(3):
    topic = str(i)
    rabbit_channel.queue_bind(exchange=EXCHANGE, queue=queue_name, routing_key=topic)

received = 0
def on_msg(ch, method, props, body):
    print('Received: {}', body)
    msg = json.loads(body)
    assert msg['topic'] == msg['seq'], "Invalid message received"

    global received
    received += 1
    if received == MESSAGES:
        print('Done!')
        sys.exit(0)


rabbit_channel.basic_consume(on_msg, queue=queue_name, no_ack=True)
rabbit_channel.start_consuming()
