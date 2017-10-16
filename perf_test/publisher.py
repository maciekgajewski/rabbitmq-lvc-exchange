#!/usr/bin/python27

import time, json, pika, argparse

parser = argparse.ArgumentParser(description='RabbitMQ performance stresser')
parser.add_argument('--exchange', help='RabbitMQ exchange', required=False, default='perftest')
parser.add_argument('--host', help='RabbitMQ host', required=False, default='localhost')
parser.add_argument('--topic_count', help='Number of topics to publish on', type=int, required=False, default=10000)
parser.add_argument('--topic_prefix', help='Prefix on topic', required=False, default='perftest')

params = parser.parse_args()

HOST = params.host
rabbit_connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
rabbit_channel = rabbit_connection.channel()
exchange_name = params.exchange
message_properties = pika.BasicProperties(content_type='application/json')


start = time.time()
total_size = 0

COUNT = params.topic_count
prefix = params.topic_prefix

print('publishing to exchange {}, {} topics'.format(exchange_name, COUNT))

while True:
    received_at = time.strftime('%Y-%m-%d %H:%M:%S')
    for i in xrange(COUNT):
        portfolio = str(i)
        market = 'XEUR'
        feedcode = 'feed'
        msg = {
            'instrument' : { 'market' : market, 'feedcode' : feedcode },
            'portfolio' : portfolio,
            'valid' : True,
            'position' : 7777,
            'invested' : 88,
            'daily_position' : 9,
            'daily_invested' : 1234,
            'received' : received_at,
        }
        topic = '{}.{}'.format(prefix, portfolio)
        serialized = json.dumps(msg)
        total_size += len(serialized)
        
        rabbit_channel.basic_publish(exchange=exchange_name, routing_key=topic, body=serialized, properties=message_properties)
    
    end = time.time()
    duration_ms = 1000*(end-start)
    msg_rate = COUNT/float(end-start)
    data_rate_kb = total_size/(1024*float(end-start))
    
    print('msg rate: {:.2f} msg/s, data rate: {:.2f} kB/s'.format(msg_rate, data_rate_kb))

    start = end
    total_size  =0
