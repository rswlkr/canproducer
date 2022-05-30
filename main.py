from confluent_kafka import Producer
import sys
import can
import json
from bson import json_util
import time
import random
import os,binascii
import asyncio
from can.interface import Bus


clientID = "123"
env = sys.argv[1]
print("env: " + env)
#ec2-52-19-41-28.eu-west-1.compute.amazonaws.com:9092
ch =sys.argv[2]
print("can channel: " + ch)
if env == "mac":
    can.rc['interface'] = 'virtual'
#     can.rc['channel'] = 'PCAN_USBBUS1'
#     can.rc['state'] = can.bus.BusState.PASSIVE
#     can.rc['bitrate'] = 500000

if env == "linux":
      channel = ch
      can.rc['interface'] = 'socketcan'
      can.rc['channel'] = channel
      can.rc['bitrate'] = 500000

async def main():
     p = Producer({
        'bootstrap.servers': 'SERVER',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms':'PLAIN',
        'sasl.username': 'USERNAME',
        'sasl.password': 'PASSWORD',
     })
     data = {
        'id': clientID,
        'can': {
             'timestamp':int(time.time()),
             'data': binascii.b2a_hex(os.urandom(8))
        }
     }
     print(data)
     p.produce(topic='dev', value=getJsonBytes(data), key=clientID)
     p.flush()



     bus = Bus()
    #
     for msg in bus:
         print(msg)
         data = {
              'id': clientID,
                 'can': {
                      'id': msg.arbitration_id,
                      'timestamp':msg.timestamp,
                      'data':  msg.data
                 }
        }
#         p.produce(topic='dev', key=clientID, value=msg.data)


def getJsonBytes(data):
    return json.dumps(data,default=json_util.default).encode('utf-8')

def loadTest():
    while True:
        data = {
            'timestamp':int(time.time()),
            'busTime':int(time.time()) - 2,
             'id': random.randint(0,50),
             'data': binascii.b2a_hex(os.urandom(8))
         }
        print (data)
        future = producer.send('Test', getJsonBytes(data) , key=b'23')

# def on_send_success(record_metadata):
#     print("yo")
#     print(record_metadata.topic)
#     print(record_metadata.partition)
#     print(record_metadata.offset)
#
# def on_send_error(excp):
#     log.error('I am an errback', exc_info=excp)
#     # handle exception


# produce keyed messages to enable hashed partitioning
# producer.send('cancar-events', key=b'foo', value=b'bar')


# produce json messages
# producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))
# producer.send('json-topic', {'key': 'value'})

# produce asynchronously
# for _ in range(100):
#     producer.send('cancar-events', b'msg')

# def on_send_success(record_metadata):
#     print(record_metadata.topic)
#     print(record_metadata.partition)
#     print(record_metadata.offset)
#
# def on_send_error(excp):
#     log.error('I am an errback', exc_info=excp)
#     # handle exception
#
# # produce asynchronously with callbacks
# producer.send('cancar-events', b'raw_bytes').add_callback(on_send_success).add_errback(on_send_error)
#
# # block until all async messages are sent
# producer.flush()
asyncio.run(main())
