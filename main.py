from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys
import can
import json
from bson import json_util
import time
import random
import os,binascii

env = sys.argv[1]
print("env: " + env)
kafkaServer = sys.argv[2]
print("server: " + kafkaServer)
ch =sys.argv[3]
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

producer = KafkaProducer(bootstrap_servers=[kafkaServer],api_version=(3,0,0))

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
        future = producer.send('cancar-events', getJsonBytes(data) , key=b'23')

loadTest()
print(producer)
# Asynchronous by default



# # Block for 'synchronous' sends
# try:
#     record_metadata = future.get(timeout=2)
#     print(record_metadata)
# except KafkaError as err:
#     # Decide what to do if produce request failed...
#     print(err)
#     pass

from can.interface import Bus
bus = Bus()



for msg in bus:
    print(msg)
    data = {
        'timestamp':msg.timestamp,
        'id': msg.arbitration_id,
        'data': msg.data
    }
    future = producer.send('cancar-events', key=bytes(str(msg.arbitration_id), encoding='ascii'), value=bytes(msg.data))

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
