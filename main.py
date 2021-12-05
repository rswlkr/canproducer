from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys
import can

env = sys.argv[1]
print(env)
if env == "mac":
    can.rc['interface'] = 'virtual'
#     can.rc['channel'] = 'PCAN_USBBUS1'
#     can.rc['state'] = can.bus.BusState.PASSIVE
#     can.rc['bitrate'] = 500000

if env == "linux":
      channel = sys.argv[2]
      can.rc['interface'] = 'socketcan'
      can.rc['channel'] = channel
      can.rc['bitrate'] = 500000

from can.interface import Bus
bus = Bus()

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(3,0,0))

for msg in bus:
    print(msg)
    future = producer.send('cancar-events',  key=bytes(msg.id) value=bytes(msg.data))


# Asynchronous by default
print(future)

# Block for 'synchronous' sends
try:
    record_metadata = future.get(timeout=10)
except KafkaError:
    # Decide what to do if produce request failed...
    log.exception()
    pass

# Successful result returns assigned partition and offset
print (record_metadata.topic)
print (record_metadata.partition)
print (record_metadata.offset)

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

# configure multiple retries
producer = KafkaProducer(retries=5)