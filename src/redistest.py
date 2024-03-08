

# -1. create the stream with pre configured maxlen
# 0. try creating the consumer group
# 1. process any pending messages (something happened between consuming and acking a message)
# 2. try to process any new messages + ack + delete processed messages (prevent trimming)
# 3. call xautoclaim to claim pending messages


# redis checker
# I delete stale consumers
# 1. get info about consumer groups for a stream
# 2. get info about every consumer in the consumer group for a stream
# 3. delete consumers with 0 pending messages and large enough idle/inactive time

# II remove old messages, keep stream size constant
# 1. get stream size periodically
# 2. if there are no pending messages in the stream, trim the stream length, otherwise generate some warning/error

import redis
import uuid
import time

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

REDIS_CONSUMER_GROUP = 'article_analyzer'
REDIS_STREAM_NAME = 'raw_articles'

consumer_name = f"{REDIS_CONSUMER_GROUP}_{uuid.uuid4().hex}"
read_timeout = 10000

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

backoff = 1
while not r.ping():
  print(f"redis not ready, waiting {backoff} seconds")
  time.sleep(backoff)
  backoff *= 2


# try creating the consumer group
try: 
  r.xgroup_create(name=REDIS_STREAM_NAME, groupname=REDIS_CONSUMER_GROUP, mkstream=True)
except Exception as e:
  print(e)


# 1. process any pending messages (something happened between consuming and acking a message)
# 2. try to process any new messages + ack + delete processed messages (prevent trimming)
# 3. call xautoclaim to claim pending messages

print(f"consumer {consumer_name} starting in consumer group {REDIS_CONSUMER_GROUP}")
last_id = "0"
check_pending_messages = True
while True:

  if check_pending_messages:
    # consume all pending messages since the last acked one
    id = last_id
  else:
    # only consume new messages
    id = ">"
  
  try:
    messages = r.xreadgroup(
      groupname=REDIS_CONSUMER_GROUP, 
      consumername=consumer_name, 
      streams={REDIS_STREAM_NAME: id}, 
      block=read_timeout,
      count=10
    )
  except Exception as e:
    print(e)

  print(f"got messages, check_pending: {check_pending_messages}")
  print(messages)

  if len(messages) == 0:
    print("timed out, no new messages")
    continue

  # when consuming pending messages, if the length is 0, we can start consuming new messages
  # when consuming new messages, the length will never be 0, we will check pending messages since the last acked message
  check_pending_messages = len(messages[0][1]) !=0 

  # consume the messages, either pending or new
  for message in messages[0][1]:
    try:
      r.xack(REDIS_STREAM_NAME, REDIS_CONSUMER_GROUP, message[0])
      last_id = message[0]

      # do we want to delete?
      # we might only want to trim here...
      # r.xdel(REDIS_STREAM_NAME, message[0])

      print(f"ack-d {message}")
    except Exception as e:
      print(e)
    
