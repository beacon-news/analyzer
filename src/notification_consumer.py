from abc import ABC, abstractmethod
from redis_handler import RedisHandler
from typing import Callable
import json
import multiprocessing as mp

class NotificationConsumer(ABC):

  @abstractmethod
  def consume(self, callback: Callable[[dict], None], *callback_args) -> None:
    raise NotImplementedError
  

class RedisNotificationConsumer(NotificationConsumer):

  def __init__(self, *args, stream_name, consumer_group, **kwargs):
    self.rh = RedisHandler(*args, **kwargs)
    self.stream_name = stream_name
    self.consumer_group = consumer_group
  
  def consume(self, callback, *callback_args) -> None:

    def process_wrapper(message: tuple[str, dict]):
      msg = json.loads(message[1]["done_meta"])
      p = mp.Process(target=callback, args=(msg, *callback_args))
      p.start()
      
    self.rh.consume_stream(self.stream_name, self.consumer_group, process_wrapper)