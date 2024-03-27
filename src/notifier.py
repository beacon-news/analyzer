from abc import ABC, abstractmethod
from utils import log_utils
import logging
import redis
import time
import json

class Notifier(ABC):

  @abstractmethod
  def send_done_notification(self, ids: list[str]):
    raise NotImplementedError

class RedisStreamsNotifier(Notifier):

  def __init__(
      self, 
      redis_host: str = "localhost", 
      redis_port: int = 6379, 
      stream_name: str = "scraper_articles", 
      log_level: int = logging.INFO
  ):
    self.log = log_utils.create_console_logger(__name__, level=log_level)
    self.stream_name = stream_name

    try:
      self.__redis = redis.Redis(host=redis_host, port=redis_port)
      backoff = 1
      while not self.__redis.ping():
        print(f"redis not ready, waiting {backoff} seconds")
        time.sleep(backoff)
        backoff *= 2

    except Exception as e:
      self.log.exception("failed to connect to redis")
      raise e


  def send_done_notification(self, results: list[dict]) -> bool:
    try:
      notification = {
        "done": json.dumps(results)
      }
      r_id = self.__redis.xadd(self.stream_name, notification)
      self.log.debug(f"added ids to stream, redis message id {r_id}")
    except Exception as e:
      self.log.error(f"error when adding ids to redis stream")
      raise e

