from api.notifications.notification_consumer import ScraperEventConsumer 
from api.redis_handler import RedisHandler 
from api.notifications.scraper_notification import ScraperDoneNotification
import json
from typing import Callable

class RedisScraperEventConsumer(ScraperEventConsumer):

  def __init__(self, *args, stream_name, consumer_group, **kwargs):
    self.rh = RedisHandler(*args, **kwargs)
    self.stream_name = stream_name
    self.consumer_group = consumer_group
  
  def consume_done_notification(self, callback: Callable[[ScraperDoneNotification], None], *callback_args) -> None:

    def message_extractor_wrapper(message: tuple[str, dict], ack: Callable[[], None]):
      # transform the json redis message into a list of scraper done notifications
      notification_list = json.loads(message[1]["done"])
      notifications = [ScraperDoneNotification(
        id=notification["id"],
      ) for notification in notification_list]

      callback(notifications, *callback_args)

      # ack the consumed message
      ack()
      
    self.rh.consume_stream(self.stream_name, self.consumer_group, message_extractor_wrapper)