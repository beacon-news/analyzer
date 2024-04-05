from notifications.notification_consumer import NotificationConsumer 
from notifications.redis_handler import RedisHandler 
from notifications.scraper_notification import ScraperDoneNotification
import json

class RedisNotificationConsumer(NotificationConsumer):

  def __init__(self, *args, stream_name, consumer_group, **kwargs):
    self.rh = RedisHandler(*args, **kwargs)
    self.stream_name = stream_name
    self.consumer_group = consumer_group
  
  def consume_scraper_done(self, callback, *callback_args) -> None:

    def message_extractor_wrapper(message: tuple[str, dict]):
      # transform the json redis message int a list of scraper done notifications
      notification_list = json.loads(message[1]["done"])
      notifications = [ScraperDoneNotification(
        id=notification["id"],
        url=notification["url"],
        scrape_time=notification["scrape_time"],
      ) for notification in notification_list]

      callback(notifications, *callback_args)
      
    self.rh.consume_stream(self.stream_name, self.consumer_group, message_extractor_wrapper)