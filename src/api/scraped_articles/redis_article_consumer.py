from api.scraped_articles.article_consumer import ScrapedArticleConsumer 
from api.redis_handler import RedisHandler 
import json
from typing import Callable

class RedisScrapedArticleConsumer(ScrapedArticleConsumer):

  def __init__(self, redis_handler: RedisHandler, stream_name, consumer_group):
    self.rh = redis_handler
    self.stream_name = stream_name
    self.consumer_group = consumer_group
  
  def consume_article(self, callback: Callable[[dict, Callable[[], None]], None], *callback_args) -> None:

    def message_extractor_wrapper(message: tuple[str, dict], ack: Callable[[], None]):
      # transform the json redis message into a scraped article
      print(message[0])
      article = json.loads(message[1]["article"])
      callback(article, ack, *callback_args)
    
    self.rh.consume_stream(self.stream_name, self.consumer_group, message_extractor_wrapper)

