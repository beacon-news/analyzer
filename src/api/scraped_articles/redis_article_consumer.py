from api.scraped_articles.article_consumer import ScrapedArticleConsumer 
from api.redis_handler import RedisHandler 
import json
from typing import Callable

class RedisScrapedArticleConsumer(ScrapedArticleConsumer):

  def __init__(self, *args, stream_name, consumer_group, **kwargs):
    self.rh = RedisHandler(*args, **kwargs)
    self.stream_name = stream_name
    self.consumer_group = consumer_group
  
  def consume_article(self, callback: Callable[[dict], None], *callback_args) -> None:

    def message_extractor_wrapper(message: tuple[str, dict]):
      # transform the json redis message into a scraped article
      article = json.loads(message[1]["article"])
      callback(article, *callback_args)
    
    self.rh.consume_stream(self.stream_name, self.consumer_group, message_extractor_wrapper)

