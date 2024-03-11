from article_store import ArticleStore
from utils import log_utils
import logging
from urllib.parse import urlparse
import json
from elasticsearch import Elasticsearch


class ElasticSearchStore(ArticleStore):

  @classmethod
  def configure_logging(cls, level: int):
    cls.loglevel = level
    cls.log = log_utils.create_console_logger(
      name=cls.__name__,
      level=level
    )

  def __init__(self, connection_str: str, log_level: int = logging.INFO):
    self.configure_logging(log_level)

    # TODO: secure with TLS
    # TODO: add some form of auth
    self.es = Elasticsearch(connection_str)

    # assert articles index
    

  
  # def store(self, article_url: str, article_result: dict) -> bool:



