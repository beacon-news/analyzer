from utils import log_utils
import logging
from elasticsearch import Elasticsearch, exceptions, helpers
import uuid


class ElasticsearchStore:

  @classmethod
  def configure_logging(cls, level: int):
    cls.log = log_utils.create_console_logger(
      name=cls.__name__,
      level=level
    )

  def __init__(
      self, 
      conn: str, 
      user: str, 
      password: str, 
      cacerts: str, 
      verify_certs: bool = True,
      log_level: int = logging.INFO
  ):
    self.configure_logging(log_level)
    self.index_name = "articles"

    # TODO: secure with TLS
    # TODO: add some form of auth
    self.log.info(f"connecting to Elasticsearch at {conn}")
    self.es = Elasticsearch(conn, basic_auth=(user, password), ca_certs=cacerts, verify_certs=verify_certs)

    # assert articles index
    try:
      self.log.info(f"creating/asserting index '{self.index_name}'")
      self.es.indices.create(index=self.index_name, mappings={
        "properties": {
          "analyzer": {
            "properties": {
              "categories": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              },
              "embeddings": {
                "type": "dense_vector",
                "dims": 384, # depends on model used
              },
              "entities": {
                "type": "text"
              },
            }
          },
          "article": {
            "properties": {
              "url": {
                "type": "keyword",
              },
              "publish_date": {
                "type": "date",
              },
              "author": {
                "type": "text",
              },
              "title": {
                "type": "text",
              },
              "paragraphs": {
                "type": "text",
              },
            }
          }
        }
      })
    except exceptions.BadRequestError as e:
      if e.message == "resource_already_exists_exception":
        self.log.info(f"index {self.index_name} already exists")

  
  def store(self, analyzed_article: dict) -> str:
    id = uuid.uuid4()
    resp = self.es.index(index=self.index_name, id=id, document=analyzed_article)
    self.log.info(f"stored article with id {resp['_id']} in {self.index_name}")
    return resp["_id"]

  def store_batch(self, analyzed_articles: list[dict]) -> list[str]:
    self.log.info(f"attempting to insert {len(analyzed_articles)} articles in {self.index_name}")
    for ok, action in helpers.streaming_bulk(self.es, self.__generate_doc_actions(analyzed_articles)):
      if not ok:
        self.log.error(f"failed to bulk store article: {action}")
        continue
  
  def __generate_doc_actions(self, articles: list[dict]):
    for i in range(len(articles)):
      id = str(uuid.uuid4())
      action = {
        "_id": id,
        "_index": self.index_name,
        **articles[i]
      }
      yield action