from utils import log_utils
from domain import Article, Category
import logging
from elasticsearch import Elasticsearch, exceptions, helpers
from repository.analyzer.analyzer_repository import AnalyzerRepository


class ElasticsearchRepository(AnalyzerRepository):

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
    self.articles_index = "articles"
    self.categories_index = "categories"

    # TODO: secure with TLS
    # TODO: add some form of auth
    self.log.info(f"connecting to Elasticsearch at {conn}")
    self.es = Elasticsearch(conn, basic_auth=(user, password), ca_certs=cacerts, verify_certs=verify_certs)
    self.__assert_articles_index()
    self.__assert_categories_index()

  def __assert_articles_index(self):
    try:
      self.log.info(f"creating/asserting index '{self.articles_index}'")
      self.es.indices.create(index=self.articles_index, mappings={
        "properties": {
          "topics": {
            "properties": {
              "topic_ids": {
                "type": "keyword"
              },
              "topic_names": {
                "type": "text"
              }
            }
          },
          "analyzer": {
            "properties": {
              "category_ids": {
                # don't index the analyzer-generated categories, index the merged ones instead
                # only to be able to differentiate between the predicted and predefined categories
                "enabled": "false",
                "type": "keyword",
              },
              "embeddings": {
                "type": "dense_vector",
                "dims": 384, # depends on the embeddings model
              },
              "entities": {
                "type": "text"
              },
            }
          },
          "article": {
            "properties": {
              "id": {
                "type": "keyword",
              },
              "url": {
                "type": "keyword",
              },
              "source": {
                "type": "text",
                # keyword mapping needed so we can do aggregations
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              },
              "publish_date": {
                "type": "date",
              },
              "image": {
                "type": "keyword",
                "enabled": "false", # don't index image urls
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
              "categories": {
                "properties": {
                  "ids" : {
                    "type": "keyword"
                  },
                  "names": {
                    "type": "text",
                    # keyword mapping needed so we can do aggregations
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  }
                }
              },
            }
          }
        }
      })
    except exceptions.BadRequestError as e:
      if e.message == "resource_already_exists_exception":
        self.log.info(f"index {self.articles_index} already exists")
  
  def __assert_categories_index(self):
    try:
      self.log.info(f"creating/asserting index '{self.categories_index}'")
      self.es.indices.create(index=self.categories_index, mappings={
        "properties": {
          "name": {
            "type": "text",
          }
        }
      })
    except exceptions.BadRequestError as e:
      if e.message == "resource_already_exists_exception":
        self.log.info(f"index {self.categories_index} already exists")
  
  def store_analyzed_articles(self, analyzed_articles: list[Article]) -> list[str]:
    """Store the analyzed articles in 'streaming bulk' mode."""

    docs = [self.__map_to_repo_doc(art) for art in analyzed_articles] 
    ids = []
    self.log.info(f"attempting to insert {len(docs)} articles in {self.articles_index}")
    for ok, action in helpers.streaming_bulk(self.es, self.__generate_article_actions(docs)):
      if not ok:
        self.log.error(f"failed to bulk store article: {action}")
        continue
      ids.append(action["index"]["_id"])
      self.log.debug(f"successfully stored article: {action}")
    return ids
  
  def __map_to_repo_doc(self, article: Article) -> dict:
    # create repository model from analyzed article
    return {
      "analyze_time": article.analyze_time.isoformat(),
      "analyzer": {
        "category_ids": [cat.id for cat in article.analyzed_categories],
        "entities": article.entities,
        "embeddings": article.embeddings,
      },
      "article": {
        "id" : article.id,
        "url": article.url,
        "source": article.source,
        "publish_date": article.publish_date.isoformat(),
        "image": article.image,
        "author": article.author,
        "title": article.title,
        "paragraphs": article.paragraphs,
        "categories": {
          "ids": [cat.id for cat in article.categories],
          "names": [cat.name for cat in article.categories],
        },
      }, 
      # topics are NOT added here, they will be added by the topic modeler
    }
  
  def __generate_article_actions(self, articles: list[dict]):
    for i in range(len(articles)):
      action = {
        "_id": articles[i]["article"]["id"],
        "_index": self.articles_index,
        **articles[i]
      }
      yield action
  

  def store_categories(self, categories: list[Category]) -> list[str]:
    """Store the categories in 'streaming bulk' mode."""

    ids = []
    self.log.info(f"attempting to insert {len(categories)} categories in {self.categories_index}")
    for ok, action in helpers.streaming_bulk(self.es, self.__generate_category_actions(categories)):
      if not ok:
        self.log.error(f"failed to bulk store category: {action}")
        continue
      ids.append(action["index"]["_id"])
      self.log.debug(f"successfully stored category: {action}")
    return ids
  
  def __generate_category_actions(self, categories: list[Category]):
    for i in range(len(categories)):
      action = {
        "_id": categories[i].id,
        "_index": self.categories_index,
        "name": categories[i].name,
      }
      yield action