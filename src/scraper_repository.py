from abc import ABC, abstractmethod
from mongodb_repository import MongoRepository


class ScraperRepository(ABC):

  @abstractmethod
  def get_article_batch(self, article_ids: list[str]) -> list[dict]:
    raise NotImplementedError 


class MongoScraperRepository(ScraperRepository):

  def __init__(self, *args, db_name, collection_name, **kwargs):

    self.mr = MongoRepository(*args, **kwargs)
    self.db_name = db_name
    self.collection_name = collection_name

  def get_article_batch(self, article_ids: list[str]) -> list[dict]:
    return self.mr.get_batch(self.db_name, self.collection_name, article_ids)
