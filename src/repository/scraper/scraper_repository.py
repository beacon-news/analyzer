from abc import ABC, abstractmethod


class ScraperRepository(ABC):

  @abstractmethod
  def get_article_batch(self, article_ids: list[str]) -> list[dict]:
    raise NotImplementedError 
