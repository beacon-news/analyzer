from abc import ABC, abstractmethod

class ArticleStore(ABC):
  """
  Stores the articles that have been scraped.
  """

  @abstractmethod
  def store(self, article_url: str, article: dict) -> bool:
    """
    Stores the article.
    """
    raise NotImplementedError
  