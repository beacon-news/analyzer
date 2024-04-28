from abc import ABC, abstractmethod
from typing import Callable


class ScrapedArticleConsumer(ABC):
  """Called when a scraped article is received."""
  
  @abstractmethod
  def consume_article(self, callback: Callable[[dict], None], *callback_args) -> None:
    """Consume a scraped article, in form of a 'dict'"""
    raise NotImplementedError
  