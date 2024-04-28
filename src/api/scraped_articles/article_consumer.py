from abc import ABC, abstractmethod
from typing import Callable


class ScrapedArticleConsumer(ABC):
  """Called when a scraped article is received."""
  
  @abstractmethod
  def consume_article(self, callback: Callable[[dict, Callable[[], None]], None], *callback_args) -> None:
    """Consume a scraped article with a callback, which takes a 'dict', an 'ack' function, and args."""
    raise NotImplementedError
  