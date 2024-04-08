from abc import ABC, abstractmethod
from domain import Article


class AnalyzerRepository(ABC):

  @abstractmethod
  def store_analyzed_articles(self, analyzed_articles: list[Article]) -> list[str]:
    """Store a list of analyzed article objects in the repository, return the ids of the stored articles."""
    raise NotImplementedError 
