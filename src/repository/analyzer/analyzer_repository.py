from abc import ABC, abstractmethod
from domain import AnalyzedArticle


class AnalyzerRepository(ABC):

  @abstractmethod
  def store_analyzed_articles(self, analyzed_articles: list[AnalyzedArticle]) -> list[str]:
    """Store a list of analyzed article objects in the repository, return the ids of the stored articles."""
    raise NotImplementedError 
