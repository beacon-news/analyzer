import pydantic
from datetime import datetime


class ArticleTopic(pydantic.BaseModel):
  id: str
  topic: str


class Article(pydantic.BaseModel):
  id: str
  url: str
  source: str | None = None
  publish_date: datetime
  image: str | None = None
  author: list[str]
  title: list[str]
  paragraphs: list[str]

  # analyzer part
  analyze_time: datetime

  # contains both the analyzed and the metadata categories
  categories: list[str]

  analyzed_categories: list[str] # subset of 'categories', only contains the categories that were assigned by the analyzer
  embeddings: list[float]
  entities: list[str]

  # topics part
  # topics are optional, will be added later by the topic modeler
  topics: list[ArticleTopic] | None = None


class ScrapedArticleMetadata(pydantic.BaseModel):
  source: str | None = None
  categories: list[str] | None = None 


class ScrapedArticle(pydantic.BaseModel):
  id: str
  url: str
  metadata: ScrapedArticleMetadata
  publish_date: datetime
  image: str | None = None
  author: list[str]
  title: list[str]
  paragraphs: list[str]

# class ArticleAnalysis(pydantic.BaseModel):
#   analyze_time: datetime
#   categories: list[str]
#   embeddings: list[float]
#   entities: list[str]

#   # topics are optional, will be added later by the topic modeler
#   topics: list[ArticleTopic] | None = None

# class ArticleAggregate(pydantic.BaseModel):
#   id: str
#   article: Article
#   analysis: ArticleAnalysis
