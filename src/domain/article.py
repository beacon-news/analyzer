import pydantic
from datetime import datetime
from domain.category import Category


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
  categories: list[Category]

  analyzed_categories: list[Category] # subset of 'categories', only contains the categories that were assigned by the analyzer
  embeddings: list[float]

  # topics part
  # topics are optional, will be added later by the topic modeler
  topics: list[ArticleTopic] | None = None
