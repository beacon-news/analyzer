import pydantic
from datetime import datetime

class Article(pydantic.BaseModel):
  id: str
  url: str
  publish_date: datetime
  author: list[str]
  title: list[str]
  paragraphs: list[str]

class AnalyzedArticle(pydantic.BaseModel):
  article: Article
  analyze_time: datetime
  categories: list[str]
  embeddings: list[float]
  entities: list[str]
