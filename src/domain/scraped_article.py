import pydantic
from datetime import datetime


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
