import pydantic
from datetime import datetime

class ScraperDoneNotification(pydantic.BaseModel):
  id: str
  url: str
  scrape_time: datetime