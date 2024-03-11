from pathlib import Path
from article_store import ArticleStore
from utils import log_utils
import logging
from urllib.parse import urlparse
import json


class FileArticleStore(ArticleStore):

  @classmethod
  def configure_logging(cls, level: int):
    cls.loglevel = level
    cls.log = log_utils.create_console_logger(
      name=cls.__name__,
      level=level
    )

  def __init__(self, output_dir: str, log_level: int = logging.INFO):
    self.configure_logging(log_level)
    self.__output_dir = output_dir
    self.__create_output_dir()
  
  def __create_output_dir(self):
    file = Path(self.__output_dir)
    file.parent.mkdir(parents=True, exist_ok=True)
    file.touch(exist_ok=True)
    self.log.info(f"created/asserted output dir: {self.__output_dir}")
  
  def store(self, article_url: str, article_result: dict) -> bool:
    parsed_url = urlparse(article_url)
    article_file = f"{parsed_url.netloc}{parsed_url.path.replace('/','_')}.json"

    dir = Path(self.__output_dir)
    article_path = str(dir.joinpath(Path(article_file)))

    with open(article_path, "w") as f:
      json.dump(article_result, f)

    self.log.info(f"saved article with url {article_url} to {article_path}")
    return True
