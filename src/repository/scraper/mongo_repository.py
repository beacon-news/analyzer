from utils import log_utils
from pymongo import MongoClient
from repository.scraper.scraper_repository import ScraperRepository


class MongoRepository(ScraperRepository):

  def __init__(
    self, 
    host: str, 
    port: int, 
    db_name: str,
    collection_name: str,
  ):
    self.log = log_utils.create_console_logger(
      self.__class__.__name__,
    )

    try:
      self.__mc = MongoClient(host=host, port=port, uuidRepresentation='standard')
      self.db = self.__mc.get_database(db_name)
      self.collection = self.db.get_collection(collection_name)

      self.log.info(f"connected to mongodb, host {host}, port {port}, db {db_name}, collection {collection_name}")
    except Exception:
      self.log.exception("failed to connect to mongodb")
      raise

  def get_article_batch(self, ids: list[str]):
    cursor = self.collection.find({"_id": { "$in": ids }})

    # TODO: don't read all at once, use batches, maybe generators
    return [doc for doc in cursor]
