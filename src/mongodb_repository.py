import time
from utils import log_utils
from pymongo import MongoClient


class MongoRepository:

  def __init__(self, host: str, port: int = None):
    self.log = log_utils.create_console_logger(
      self.__class__.__name__,
    )

    try:
      self.__mc = MongoClient(host=host, port=port)
      info = self.__mc.server_info()
      self.log.info(f"connected to mongodb, host {host}, port {port}, server info {info}")
  
    except Exception:
      self.log.exception("failed to connect to mongodb")
      raise

  def watch_collection(
      self, 
      db_name: str, 
      collection_name: str, 
      interval_ms: int, 
      max_count: int, 
      processed_update_field: str,
      callback, 
      *callback_args
    ):
    try:
      # assert database
      db = self.__mc.get_database(db_name)
      self.log.info(f"using database {db_name}")

      # assert collection
      collection = db.get_collection(collection_name)
      self.log.info(f"using collection {collection_name}")
    except Exception:
      self.log.exception(f"error while asserting database {db_name} and collection {collection_name}")
      raise
    
    self.log.info(f"watching collection {collection_name}, interval {interval_ms}ms")
    
    while True:
      try:
        # count = collection.count_documents({})

        # if count == 0:
        #   self.log.info(f"no documents in collection {collection_name}")
        #   time.sleep(interval_ms / 1000)
        #   continue

        # self.log.info(f"processing {count} documents from collection {collection_name}")

        # find all documents that have not been processed
        cursor = collection.find(
          {
            processed_update_field: { "$exists": False }
          }, 
          limit=max_count
        )

        docs = [doc for doc in cursor]
        if len(docs) == 0:
          self.log.info(f"no documents in cursor, sleeping")
          time.sleep(interval_ms / 1000)
          continue

        self.log.info(f"processing {len(docs)} documents from collection {collection_name}")

        callback(docs, *callback_args)

        # mark all documents as processed
        collection.update_many(
          {
            "_id": {
              "$in": [doc["_id"] for doc in docs]
            }
          },
          {
            "$set": {
              processed_update_field: True
            }
          }
        )
        self.log.info(f"updated {len(docs)} documents as processed from collection {collection_name}")

      except Exception:
        self.log.exception(f"error while watching collection {collection_name}")
        raise
    
  
  # def store_article(self, db_name: str, collection_name: str, article: dict):
  #   try:
  #     # assert database
  #     db = self.__mc.get_database(db_name)

  #     # assert collection
  #     collection = db.get_collection(collection_name)
  #   except Exception:
  #     self.log.exception(f"error while asserting database {db_name} and collection {collection_name}")
  #     raise
    
  #   try:
  #     collection.insert_one(article)
  #     self.log.info(f"inserted article into collection {collection_name}")
  #   except Exception:
  #     self.log.exception(f"error when inserting article into mongodb collection {collection_name}")
  #     raise
    
  def store_articles(self, db_name: str, collection_name: str, articles: list[dict]):
    try:
      # assert database
      db = self.__mc.get_database(db_name)

      # assert collection
      collection = db.get_collection(collection_name)
    except Exception:
      self.log.exception(f"error while asserting database {db_name} and collection {collection_name}")
      raise
    
    try:
      res = collection.insert_many(articles)
      self.log.info(f"inserted {len(articles)} articles into collection {collection_name}")
      self.log.debug(f"inserted ids {res.inserted_ids}")
    except Exception:
      self.log.exception(f"error when inserting articles into mongodb collection {collection_name}")
      raise
