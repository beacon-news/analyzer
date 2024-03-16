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
  
    except Exception as e:
      self.log.exception("failed to connect to mongodb")
      raise e

  def watch_collection(
      self, 
      db_name: str, 
      collection_name: str, 
      interval_ms: int, 
      max_count: int, 
      callback, 
      *callback_args
    ):
    try:
      # assert database
      self.__db = self.__mc.get_database(db_name)
      self.log.info(f"using database {db_name}")

      # assert collection
      self.__col = self.__db.get_collection(collection_name)
      self.log.info(f"using collection {collection_name}")
    except Exception:
      self.log.exception(f"error while asserting database {db_name} and collection {collection_name}")
    
    self.log.info(f"watching collection {collection_name}, interval {interval_ms}ms")
    
    while True:
      try:
        count = self.__col.count_documents({})

        if count == 0:
          self.log.info(f"no documents in collection {collection_name}")
          time.sleep(interval_ms / 1000)
          continue

        self.log.info(f"processing {count} documents from collection {collection_name}")

        cursor = self.__col.find({}, {
          "_id": 0,
        }, limit=max_count)

        docs = [doc for doc in cursor]
        callback(docs, *callback_args)

      except Exception:
        self.log.exception(f"error while watching collection {collection_name}")

      time.sleep(interval_ms / 1000)
    
