import time
from utils import log_utils
from pymongo import MongoClient, collection


class MongoRepository:

  def __init__(self, host: str, port: int = None):
    self.log = log_utils.create_console_logger(
      self.__class__.__name__,
    )

    try:
      self.__mc = MongoClient(host=host, port=port, uuidRepresentation='standard')
      self.log.info(f"connected to mongodb, host {host}, port {port}")
    except Exception:
      self.log.exception("failed to connect to mongodb")
      raise

  def get_batch(self, db_name: str, collection_name: str, ids: list[str]):
    coll = self.__get_collection(db_name, collection_name)
    cursor = coll.find({"_id": { "$in": ids }})

    # TODO: don't read all at once, use batches
    return [doc for doc in cursor]

  def __get_collection(self, db_name: str, collection_name: str) -> collection.Collection:
    try:
      # assert database
      db = self.__mc.get_database(db_name)

      # assert collection
      return db.get_collection(collection_name)
    except Exception:
      self.log.exception(f"error while asserting database {db_name} and collection {collection_name}")
      raise

  def store_docs(self, db_name: str, collection_name: str, docs: list[dict]) -> list:
    coll = self.__get_collection(db_name, collection_name)
    
    try:
      res = coll.insert_many(docs)
      self.log.info(f"inserted {len(docs)} documents into collection {collection_name}")
      self.log.debug(f"inserted ids {res.inserted_ids}")
      return res.inserted_ids
    except Exception:
      self.log.exception(f"error when inserting documents into mongodb collection {collection_name}")
      raise