from utils import log_utils
import logging
from topic_modeling.bertopic_container import BertopicContainer


class BertopicModel:

  log = log_utils.create_console_logger(
    name='BertopicModel',
    level=logging.INFO
  )

  def __init__(self, bertopic_container: BertopicContainer):
    self.bc = bertopic_container
  
  def infer_topic(self, docs) -> tuple[list, list]:
    topics, probabilities = self.bc.bertopic.transform(docs)
    return topics, probabilities

