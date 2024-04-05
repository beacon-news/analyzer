from analysis.embeddings.embeddings_container import EmbeddingsModelContainer
import numpy as np
from utils import log_utils
import logging


class EmbeddingsModel:

  @classmethod
  def configure_logging(cls, level: int):
    cls.log = log_utils.create_console_logger(
      name=cls.__name__,
      level=level
    )

  def __init__(self, embeddings_container: EmbeddingsModelContainer, log_level: int = logging.INFO):
    self.ec = embeddings_container
    self.configure_logging(log_level)

  def encode(self, docs) -> np.ndarray:
    self.log.info(f"embedding batch of {len(docs)} documents")
    return self.ec.embeddings_model.encode(docs)
