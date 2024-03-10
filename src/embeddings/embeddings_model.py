from embeddings.embeddings_container import EmbeddingsContainer
import numpy as np


class EmbeddingsModel:

  def __init__(self, embeddings_container: EmbeddingsContainer):
    self.ec = embeddings_container

  def encode(self, docs) -> np.ndarray:
    return self.ec.embeddings_model.encode(docs)
