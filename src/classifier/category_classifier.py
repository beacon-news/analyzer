from classifier.model_container import ModelContainer
import numpy as np
from utils import log_utils
import logging

class CategoryClassifier:

  @classmethod
  def configure_logging(cls, level: int):
    cls.log = log_utils.create_console_logger(
      name=cls.__name__,
      level=level
    )

  def __init__(self, mc: ModelContainer, log_level: int = logging.INFO):
    self.configure_logging(log_level)
    self.mc = mc
    self.tfidf = mc.tfidf
    self.clfs = mc.clfs
    self.thresholds = mc.thresholds
    self.target_names = mc.target_names

  def predict(self, text: str) -> list[str]:
    self.log.info(f"predicting category for single document {text[:20]}...")
    vect = self.tfidf.transform([text])

    labels = []
    for i in range(len(self.clfs)):
      pred = self.clfs[i].predict_proba(vect)
      if pred[0, 1] > self.thresholds[i]:
        labels.append(self.target_names[i])

    return labels

  def predict_batch(self, texts: list[str]) -> list[str]:
    self.log.info(f"predicting category for batch of {len(texts)} documents")
    
    vects = self.tfidf.transform(texts)

    labels = [[] for _ in range(len(texts))]
    for i in range(len(self.clfs)):
      pred = self.clfs[i].predict_proba(vects)
      doc_indices = np.argwhere(pred[:, 1] > self.thresholds[i]).flatten()
      for j in doc_indices:
        labels[j].append(self.target_names[i])
    return labels