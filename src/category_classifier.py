from utils import log_utils
import logging
from model_container import ModelContainer


class CategoryClassifier:

  log = log_utils.create_console_logger(
    name='CategoryClassifier',
    level=logging.INFO
  )

  def __init__(self, mc: ModelContainer):
    self.mc = mc
    self.tfidf = mc.tfidf
    self.clfs = mc.clfs
    self.thresholds = mc.thresholds
    self.target_names = mc.target_names

  def predict(self, text: str) -> list[str]:
    vect = self.tfidf.transform([text])

    labels = []
    for i in range(len(self.clfs)):
      pred = self.clfs[i].predict_proba(vect)
      if pred[0, 1] > self.thresholds[i]:
        labels.append(self.target_names[i])

    return labels