import pickle
from datetime import date
from utils import log_utils
import logging

class CategoryClassifier: pass

class CategoryClassifier:

  log = log_utils.create_console_logger(
    name='CategoryClassifier',
    level=logging.INFO
  )

  def __init__(self, tfidf, clfs, thresholds, target_names, results):
    self.tfidf = tfidf
    self.clfs = clfs
    self.thresholds = thresholds 
    self.target_names = target_names
    self.results = results

  def save(self, filename):
    self.last_save_date = date.today()
    with open(filename, 'wb') as f:
      pickle.dump(self, f, protocol=pickle.HIGHEST_PROTOCOL)
      self.log.info(f"saved {self.__class__.__name__} at {filename}")

  @classmethod
  def load(cls, filename) -> CategoryClassifier:
    with open(filename, 'rb') as f:
      mc = pickle.load(f)
      cls.log.info(f"loaded {cls.__name__} from {filename}")
    return mc

  def predict(self, text: str) -> list[str]:
    vect = self.tfidf.transform([text])

    labels = []
    for i in range(len(self.clfs)):
      pred = self.clfs[i].predict_proba(vect)
      if pred[0, 1] > self.thresholds[i]:
        labels.append(self.target_names[i])

    return labels