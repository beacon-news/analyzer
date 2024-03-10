import pickle
from datetime import date

# Contains the OVR classification models

class ModelContainer: pass

class ModelContainer:

  def __init__(self, tfidf, clfs, thresholds, target_names, results, train_date):
    self.tfidf = tfidf
    self.clfs = clfs
    self.thresholds = thresholds
    self.target_names = target_names
    self.results = results
    self.train_date = train_date

  def save(self, filename):
    self.save_date = date.today()

    d = {
        "save_date": self.save_date,
        "train_date": self.train_date,
        "tfidf": self.tfidf,
        "clfs": self.clfs,
        "thresholds": self.thresholds,
        "target_names": self.target_names,
        "results": self.results,
    }
    with open(filename, 'wb') as f:
      pickle.dump(d, f, protocol=pickle.HIGHEST_PROTOCOL)
      print(f"saved {self.__class__.__name__} at {filename}")

  @classmethod
  def load(cls, filename) -> ModelContainer:
    with open(filename, 'rb') as f:
      d = pickle.load(f)
      save_date = d['save_date']
      train_date = d['train_date']
      tfidf = d['tfidf']
      clfs = d['clfs']
      thresholds = d['thresholds']
      t_names = d['target_names']
      results = d['results']
      mc = ModelContainer(tfidf, clfs, thresholds, t_names, results, train_date)
      mc.save_date = save_date
      print(f"loaded {cls.__name__} from {filename}")
      return mc