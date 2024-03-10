import pickle
from datetime import date

# Contains the documents, embeddings, embeddings model

class EmbeddingsContainer: pass

class EmbeddingsContainer:

  def __init__(self, docs, embeddings, embeddings_model, embeddings_model_name, train_date):
    self.docs = docs
    self.embeddings = embeddings
    self.embeddings_model = embeddings_model
    self.embeddings_model_name = embeddings_model_name
    self.train_date = train_date

  def save(self, filename):
    self.save_date = date.today()

    d = {
        "save_date": self.save_date,
        "train_date": self.train_date,
        "docs": self.docs,
        "embeddings": self.embeddings,
        "embeddings_model": self.embeddings_model,
        "embeddings_model_name": self.embeddings_model_name,
    }
    with open(filename, 'wb') as f:
      pickle.dump(d, f, protocol=pickle.HIGHEST_PROTOCOL)
      print(f"saved {self.__class__.__name__} at {filename}")

  @classmethod
  def load(cls, filename) -> EmbeddingsContainer:
    with open(filename, 'rb') as f:
      print(f"loading {cls.__name__} from {filename}")
      d = pickle.load(f)
      save_date = d['save_date']
      train_date = d['train_date']
      docs = d['docs']
      embeddings = d['embeddings']
      embeddings_model = d['embeddings_model']
      embeddings_model_name = d['embeddings_model_name']
      ec = EmbeddingsContainer(docs, embeddings, embeddings_model, embeddings_model_name, train_date)
      ec.save_date = save_date
      print(f"loaded {cls.__name__} from {filename}")
      return ec