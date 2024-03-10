import pickle
from datetime import date

# Contains bertopic model for topic modeling

class BertopicContainer: pass

class BertopicContainer:

  def __init__(self, bertopic, train_date):
    self.bertopic = bertopic
    self.train_date = train_date

  def save_without_embeddings_model(self, filename):
    # set the embedding model to None 
    em = self.bertopic.embedding_model.embedding_model
    self.bertopic.embedding_model.embedding_model = None

    self.save_date = date.today()

    d = {
        "save_date": self.save_date,
        "train_date": self.train_date,
        "bertopic": self.bertopic,
    }
    with open(filename, 'wb') as f:
      pickle.dump(d, f, protocol=pickle.HIGHEST_PROTOCOL)
      print(f"saved {self.__class__.__name__} without embeddings model at {filename}")
    
    # restore the embedding model
    self.bertopic.embedding_model.embedding_model = em

  @classmethod
  def load(cls, filename, embeddings_model) -> BertopicContainer:
    with open(filename, 'rb') as f:
      print(f"loading {cls.__name__} from {filename}")
      d = pickle.load(f)
      save_date = d['save_date']
      train_date = d['train_date']
      bertopic = d['bertopic']

      # set the embedding model separately
      bertopic.embedding_model.embedding_model = embeddings_model

      bc = BertopicContainer(bertopic, train_date)
      bc.save_date = save_date
      print(f"loaded {cls.__name__} from {filename}")
      return bc