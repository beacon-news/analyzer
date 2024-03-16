from classifier.model_container import ModelContainer
import numpy as np

class CategoryClassifier:

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

  def predict_batch(self, texts: list[str]) -> list[str]:
    vects = self.tfidf.transform(texts)

    labels = [[] for _ in range(len(texts))]
    for i in range(len(self.clfs)):
      pred = self.clfs[i].predict_proba(vects)
      doc_indices = np.argwhere(pred[:, 1] > self.thresholds[i]).flatten()
      for j in doc_indices:
        labels[j].append(self.target_names[i])
    return labels