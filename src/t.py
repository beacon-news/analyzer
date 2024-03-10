from topic_modeling.bertopic_container import BertopicContainer
from embeddings.embeddings_container import EmbeddingsContainer

print("importing bertopic")
# from bertopic import BERTopic
print("finished importing bertopic")
# from bertopic.representation import MaximalMarginalRelevance
# from umap import UMAP
# from hdbscan import HDBSCAN
# from sklearn.feature_extraction.text import CountVectorizer

if __name__ == '__main__':

  ec = EmbeddingsContainer.load('models/embeddings/embeddings_container_2024-03-10.pkl')
  bc = BertopicContainer.load('models/bertopic/bertopic_container_2024-03-10.pkl', ec.embeddings_model)

  t = 'Donald Trump caught in another scandal while he was on vacation in Canada'
  # print(ec.embeddings_model.encode([t]))

  topics, probabilities = bc.bertopic.transform([t])

  # from sentence_transformers import SentenceTransformer

  # m = SentenceTransformer('all-MiniLM-L6-v2')
  # print(m.encode([t]))

  print(bc.bertopic.get_topic(topics[0]))


  # print(topics)
  # print(probabilities)