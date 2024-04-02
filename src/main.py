import os
from notification_consumer import *
from classifier import CategoryClassifier, ModelContainer
from embeddings import EmbeddingsModelContainer, EmbeddingsModel
from ner import SpacyEntityRecognizer
from utils import log_utils
from article_store.elasticsearch_store import ElasticsearchStore
from datetime import datetime
from scraper_repository import *
from analyzer_repository import *
from notifier import *

def check_env(name: str, default=None) -> str:
  value = os.environ.get(name, default)
  if value is None:
    raise ValueError(f'{name} environment variable is not set')
  return value


CAT_CLF_MODEL_PATH = check_env('CAT_CLF_MODEL_PATH')
EMBEDDINGS_MODEL_PATH = check_env('EMBEDDINGS_MODEL_PATH')
SPACY_MODEL = check_env('SPACY_MODEL')
SPACY_MODEL_DIR = check_env('SPACY_MODEL_DIR')


REDIS_HOST = check_env('REDIS_HOST', 'localhost')
REDIS_PORT = int(check_env('REDIS_PORT', 6379))

REDIS_CONSUMER_GROUP = check_env('REDIS_CONSUMER_GROUP', 'article_analyzer')
REDIS_STREAM_NAME = check_env('REDIS_STREAM_NAME', 'scraper_articles')

ELASTIC_USER = check_env('ELASTIC_USER', 'elastic')
ELASTIC_PASSWORD = check_env('ELASTIC_PASSWORD')
ELASTIC_CONN = check_env('ELASTIC_HOST', 'https://localhost:9200')
ELASTIC_CA_PATH = check_env('ELASTIC_CA_PATH', 'certs/_data/ca/ca.crt')
ELASTIC_TLS_INSECURE = bool(check_env('ELASTIC_TLS_INSECURE', False))


MONGO_HOST = check_env('MONGO_HOST', 'localhost')
MONGO_PORT = int(check_env('MONGO_PORT', 27017))
MONGO_DB_SCRAPER = check_env('MONGO_DB_SCRAPER', 'scraper')
# MONGO_DB_ANALYZER = check_env('MONGO_DB_ANALYZER', 'analyzer')
MONGO_COLLECTION_SCRAPER = check_env('MONGO_COLLECTION_SCRAPER', 'scraped_articles')
# MONGO_COLLECTION_ANALYZER = check_env('MONGO_COLLECTION_ANALYZER', 'scraped_articles')

mc = ModelContainer.load(CAT_CLF_MODEL_PATH)
clf = CategoryClassifier(mc)

ec = EmbeddingsModelContainer.load(EMBEDDINGS_MODEL_PATH)
em = EmbeddingsModel(ec)

ser = SpacyEntityRecognizer(SPACY_MODEL, SPACY_MODEL_DIR)

es = ElasticsearchStore(
  ELASTIC_CONN, 
  ELASTIC_USER, 
  ELASTIC_PASSWORD, 
  ELASTIC_CA_PATH, 
  not ELASTIC_TLS_INSECURE
)

rh = RedisHandler(REDIS_HOST, REDIS_PORT)


def print_message(message):
  print(f"received from stream: {message}")


log = log_utils.create_console_logger("Analyzer")

# TODO: use interfaces for better arch

scraper_repo = MongoScraperRepository(
  host=MONGO_HOST,
  port=MONGO_PORT,
  db_name=MONGO_DB_SCRAPER,
  collection_name=MONGO_COLLECTION_SCRAPER,
) 

# analyzer_repo = MongoAnalyzerRepository(
#   host=MONGO_HOST,
#   port=MONGO_PORT,
#   db_name=MONGO_DB_ANALYZER,
#   collection_name=MONGO_COLLECTION_ANALYZER,
# ) 

notifier = RedisStreamsNotifier(
  REDIS_HOST,
  REDIS_PORT,
  stream_name="analyzer_articles",
)

def process_notification(message: dict):

  ids = [m['id'] for m in message]
  
  # TODO: use Processes and a process pool executor

  # get the scraped batch
  docs = scraper_repo.get_article_batch(ids) 
  if len(docs) == 0:
    log.warning(f"no documents found in scraped batch, exiting")
    return
  
  results = []

  # process the batch
  process(docs, results)

  log.info(f"processed {len(results)} elements: {results}")


def process(docs: list[dict], results=list):

  transformed_docs = []
  prepared_texts = []

  try: 
    for doc in docs:

      article = transform_doc(doc)
      if article is None:
        continue

      transformed_docs.append(article)

      # extract text for analysis for each document
      text = '\n'.join(article["title"]) + '\n'.join(article["paragraphs"])
      prepared_texts.append(text)

    # run analysis in batch
    labels, embeddings, entities = analyze_batch(prepared_texts)

    # merge transformed docs with analysis results
    analyzed_docs = []
    for i in range(len(transformed_docs)):
      analyzed_docs.append({
        "categories": labels[i],
        "entities": entities[i],
        "embeddings": embeddings[i],
      })
    
    final_docs = [
      {
        "analyze_time": datetime.now(),
        "analyzer": {
          "categories": art_labels,
          "entities": art_entities,
          "embeddings": art_embeddings,
        },
        "article": {
          **article
        }
      } for article, art_labels, art_embeddings, art_entities in zip(transformed_docs, labels, embeddings, entities)
    ]

    # replace publish_date with iso time string
    es_docs = []
    for doc in final_docs:
      doc['article']['publish_date'] = doc['article']['publish_date'].isoformat()
      es_docs.append(doc)
    
    # store in elasticsearch
    ids = es.store_article_batch(es_docs)
    log.info(f"done storing batch of {len(final_docs)} articles in Elasticsearch")

    results.extend(ids)

  except Exception:
    log.exception(f"error trying to analyze doc: {doc}")


def transform_doc(doc: dict) -> dict | None:
  if 'id' not in doc:
    log.error(f"no 'id' in doc: {doc}, skipping analysis")
    return None
  id = doc['id']

  if 'url' not in doc:
    log.error(f"no 'url' in doc: {doc}, skipping analysis")
    return None
  url = doc['url']

  if 'components' not in doc:
    log.error(f"no 'components' in doc: {doc}, skipping analysis")
    return None
  
  comps = doc['components']
  if 'article' not in comps:
    log.error(f"'components.article' not found in doc: {doc}, skipping analysis")
    return None
  
  comps = comps['article']
  if not isinstance(comps, list):
    log.error(f"'components.article' is not an array: {doc}, skipping analysis")
    return None
  
  # should only contain 1 title, and 1 paragraphs section, but just in case
  titles = [] 
  paras = [] 
  authors = []
  publish_date = None
  for component in comps:
    if 'title' in component:
      titles.append(component['title'])
    elif 'paragraphs' in component:
      if not isinstance(component['paragraphs'], list):
        log.error(f"'components.article.paragraphs' is not an array: {doc}, skipping analysis")
        return None
      paras.extend(component['paragraphs'])
    elif 'author' in component:
      authors.append(component['author'])
    elif 'publish_date' in component:
      publish_date = component['publish_date']
      publish_date = datetime.fromisoformat(publish_date).replace(second=0, microsecond=0)

  # transform the scraped format into a more manageable one
  return {
    "id": id, 
    "url": url,
    "publish_date": publish_date,
    "author": authors,
    "title": titles,
    "paragraphs": paras,
  }


def analyze_batch(texts: list[str]) -> list[tuple[list[str], list[float], list[str]]]:
  # classify the text
  labels = clf.predict_batch(texts)

  # create embeddings, take the first one as we only have 1 document
  embeddings = em.encode(texts)

  # get named entities
  entities = ser.ner_batch(texts)

  return (labels, embeddings.tolist(), entities)

# # from topic_modeling.bertopic_container import *
# # from topic_modeling.bertopic_model import *
# import numpy as np

# # # BERTOPIC_MODEL_PATH = check_env("BERTOPIC_MODEL_PATH")
# # # bc = BertopicContainer.load(BERTOPIC_MODEL_PATH, em.ec.embeddings_model)
# # # bm = BertopicModel(bc)

# log.info("importing BERTopic and its dependencies")
# from bertopic import BERTopic

# from umap import UMAP
# from hdbscan import HDBSCAN
# from sklearn.feature_extraction.text import CountVectorizer
# from bertopic.representation import PartOfSpeech, MaximalMarginalRelevance

# log.info("initializing BERTopic dependencies")

# umap_model = UMAP(
#     n_neighbors=15, # global / local view of the manifold default 15
#     n_components=5, # target dimensions default 5
#     metric='cosine',
#     min_dist=0.0 # smaller --> more clumped embeddings, larger --> more evenly dispersed default 0.0
# )

# hdbscan_model = HDBSCAN(
#     min_cluster_size=2, # nr. of points required for a cluster (documents for a topic) default 10
#     metric='euclidean',
#     cluster_selection_method='eom',
#     prediction_data=True, # if we want to approximate clusters for new points
# )

# vectorizer_model = CountVectorizer(
#     ngram_range=(1, 1),
#     stop_words='english',
# )

# # ps = PartOfSpeech("en_core_web_sm")
# mmr = MaximalMarginalRelevance(diversity=0.3)

# # representation_model = [ps, mmr]
# representation_model = mmr

# bt = BERTopic(
#     embedding_model=em.ec.embeddings_model,
#     umap_model=umap_model,
#     hdbscan_model=hdbscan_model,
#     vectorizer_model=vectorizer_model,
#     representation_model=representation_model,
#     # verbose=True
# )

# import pandas as pd

# pd.set_option('display.max_columns', None)

# def model_topics(docs):

#   doc_texts = ["\n".join(d["article"]["title"]) + "\n" + "\n".join(d["article"]["paragraphs"]) for d in docs]
#   doc_embeddings = np.array([d["analyzer"]["embeddings"] for d in docs])

#   log.info(f"fitting topic modeling model on {len(doc_texts)} docs")

#   # bm.bc.bertopic.fit_transform(doc_texts, doc_embeddings)
#   bt.fit_transform(doc_texts, doc_embeddings)

#   # ti = bm.bc.bertopic.get_topic_info()
#   ti = bt.get_topic_info()
#   print(ti)
#   print("=============================")


#   # 1. create the topics

#   d = {
#     "_id": ...,
#     "topic": "hungary nato orban",
#     "start_date": ...,
#     "end_date": ...,
#     "count": 103,
#     "repr_docs": [
#       {
#         "_id":...,
#         "url": ...,
#         "publish_date": ...,
#         "author": ...,
#         "title": ...,
#       },
#       {
#         "_id":...,
#         "url": ...,
#         "publish_date": ...,
#         "author": ...,
#         "title": ...,
#       },
#       {
#         "_id":...,
#         "url": ...,
#         "publish_date": ...,
#         "author": ...,
#         "title": ...,
#       },
#     ],
#   }

#   # create the topics without docs
#   topics = []
#   topic_info = bt.get_topic_info()

#   start_time = datetime.now().isoformat()
#   end_time = datetime.now().isoformat()

#   topic_df_dict = topic_info.loc[topic_info["Topic"] != -1, ["Count", "Representation"]].to_dict()
#   for d in zip(topic_df_dict["Count"].values(), topic_df_dict["Representation"].values()):
#     topics.append({
#       "_id": uuid.uuid4().hex, 
#       "start_time": start_time,
#       "end_time": end_time,
#       "topic": " ".join(d[1]),
#       "count": d[0],
#       "representative_articles": [],
#     })

#   # add docs to topics
#   doc_info = bt.get_document_info(doc_texts)

#   # don't filter out anything, we need the correct order to correlate with 'docs'
#   doc_df_dict = doc_info.loc[doc_info["Topic"] != -1, ["Topic", "Representative_document"]].to_dict()
#   for doc_ind, topic_ind, representative in zip(
#     doc_df_dict["Topic"].keys(), 
#     doc_df_dict["Topic"].values(), 
#     doc_df_dict["Representative_document"].values()
#   ):
    
#     # add representative doc
#     if representative:

#       # add duplicate of article to topic
#       art = docs[doc_ind]["article"]
#       art_duplicate = {
#         "_id": docs[doc_ind]["_id"],
#         "url": art["url"],
#         "publish_date": art["publish_date"],
#         "author": art["author"],
#         "title": art["title"],
#       }
#       topics[topic_ind]["representative_articles"].append(art_duplicate)

#     # update the article with duplicate of topic
#     # TODO: upsert with array item
#     # if "topics" not in docs[doc_ind]["analyzer"]: 
#     #   docs[doc_ind]["analyzer"]["topics"] = []
    
#     # docs[doc_ind]["analyzer"]["topics"].append(
#     #   {
#     #     "_id": topics[topic_ind]["_id"],
#     #     "topic": topics[topic_ind]["topic"],
#     #   }
#     # )

#     es_topic = {
#       "id": topics[topic_ind]["_id"],
#       "topic": topics[topic_ind]["topic"],
#     }
#     es.update_article_topic(docs[doc_ind]["_id"], es_topic)
  
#   # insert the topics
#   # mr.store_docs(MONGO_DB_ANALYZER, "topics", topics)
#   es.store_topic_batch(topics)
  

#   # update docs in mongodb

#   # update docs in elasticsearch

#   # TODO: insert into elasticsearch
#   # es.es.index(index="article_topics", )

  

#   # di = bm.bc.bertopic.get_document_info(doc_texts)
#   # di = bt.get_document_info(doc_texts)

#   # print(di)
#   print(topics[0])
#   # print(json.dumps(topics[0], indent=2))
#   print("=======================================")
  
#   import copy
#   cp = copy.deepcopy(docs)
#   for c in cp:
#     del c["analyzer"]["embeddings"]
#     del c["article"]["paragraphs"]

#   # print(json.dumps(cp[0], indent=2))
#   print(cp[0])
#   print("=======================================")

#   exit(0)


if __name__ == '__main__':

  rh.consume_stream(REDIS_STREAM_NAME, REDIS_CONSUMER_GROUP, process_notification)

  RedisNotificationConsumer(
    REDIS_HOST, 
    REDIS_PORT,
    stream_name=REDIS_STREAM_NAME,
    consumer_group=REDIS_CONSUMER_GROUP
  ).consume(process_notification)
