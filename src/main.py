import os
import uuid
import json
from redis_handler import RedisHandler
from mongodb_repository import MongoRepository
from classifier import CategoryClassifier, ModelContainer
from embeddings import EmbeddingsModelContainer, EmbeddingsModel
from ner import SpacyEntityRecognizer
from utils import log_utils
from article_store.elasticsearch_store import ElasticsearchStore
from datetime import datetime

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
REDIS_STREAM_NAME = check_env('REDIS_STREAM_NAME', 'scraped_articles')

ELASTIC_USER = check_env('ELASTIC_USER', 'elastic')
ELASTIC_PASSWORD = check_env('ELASTIC_PASSWORD')
ELASTIC_CONN = check_env('ELASTIC_HOST', 'https://localhost:9200')
ELASTIC_CA_PATH = check_env('ELASTIC_CA_PATH', 'certs/_data/ca/ca.crt')
ELASTIC_TLS_INSECURE = bool(check_env('ELASTIC_TLS_INSECURE', False))


MONGO_HOST = check_env('MONGO_HOST', 'localhost')
MONGO_PORT = int(check_env('MONGO_PORT', 27017))
MONGO_DB_SCRAPER = check_env('MONGO_DB_SCRAPER', 'scraper')
MONGO_DB_ANALYZER = check_env('MONGO_DB_ANALYZER', 'analyzer')
MONGO_COLLECTION_SCRAPER = check_env('MONGO_COLLECTION_SCRAPER', 'scraped_articles')
MONGO_COLLECTION_ANALYZER = check_env('MONGO_COLLECTION_ANALYZER', 'scraped_articles')
mr = MongoRepository(host=MONGO_HOST, port=MONGO_PORT)

mc = ModelContainer.load(CAT_CLF_MODEL_PATH)
clf = CategoryClassifier(mc)

ec = EmbeddingsModelContainer.load(EMBEDDINGS_MODEL_PATH)
em = EmbeddingsModel(ec)

ser = SpacyEntityRecognizer(SPACY_MODEL, SPACY_MODEL_DIR)

# es = ElasticsearchStore(
#   ELASTIC_CONN, 
#   ELASTIC_USER, 
#   ELASTIC_PASSWORD, 
#   ELASTIC_CA_PATH, 
#   not ELASTIC_TLS_INSECURE
# )


def print_message(message):
  print(f"received from stream: {message}")


log = log_utils.create_console_logger("MessageProcessor")


def process(docs: list[dict]):

  transformed_docs = []
  prepared_texts = []

  try: 
    for doc in docs:

      if 'url' not in doc:
        log.error(f"no 'url' in doc: {doc}, skipping analysis")
        return
      
      url = doc['url']
      if 'components' not in doc:
        log.error(f"no 'components' in doc: {doc}, skipping analysis")
        return
      
      comps = doc['components']
      if 'article' not in comps:
        log.error(f"'components.article' not found in doc: {doc}, skipping analysis")
        return
      
      comps = comps['article']
      if not isinstance(comps, list):
        log.error(f"'components.article' is not an array: {doc}, skipping analysis")
        return
      
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
            return
          paras.extend(component['paragraphs'])
        elif 'author' in component:
          authors.append(component['author'])
        elif 'publish_date' in component:
          publish_date = component['publish_date']
          publish_date = datetime.fromisoformat(publish_date).replace(second=0, microsecond=0)

      # transform the scraped format into a more manageable one
      article = {
        "url": url,
        "publish_date": publish_date,
        "author": authors,
        "title": titles,
        "paragraphs": paras,
      }
      transformed_docs.append(article)

      # extract text for analysis for each document
      text = '\n'.join(titles) + '\n'.join(paras)
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
        "analyzer": {
          "categories": art_labels,
          "entities": art_entities,
          "embeddings": art_embeddings,
        },
        "article": {
          "url": article['url'],
          "publish_date": article['publish_date'],
          "author": article['author'],
          "title": article['title'],
          "paragraphs": article['paragraphs'],
        }
      } for article, art_labels, art_embeddings, art_entities in zip(transformed_docs, labels, embeddings, entities)
    ]

    # replace publish_date with iso time string
    es_docs = []
    for doc in final_docs:
      doc['article']['publish_date'] = doc['article']['publish_date'].isoformat()
      es_docs.append(doc)
    
    # store in elasticsearch
    # es.store_batch(es_docs)

    # store in mongodb
    mr.store_articles(MONGO_DB_ANALYZER, MONGO_COLLECTION_ANALYZER, final_docs)


    # only print some embeddings
    # p_a = analysis.copy()
    # p_a['embeddings'] = p_a['embeddings'][:4]

    # print("=================================")
    # print(f"result: {json.dumps(transformed_docs, indent=2)}")
    # print("********************************")
    # print(f"result: {json.dumps(analyzed_docs, indent=2)}")
    # print("=================================")
    # print("")
    
    # p_a = analysis.copy()
    # p_a['embeddings'] = p_a['embeddings'][:4]

    # print("=================================")
    # print(f"result: {json.dumps(article, indent=2)}")
    # print("********************************")
    # print(f"result: {json.dumps(p_a, indent=2)}")
    # print("=================================")
    # print("")

  except Exception:
    log.exception(f"error trying to analyze doc: {doc}")

def analyze_batch(texts: list[str]) -> list[tuple[list[str], list[float], list[str]]]:
  # classify the text
  labels = clf.predict_batch(texts)

  # create embeddings, take the first one as we only have 1 document
  embeddings = em.encode(texts)

  # get named entities
  entities = ser.ner_batch(texts)

  return (labels, embeddings.tolist(), entities)

def analyze(text) -> tuple[list[str], list[float], list[str]]:
  # classify the text
  labels = clf.predict(text)

  # create embeddings, take the first one as we only have 1 document
  embeddings = em.encode([text])[0]

  # get named entities
  entities = ser.ner(text)

  return (labels, embeddings.tolist(), entities)


# def process(message):

#   try: 
#     content = json.loads(message[1]["article"])

#     if 'url' not in content:
#       log.error(f"no 'url' in message: {message}, skipping analysis")
#       return
    
#     url = content['url']

#     if 'components' not in content:
#       log.error(f"no 'components' in message: {message}, skipping analysis")
#       return
    
#     comps = content['components']
#     if 'article' not in comps:
#       log.error(f"'components.article' not found in message: {message}, skipping analysis")
#       return
    
#     comps = comps['article']
#     if not isinstance(comps, list):
#       log.error(f"'components.article' is not an array: {message}, skipping analysis")
#       return
    
#     # should only contain 1 title, and 1 paragraphs section, but just in case
#     titles = [] 
#     paras = [] 
#     authors = []
#     publish_date = None
#     for component in comps:
#       if 'title' in component:
#         titles.append(component['title'])
#       elif 'paragraphs' in component:
#         if not isinstance(component['paragraphs'], list):
#           log.error(f"'components.article.paragraphs' is not an array: {message}, skipping analysis")
#           return
#         paras.extend(component['paragraphs'])
#       elif 'author' in component:
#         authors.append(component['author'])
#       elif 'publish_date' in component:
#         publish_date = component['publish_date']

#     # transform the scraped format into a more manageable one
#     article = {
#       "url": url,
#       "publish_date": publish_date,
#       "author": authors,
#       "title": titles,
#       "paragraphs": paras,
#     }

#     # run analysis on text block
#     text = '\n'.join(titles) + '\n' + '\n'.join(paras)
#     labels, embeddings, entities = analyze(text)
#     analysis = {
#       "categories": labels,
#       "entities": entities,
#       "embeddings": embeddings,
#     }

#     # store in elasticsearch
#     es.store(article, analysis)

#     # only print some embeddings
#     p_a = analysis.copy()
#     p_a['embeddings'] = p_a['embeddings'][:4]

#     print("=================================")
#     print(f"result: {json.dumps(article, indent=2)}")
#     print("********************************")
#     print(f"result: {json.dumps(p_a, indent=2)}")
#     print("=================================")
#     print("")

#   except Exception:
#     log.exception(f"error trying to analyze message: {message}")


from topic_modeling.bertopic_container import *
from topic_modeling.bertopic_model import *
import numpy as np

# BERTOPIC_MODEL_PATH = check_env("BERTOPIC_MODEL_PATH")
# bc = BertopicContainer.load(BERTOPIC_MODEL_PATH, em.ec.embeddings_model)
# bm = BertopicModel(bc)

log.info("importing BERTopic and its dependencies")
from bertopic import BERTopic

from umap import UMAP
from hdbscan import HDBSCAN
from sklearn.feature_extraction.text import CountVectorizer
from bertopic.representation import PartOfSpeech, MaximalMarginalRelevance

log.info("initializing BERTopic dependencies")

umap_model = UMAP(
    n_neighbors=15, # global / local view of the manifold default 15
    n_components=5, # target dimensions default 5
    metric='cosine',
    min_dist=0.0 # smaller --> more clumped embeddings, larger --> more evenly dispersed default 0.0
)

hdbscan_model = HDBSCAN(
    min_cluster_size=2, # nr. of points required for a cluster (documents for a topic) default 10
    metric='euclidean',
    cluster_selection_method='eom',
    prediction_data=True, # if we want to approximate clusters for new points
)

vectorizer_model = CountVectorizer(
    ngram_range=(1, 1),
    stop_words='english',
)


# ps = PartOfSpeech("en_core_web_sm")
mmr = MaximalMarginalRelevance(diversity=0.3)

# representation_model = [ps, mmr]
representation_model = mmr

bt = BERTopic(
    embedding_model=em.ec.embeddings_model,
    umap_model=umap_model,
    hdbscan_model=hdbscan_model,
    vectorizer_model=vectorizer_model,
    representation_model=representation_model,
    # verbose=True
)

import pandas as pd

pd.set_option('display.max_columns', None)

def model_topics(docs):

  doc_texts = ["\n".join(d["article"]["title"]) + "\n" + "\n".join(d["article"]["paragraphs"]) for d in docs]
  doc_embeddings = np.array([d["analyzer"]["embeddings"] for d in docs])

  log.info(f"fitting topic modeling model on {len(doc_texts)} docs")

  # bm.bc.bertopic.fit_transform(doc_texts, doc_embeddings)
  bt.fit_transform(doc_texts, doc_embeddings)

  # ti = bm.bc.bertopic.get_topic_info()
  ti = bt.get_topic_info()
  print(ti)
  print("=============================")


  # 1. create the topics

  d = {
    "_id": ...,
    "topic": "hungary nato orban",
    "start_date": ...,
    "end_date": ...,
    "count": 103,
    "repr_docs": [
      {
        "_id":...,
        "url": ...,
        "publish_date": ...,
        "author": ...,
        "title": ...,
      },
      {
        "_id":...,
        "url": ...,
        "publish_date": ...,
        "author": ...,
        "title": ...,
      },
      {
        "_id":...,
        "url": ...,
        "publish_date": ...,
        "author": ...,
        "title": ...,
      },
    ],
  }

  # create the topics without docs
  topics = []
  topic_info = bt.get_topic_info()

  start_time = datetime.now().isoformat()
  end_time = datetime.now().isoformat()

  topic_df_dict = topic_info.loc[topic_info["Topic"] != -1, ["Count", "Representation"]].to_dict()
  for d in zip(topic_df_dict["Count"].values(), topic_df_dict["Representation"].values()):
    topics.append({
      "_id": uuid.uuid4(), 
      "start_time": start_time,
      "end_time": end_time,
      "topic": " ".join(d[1]),
      "count": d[0],
      "representative_articles": [],
      "articles": [],
    })

  # add docs to topics
  doc_info = bt.get_document_info(doc_texts)

  # don't filter out anything, we need the correct order to correlate with 'docs'
  doc_df_dict = doc_info.loc[doc_info["Topic"] != -1, ["Topic", "Representative_document"]].to_dict()
  for doc_ind, topic_ind, representative in zip(
    doc_df_dict["Topic"].keys(), 
    doc_df_dict["Topic"].values(), 
    doc_df_dict["Representative_document"].values()
  ):

    # add duplicate of article to topic
    art = docs[doc_ind]["article"]

    art_duplicate = {
      "_id": docs[doc_ind]["_id"],
      "url": art["url"],
      "publish_date": art["publish_date"],
      "author": art["author"],
      "title": art["title"],
    }
    
    topics[topic_ind]["articles"].append(art_duplicate)
    
    # add representative doc
    if representative:
      topics[topic_ind]["representative_articles"].append(docs[doc_ind]["_id"])

    # TODO: instead of updating articles, add doc ids to topics

    # update the article with duplicate of topic
    if "topics" not in docs[doc_ind]["analyzer"]: 
      docs[doc_ind]["analyzer"]["topics"] = []
    
    docs[doc_ind]["analyzer"]["topics"].append(
      {
        "_id": topics[topic_ind]["_id"],
        "topic": topics[topic_ind]["topic"],
      }
    )
  
  # insert the topics
  mr.store_docs(MONGO_DB_ANALYZER, "topics", topics)

  # TODO: insert into elasticsearch

  

  # di = bm.bc.bertopic.get_document_info(doc_texts)
  # di = bt.get_document_info(doc_texts)

  # print(di)
  print(topics[0])
  # print(json.dumps(topics[0], indent=2))
  print("=======================================")
  
  import copy
  cp = copy.deepcopy(docs)
  for c in cp:
    del c["analyzer"]["embeddings"]
    del c["article"]["paragraphs"]

  # print(json.dumps(cp[0], indent=2))
  print(cp[0])
  print("=======================================")

  exit(0)


from multiprocessing import Process

if __name__ == '__main__':
  # rh.consume_stream(REDIS_STREAM_NAME, REDIS_CONSUMER_GROUP, process)

  # TODO: use multiprocessing

  # mr.watch_collection(
  #   MONGO_DB_SCRAPER, 
  #   MONGO_COLLECTION_SCRAPER, 
  #   1000, 
  #   5, 
  #   processed_update_field="analyzer.processed", 
  #   callback=process
  # )


  # mr.watch_collection(
  #   MONGO_DB_ANALYZER, 
  #   MONGO_COLLECTION_ANALYZER, 
  #   3000, 
  #   200, 
  #   processed_update_field="topic_modeling.processed",
  #   callback=model_topics
  # )

  p2 = Process(target=mr.watch_collection, args=(
    MONGO_DB_ANALYZER, 
    MONGO_COLLECTION_ANALYZER, 
    3000, 
    200, 
    "topic_modeling.processed",
    model_topics
  ))

  p2.start()

  p2.join()