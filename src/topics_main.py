from utils import log_utils
from article_store.elasticsearch_store import ElasticsearchStore
import os
import hashlib
import schedule

def check_env(name: str, default=None) -> str:
  value = os.environ.get(name, default)
  if value is None:
    raise ValueError(f'{name} environment variable is not set')
  return value

EMBEDDINGS_MODEL_PATH = check_env('EMBEDDINGS_MODEL_PATH')
ELASTIC_USER = check_env('ELASTIC_USER', 'elastic')
ELASTIC_PASSWORD = check_env('ELASTIC_PASSWORD')
ELASTIC_CONN = check_env('ELASTIC_HOST', 'https://localhost:9200')
ELASTIC_CA_PATH = check_env('ELASTIC_CA_PATH', 'certs/_data/ca/ca.crt')
ELASTIC_TLS_INSECURE = bool(check_env('ELASTIC_TLS_INSECURE', False))

log = log_utils.create_console_logger("TopicModeler")

es = ElasticsearchStore(
  ELASTIC_CONN, 
  ELASTIC_USER, 
  ELASTIC_PASSWORD, 
  ELASTIC_CA_PATH, 
  not ELASTIC_TLS_INSECURE
)

log.info("importing BERTopic and its dependencies")

from embeddings.embeddings_container import EmbeddingsModelContainer
from embeddings.embeddings_model import EmbeddingsModel
from datetime import datetime
import uuid
import numpy as np
from bertopic import BERTopic
from umap import UMAP
from hdbscan import HDBSCAN
from sklearn.feature_extraction.text import CountVectorizer
from bertopic.representation import PartOfSpeech, MaximalMarginalRelevance
import pandas as pd

log.info("initializing BERTopic dependencies")

em = EmbeddingsModel(EmbeddingsModelContainer.load(EMBEDDINGS_MODEL_PATH))

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
    topic_name = " ".join(d[1])
    topic_id = hashlib.sha1(f"{topic_name}-{start_time}-{end_time}".encode()).hexdigest()
    topics.append({
      "_id": topic_id, 
      "start_time": start_time,
      "end_time": end_time,
      "topic": topic_name,
      "count": d[0],
      "representative_articles": [],
    })
  
  if len(topics) == 0:
    log.info(f"only outliers found ({len(topics)}), returning")
    return
  
  log.info(f"found {len(topics)} topics, adding representative docs, updating docs with topics")

  # add docs to topics
  doc_info = bt.get_document_info(doc_texts)

  # don't filter out anything, we need the correct order to correlate with 'docs'
  doc_df_dict = doc_info.loc[doc_info["Topic"] != -1, ["Topic", "Representative_document"]].to_dict()
  for doc_ind, topic_ind, representative in zip(
    doc_df_dict["Topic"].keys(), 
    doc_df_dict["Topic"].values(), 
    doc_df_dict["Representative_document"].values()
  ):
    
    # add representative doc
    if representative:

      # add duplicate of article to topic
      art = docs[doc_ind]["article"]
      art_duplicate = {
        "_id": docs[doc_ind]["_id"],
        "url": art["url"],
        "publish_date": art["publish_date"],
        "author": art["author"],
        "title": art["title"],
      }
      topics[topic_ind]["representative_articles"].append(art_duplicate)

    # update the article with duplicate of topic
    # TODO: upsert with array item
    # if "topics" not in docs[doc_ind]["analyzer"]: 
    #   docs[doc_ind]["analyzer"]["topics"] = []
    
    # docs[doc_ind]["analyzer"]["topics"].append(
    #   {
    #     "_id": topics[topic_ind]["_id"],
    #     "topic": topics[topic_ind]["topic"],
    #   }
    # )

    es_topic = {
      "id": topics[topic_ind]["_id"],
      "topic": topics[topic_ind]["topic"],
    }
    es.update_article_topic(docs[doc_ind]["_id"], es_topic)
  
  # insert the topics
  # mr.store_docs(MONGO_DB_ANALYZER, "topics", topics)
  ids = es.store_topic_batch(topics)

  log.info(f"stored {len(ids)} topics")
  
  print(topics[0])
  print("=======================================")
  
  import copy
  cp = copy.deepcopy(docs)
  for c in cp:
    del c["analyzer"]["embeddings"]
    del c["article"]["paragraphs"]

  print(cp[0])
  print("=======================================")



if __name__ == '__main__':

  # script creates crontab entries for itself?

  # runs a loop in the background and starts new processes?

  # minute hour day(of month) month day(day of week)

  import sys

  cron_str = sys.argv[1]

  elems = cron_str.split(' ')
  if len(elems) != 5:
    raise ValueError(f"Invalid cron string {cron_str}, it has {len(elems)} values")


  print(elems)
  
  exit(0)

  # schedule.every()


  # parse config (cron string + query)
  date_min = datetime.fromtimestamp(0).isoformat()
  date_max = datetime.now().isoformat()

  q = {
    "publish_date": {
      "from": date_min,
      "to": date_max,
    },
  }

  # transform query in config to db query
  query = {
    "bool": {
      "filter": {
        "range": {
          "article.publish_date": {
            "gte": q["publish_date"]["from"],
            "lte": q["publish_date"]["to"],
          }
        }
      }
    }
  }

  log.info(f"running topic modeling with config {q}")

  # query the db, only what is needed
  docs = es.es.search(
    index="articles",
    query=query,
  )

  # transform 
  dt = [{
    "_id": d["_id"],
    "analyzer": {
      "embeddings": d["_source"]["analyzer"]["embeddings"],
    },
    "article": {
      **d["_source"]["article"],
    }
  } for d in docs["hits"]["hits"]]

  # run topic modeling, insert topics into db, update documents with topics
  model_topics(dt)