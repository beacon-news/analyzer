import os
import json
from redis_handler import RedisHandler
from mongodb_repository import MongoRepository
from classifier import CategoryClassifier, ModelContainer
from embeddings import EmbeddingsModelContainer, EmbeddingsModel
from ner import SpacyEntityRecognizer
from utils import log_utils
from article_store.elasticsearch_store import ElasticsearchStore
from pydantic import BaseModel

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
MONGO_DB = check_env('MONGO_DB_NAME', 'scraper')
MONGO_COLLECTION = check_env('MONGO_COLLECTION_NAME', 'scraped_articles')
mr = MongoRepository(host=MONGO_HOST, port=MONGO_PORT)

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
    
    # store in elasticsearch
    es.store_batch(transformed_docs, analyzed_docs)

    # only print some embeddings
    # p_a = analysis.copy()
    # p_a['embeddings'] = p_a['embeddings'][:4]

    print("=================================")
    print(f"result: {json.dumps(transformed_docs, indent=2)}")
    print("********************************")
    print(f"result: {json.dumps(analyzed_docs, indent=2)}")
    print("=================================")
    print("")
    
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


if __name__ == '__main__':
  # rh.consume_stream(REDIS_STREAM_NAME, REDIS_CONSUMER_GROUP, process)


  mr.watch_collection(MONGO_DB, MONGO_COLLECTION, 1000, 5, process)