import os
import json
from redis_handler import RedisHandler
from classifier import CategoryClassifier, ModelContainer
from embeddings import EmbeddingsContainer, EmbeddingsModel
from ner import SpacyEntityRecognizer
from utils import log_utils
from article_store.elasticsearch_store import ElasticsearchStore

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

mc = ModelContainer.load(CAT_CLF_MODEL_PATH)
clf = CategoryClassifier(mc)

ec = EmbeddingsContainer.load(EMBEDDINGS_MODEL_PATH)
em = EmbeddingsModel(ec)

ser = SpacyEntityRecognizer(SPACY_MODEL, SPACY_MODEL_DIR)

rh = RedisHandler(REDIS_HOST, REDIS_PORT)
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

# schema = {
#   "description": "scraped article with metadata",
#   "type": "object",
#   "required": ["url", "scrape_time", "components"],
#   "properties": {
#     "url": {
#       "type": "string"
#     },
#     "scrape_time": {
#       "type": "string"
#     },
#     "components": {
#       "type": "object",
#       "required": ["article"],
#       "properties": {
#         "article": {
#           "type": "array",
#           "items": {
#             "type": "object",

#           }
#         }
#       }
#     }
#   }
# }


def analyze(text) -> tuple[list[str], list[float], list[str]]:
  # classify the text
  labels = clf.predict(text)

  # create embeddings, take the first one as we only have 1 document
  embeddings = em.encode([text])[0]

  # get named entities
  entities = ser.ner(text)

  return (labels, embeddings.tolist(), entities)


def process(message):

  try: 
    content = json.loads(message[1]["article"])

    if 'url' not in content:
      log.error(f"no 'url' in message: {message}, skipping analysis")
      return
    
    url = content['url']

    if 'components' not in content:
      log.error(f"no 'components' in message: {message}, skipping analysis")
      return
    
    comps = content['components']
    if 'article' not in comps:
      log.error(f"'components.article' not found in message: {message}, skipping analysis")
      return
    
    comps = comps['article']
    if not isinstance(comps, list):
      log.error(f"'components.article' is not an array: {message}, skipping analysis")
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
          log.error(f"'components.article.paragraphs' is not an array: {message}, skipping analysis")
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

    # run analysis on text block
    text = '\n'.join(titles) + '\n' + '\n'.join(paras)
    labels, embeddings, entities = analyze(text)
    analysis = {
      "categories": labels,
      "entities": entities,
      "embeddings": embeddings,
    }

    # store in elasticsearch
    es.store(article, analysis)

    # result = {
    #   "analyzer": {
    #     "categories": labels,
    #     "entities": entities,
    #     "embeddings": embeddings.tolist(),
    #   },
    #   "scraper": content,
    # }

    # only print some embeddings
    p_a = analysis.copy()
    p_a['embeddings'] = p_a['embeddings'][:4]

    print("=================================")
    print(f"result: {json.dumps(article, indent=2)}")
    print("********************************")
    print(f"result: {json.dumps(p_a, indent=2)}")
    print("=================================")
    print("")

  except Exception:
    log.exception(f"error trying to classify message: {message}")


if __name__ == '__main__':

  rh.consume_stream(REDIS_STREAM_NAME, REDIS_CONSUMER_GROUP, process)

