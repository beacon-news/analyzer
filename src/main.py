import os
import json
from redis_handler import RedisHandler
from classifier import CategoryClassifier, ModelContainer
from embeddings import EmbeddingsContainer, EmbeddingsModel
from ner import SpacyEntityRecognizer
from utils import log_utils


CAT_CLF_MODEL_PATH = os.environ.get('CAT_CLF_MODEL_PATH')
if CAT_CLF_MODEL_PATH is None:
  raise ValueError('CAT_CLF_MODEL_PATH environment variable is not set')

EMBEDDINGS_MODEL_PATH = os.environ.get('EMBEDDINGS_MODEL_PATH')
if EMBEDDINGS_MODEL_PATH is None:
  raise ValueError('EMBEDDINGS_MODEL_PATH environment variable is not set')

SPACY_MODEL = os.environ.get('SPACY_MODEL')
if SPACY_MODEL is None:
  raise ValueError('SPACY_MODEL environment variable is not set')

SPACY_MODEL_DIR = os.environ.get('SPACY_MODEL_DIR')
if SPACY_MODEL_DIR is None:
  raise ValueError('SPACY_MODEL_DIR environment variable is not set')


REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

REDIS_CONSUMER_GROUP = os.getenv('REDIS_CONSUMER_GROUP', 'article_analyzer')
REDIS_STREAM_NAME = os.getenv('REDIS_STREAM_NAME', 'scraped_articles')


mc = ModelContainer.load(CAT_CLF_MODEL_PATH)
clf = CategoryClassifier(mc)

ec = EmbeddingsContainer.load(EMBEDDINGS_MODEL_PATH)
em = EmbeddingsModel(ec)

ser = SpacyEntityRecognizer(SPACY_MODEL, SPACY_MODEL_DIR)

rh = RedisHandler(REDIS_HOST, REDIS_PORT)
es = ElasticSearchStore()


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



def analyze(message):

  try: 
    content = json.loads(message[1]["article"])

    if 'components' not in content:
      log.error(f"no 'components' in message: {message}, skipping analysis")
      return
    
    p = content['components']
    if 'article' not in p:
      log.error(f"'components.article' not found in message: {message}, skipping analysis")
      return
    
    p = p['article']
    if not isinstance(p, list):
      log.error(f"'components.article' is not an array: {message}, skipping analysis")
      return
    
    # should only contain 1 title, and 1 paragraphs section, but just in case
    titles = ''
    paras = ''
    for component in p:
      if 'title' in component:
        titles += component['title'] + '\n'
      elif 'paragraphs' in component:
        if not isinstance(component['paragraphs'], list):
          log.error(f"'components.article.paragraphs' is not an array: {message}, skipping analysis")
          return
        paras = '\n'.join(component['paragraphs'])
    
    text = titles + '\n' + paras


    # classify the text
    labels = clf.predict(text)

    # create embeddings
    embeddings = em.encode([text])

    # get named entities
    entities = ser.ner(text)

    result = {
      "analyzer": {
        "categories": labels,
        "entities": entities,
        "embeddings": embeddings.tolist(),
      },
      "scraper": content,
    }

    print("=================================")
    print(f"result: {json.dumps(result, indent=2)}")
    # print(f"classification results: {labels}")
    # print(f"entities: {entities}")
    # print(f"embeddings shape: {embeddings.shape}")
    print("")

    # store in elasticsearch
  
  except Exception:
    log.exception(f"error trying to classify message: {message}")
    

if __name__ == '__main__':

  rh.consume_stream(REDIS_STREAM_NAME, REDIS_CONSUMER_GROUP, analyze)

