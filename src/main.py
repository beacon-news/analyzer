import os
import json
from redis_handler import RedisHandler
from category_classifier import CategoryClassifier
from utils import log_utils


MODEL_PATH = os.environ.get('MODEL_PATH')
if MODEL_PATH is None:
  raise ValueError('MODEL_PATH environment variable is not set')

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

REDIS_CONSUMER_GROUP = os.getenv('REDIS_CONSUMER_GROUP', 'article_analyzer')
REDIS_STREAM_NAME = os.getenv('REDIS_STREAM_NAME', 'scraped_articles')


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

def classify(message, clf: CategoryClassifier):

  try: 
    url = message[1]["url"] 
    content = json.loads(message[1]["article"])

    if 'components' not in content:
      log.error(f"no 'components' in message: {message}, skipping classification")
      return
    
    p = content['components']
    if 'article' not in p:
      log.error(f"'components.article' not found in message: {message}, skipping classification")
      return
    
    p = p['article']
    if not isinstance(p, list):
      log.error(f"'components.article' is not an array: {message}, skipping classification")
      return
    
    # should only contain 1 title, and 1 paragraphs section, but just in case
    titles = ''
    paras = ''
    for component in p:
      if 'title' in component:
        titles += component['title'] + '\n'
      elif 'paragraphs' in component:
        if not isinstance(component['paragraphs'], list):
          log.error(f"'components.article.paragraphs' is not an array: {message}, skipping classification")
          return
        paras = '\n'.join(component['paragraphs'])
    
    text = titles + '\n' + paras

    # classify the text
    labels = clf.predict(text)
    result = {
      "analyzer": {
        "categories": labels,
      },
      "scraper": content,
    }

    print("=================================")
    print(f"result: {json.dumps(result, indent=2)}")
    print("")
  
  except Exception:
    log.exception(f"error trying to classify message: {message}")
    

if __name__ == '__main__':

  clf = CategoryClassifier.load(MODEL_PATH)

  rh = RedisHandler(REDIS_HOST, REDIS_PORT)
  rh.consume_stream(REDIS_STREAM_NAME, REDIS_CONSUMER_GROUP, classify, clf)

