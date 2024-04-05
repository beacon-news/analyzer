import os
from notifications import RedisNotificationConsumer, ScraperDoneNotification
from classifier import CategoryClassifier, ModelContainer
from embeddings import EmbeddingsModelContainer, EmbeddingsModel
from ner import SpacyEntityRecognizer
from utils import log_utils
from repository.analyzer import *
from repository.scraper import *
from datetime import datetime
from domain import Article, AnalyzedArticle

def check_env(name: str, default=None) -> str:
  value = os.environ.get(name, default)
  if value is None:
    raise ValueError(f'{name} environment variable is not set')
  return value


# ML models
CAT_CLF_MODEL_PATH = check_env('CAT_CLF_MODEL_PATH')
EMBEDDINGS_MODEL_PATH = check_env('EMBEDDINGS_MODEL_PATH')
SPACY_MODEL = check_env('SPACY_MODEL')
SPACY_MODEL_DIR = check_env('SPACY_MODEL_DIR')

# Redis
REDIS_HOST = check_env('REDIS_HOST', 'localhost')
REDIS_PORT = int(check_env('REDIS_PORT', 6379))

REDIS_CONSUMER_GROUP = check_env('REDIS_CONSUMER_GROUP', 'article_analyzer')
REDIS_STREAM_NAME = check_env('REDIS_STREAM_NAME', 'scraper_articles')

# Elasticsearch
ELASTIC_USER = check_env('ELASTIC_USER', 'elastic')
ELASTIC_PASSWORD = check_env('ELASTIC_PASSWORD')
ELASTIC_CONN = check_env('ELASTIC_CONN', 'https://localhost:9200')
ELASTIC_CA_PATH = check_env('ELASTIC_CA_PATH', 'certs/_data/ca/ca.crt')
ELASTIC_TLS_INSECURE = bool(check_env('ELASTIC_TLS_INSECURE', False))

# Mongo
MONGO_HOST = check_env('MONGO_HOST', 'localhost')
MONGO_PORT = int(check_env('MONGO_PORT', 27017))
MONGO_DB_SCRAPER = check_env('MONGO_DB_SCRAPER', 'scraper')
MONGO_COLLECTION_SCRAPER = check_env('MONGO_COLLECTION_SCRAPER', 'scraped_articles')

log = log_utils.create_console_logger("Analyzer")
log.info(f"Initializing dependencies")

category_classifier = CategoryClassifier(ModelContainer.load(CAT_CLF_MODEL_PATH))
embeddings_model = EmbeddingsModel(EmbeddingsModelContainer.load(EMBEDDINGS_MODEL_PATH))
named_entity_recognizer = SpacyEntityRecognizer(SPACY_MODEL, SPACY_MODEL_DIR)

repository: AnalyzerRepository = ElasticsearchRepository(
  ELASTIC_CONN, 
  ELASTIC_USER, 
  ELASTIC_PASSWORD, 
  ELASTIC_CA_PATH, 
  not ELASTIC_TLS_INSECURE
)

scraper_repo: ScraperRepository = MongoRepository(
  host=MONGO_HOST,
  port=MONGO_PORT,
  db_name=MONGO_DB_SCRAPER,
  collection_name=MONGO_COLLECTION_SCRAPER,
) 


def process_notification(notifications: list[ScraperDoneNotification]):

  ids = [m.id for m in notifications]
  
  # TODO: use Processes and a process pool executor?

  # get the scraped batch
  docs = scraper_repo.get_article_batch(ids) 
  if len(docs) == 0:
    log.warning(f"no documents found in scraped batch, exiting")
    return
  
  # process the batch
  result_ids = process(docs)

  log.info(f"processed {len(result_ids)} elements: {result_ids}")


def process(docs: list[dict]) -> list[str]:

  articles = []
  prepared_texts = []

  try: 
    for doc in docs:

      article = map_to_article(doc)
      if article is None:
        continue

      articles.append(article)

      # extract text for analysis for each document
      text = '\n'.join(article.title) + '\n'.join(article.paragraphs)
      prepared_texts.append(text)

    # run analysis in batch
    category_labels, embeddings, entities = analyze_batch(prepared_texts)
    
    analyze_time = datetime.now()
    analyzed_articles = [
      AnalyzedArticle(
        article=article,
        analyze_time=analyze_time,
        categories=art_categories,
        embeddings=art_embeddings,
        entities=art_entities
      )
      for article, art_categories, art_embeddings, art_entities in zip(articles, category_labels, embeddings, entities)
    ]

    # store in elasticsearch
    ids = repository.store_analyzed_articles(analyzed_articles)
    log.info(f"done storing batch of {len(analyzed_articles)} articles in Elasticsearch")

    return ids

  except Exception:
    log.exception(f"error trying to analyze doc: {doc}")


def map_to_article(doc: dict) -> Article | None:
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
  return Article(
    id=id,
    url=url,
    publish_date=publish_date,
    author=authors,
    title=titles,
    paragraphs=paras,
  )

def analyze_batch(texts: list[str]) -> list[tuple[list[str], list[float], list[str]]]:
  # classify the text
  labels = category_classifier.predict_batch(texts)

  # create embeddings, take the first one as we only have 1 document
  embeddings = embeddings_model.encode(texts)

  # get named entities
  entities = named_entity_recognizer.ner_batch(texts)

  return (labels, embeddings.tolist(), entities)


if __name__ == '__main__':

  RedisNotificationConsumer(
    REDIS_HOST, 
    REDIS_PORT,
    stream_name=REDIS_STREAM_NAME,
    consumer_group=REDIS_CONSUMER_GROUP
  ).consume_scraper_done(process_notification)
