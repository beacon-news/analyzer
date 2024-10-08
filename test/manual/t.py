from src.api.notifications import RedisScraperEventConsumer, ScraperDoneNotification
from src.analysis.classifier import CategoryClassifier, ModelContainer
from src.analysis.embeddings import EmbeddingsModelContainer, EmbeddingsModel
from src.analysis.ner import SpacyEntityRecognizer
from src.domain import *
from src.utils import log_utils
from src.repository.analyzer import *
from src.repository.scraper import *
from datetime import datetime
import os
import hashlib

def check_env(name: str, default=None) -> str:
  value = os.environ.get(name, default)
  if value is None:
    raise ValueError(f'{name} environment variable is not set')
  return value


# # ML models
# CAT_CLF_MODEL_PATH = check_env('CAT_CLF_MODEL_PATH')
# EMBEDDINGS_MODEL_PATH = check_env('EMBEDDINGS_MODEL_PATH')
# SPACY_MODEL = check_env('SPACY_MODEL')
# SPACY_MODEL_DIR = check_env('SPACY_MODEL_DIR')

# Redis
REDIS_HOST = check_env('REDIS_HOST', 'localhost')
REDIS_PORT = int(check_env('REDIS_PORT', 6379))

REDIS_CONSUMER_GROUP = check_env('REDIS_CONSUMER_GROUP', 'article_analyzer')
REDIS_STREAM_NAME = check_env('REDIS_STREAM_NAME', 'scraper_articles')

# # Elasticsearch
# ELASTIC_USER = check_env('ELASTIC_USER', 'elastic')
# ELASTIC_PASSWORD = check_env('ELASTIC_PASSWORD')
# ELASTIC_CONN = check_env('ELASTIC_CONN', 'https://localhost:9200')
# ELASTIC_CA_PATH = check_env('ELASTIC_CA_PATH', 'certs/_data/ca/ca.crt')
# ELASTIC_TLS_INSECURE = bool(check_env('ELASTIC_TLS_INSECURE', False))

# # Mongo
# MONGO_HOST = check_env('MONGO_HOST', 'localhost')
# MONGO_PORT = int(check_env('MONGO_PORT', 27017))
# MONGO_DB_SCRAPER = check_env('MONGO_DB_SCRAPER', 'scraper')
# MONGO_COLLECTION_SCRAPER = check_env('MONGO_COLLECTION_SCRAPER', 'scraped_articles')

# log = log_utils.create_console_logger("Analyzer")
# log.info(f"Initializing dependencies")

# category_classifier = CategoryClassifier(ModelContainer.load(CAT_CLF_MODEL_PATH))
# embeddings_model = EmbeddingsModel(EmbeddingsModelContainer.load(EMBEDDINGS_MODEL_PATH))
# named_entity_recognizer = SpacyEntityRecognizer(SPACY_MODEL, SPACY_MODEL_DIR)

# repository: AnalyzerRepository = ElasticsearchRepository(
#   ELASTIC_CONN, 
#   ELASTIC_USER, 
#   ELASTIC_PASSWORD, 
#   ELASTIC_CA_PATH, 
#   not ELASTIC_TLS_INSECURE
# )

# scraper_repo: ScraperRepository = MongoRepository(
#   host=MONGO_HOST,
#   port=MONGO_PORT,
#   db_name=MONGO_DB_SCRAPER,
#   collection_name=MONGO_COLLECTION_SCRAPER,
# ) 

# # TODO: separate this processing stuff into a proper business layer

# def process_notification(notifications: list[ScraperDoneNotification]):

#   ids = [m.id for m in notifications]
  
#   # TODO: use Processes and a process pool executor?

#   # get the scraped batch
#   docs = scraper_repo.get_article_batch(ids) 
#   if len(docs) == 0:
#     log.warning(f"no documents found in scraped batch, exiting")
#     return
  
#   # process the batch
#   result_ids = process(docs)

#   log.info(f"processed {len(result_ids)} elements: {result_ids}")


# def process(docs: list[dict]) -> list[str]:

#   scraped_articles = []
#   prepared_texts = []

#   try: 
#     for doc in docs:

#       scraped_article = map_to_article(doc)
#       if scraped_article is None:
#         continue
    
#       scraped_articles.append(scraped_article)

#       # extract text for analysis for each document
#       text = '\n'.join(scraped_article.title) + '\n'.join(scraped_article.paragraphs)
#       prepared_texts.append(text)
    
#     if len(prepared_texts) == 0:
#       log.warning(f"no text found in documents in scraped batch, skipping batch")
#       return []

#     # run analysis in batch
#     category_labels, embeddings, entities = analyze_batch(prepared_texts)

#     # gather the categories and articles
#     articles = []
#     unique_category_names = set()
#     categories = {}
#     analyze_time = datetime.now()
#     for scr_art, art_categories, art_embeddings, art_entities in zip(scraped_articles, category_labels, embeddings, entities):
      
#       # gather the categories
#       meta_categories = [cat.strip().lower() for cat in scr_art.metadata.categories]
#       meta_and_predicted_cat = set([*art_categories, *meta_categories])

#       # add missing category objects
#       for cat_name in meta_and_predicted_cat.difference(unique_category_names):
#         categories[cat_name] = Category(
#           id=hashlib.sha1(cat_name.encode()).hexdigest(), 
#           name=cat_name
#         )
#       unique_category_names = unique_category_names.union(meta_and_predicted_cat)

#       # create Category objects
#       meta_and_predicted_cat = [categories[name] for name in meta_and_predicted_cat]
#       predicted_categories = [categories[name] for name in art_categories]

#       articles.append(
#         Article(
#           id=scr_art.id,
#           url=scr_art.url,
#           source=scr_art.metadata.source,
#           publish_date=scr_art.publish_date,
#           image=scr_art.image,
#           author=scr_art.author,
#           title=scr_art.title,
#           paragraphs=scr_art.paragraphs,
#           categories=meta_and_predicted_cat,
#           analyze_time=analyze_time,
#           analyzed_categories=predicted_categories,
#           embeddings=art_embeddings,
#           entities=art_entities,
#           topics=None
#         )
#       )

#     # create the categories if they don't exist
#     cat_ids = repository.store_categories(list(categories.values()))
#     log.info(f"stored {len(cat_ids)} categories")

#     # create the articles
#     ids = repository.store_analyzed_articles(articles)
#     log.info(f"done storing batch of {len(articles)} articles")

#     return ids

#   except Exception:
#     log.exception(f"error trying to analyze doc: {doc}")


# def map_to_article(doc: dict) -> ScrapedArticle | None:
#   if 'id' not in doc:
#     log.error(f"no 'id' in doc: {doc}, skipping analysis")
#     return None
#   id = doc['id']

#   if 'url' not in doc:
#     log.error(f"no 'url' in doc: {doc}, skipping analysis")
#     return None
#   url = doc['url']

#   # metadata is optional, can be None
#   art_meta = ScrapedArticleMetadata()
#   metadata = doc.get('metadata', None)
#   if metadata:
#     art_meta.source = metadata.get('source', None)
#     art_meta.categories = metadata.get('categories', [])

#   if 'components' not in doc:
#     log.error(f"no 'components' in doc: {doc}, skipping analysis")
#     return None
  
#   comps = doc['components']
#   if 'article' not in comps:
#     log.error(f"'components.article' not found in doc: {doc}, skipping analysis")
#     return None
  
#   comps = comps['article']
#   if not isinstance(comps, list):
#     log.error(f"'components.article' is not an array: {doc}, skipping analysis")
#     return None
  
#   # should only contain 1 title, and 1 paragraphs section, but just in case
#   titles = [] 
#   paras = [] 
#   authors = []
#   publish_date = None
#   image = None
#   for component in comps:
#     if 'title' in component:
#       titles.append(component['title'])
#     elif 'paragraphs' in component:
#       if not isinstance(component['paragraphs'], list):
#         log.error(f"'components.article.paragraphs' is not an array: {doc}, skipping analysis")
#         return None
#       paras.extend(component['paragraphs'])
#     elif 'author' in component:
#       if type(component['author']) == list:
#         authors.extend(component['author'])
#       else:
#         authors.append(component['author'])
#     elif 'publish_date' in component:
#       publish_date = component['publish_date']
#       publish_date = datetime.fromisoformat(publish_date).replace(second=0, microsecond=0)
#     elif 'image' in component:
#       image = component['image']
  
#   # verify essential attributes
#   if len(titles) == 0 or len(paras) == 0 or publish_date is None:
#     log.error(f"one of 'title', 'paragraphs', 'publish_date' not found in doc: {doc}, skipping analysis")
#     return None

#   # transform the scraped format into a more manageable one
#   return ScrapedArticle(
#     id=id,
#     url=url,
#     metadata=art_meta,
#     publish_date=publish_date,
#     image=image,
#     author=authors,
#     title=titles,
#     paragraphs=paras,
#   )

# def analyze_batch(texts: list[str]) -> list[tuple[list[str], list[float], list[str]]]:
#   # classify the text
#   labels = category_classifier.predict_batch(texts)

#   # create embeddings, take the first one as we only have 1 document
#   embeddings = embeddings_model.encode(texts)

#   # get named entities
#   entities = named_entity_recognizer.ner_batch(texts)

#   return (labels, embeddings.tolist(), entities)


if __name__ == '__main__':

  # RedisScraperEventConsumer(
  #   REDIS_HOST, 
  #   REDIS_PORT,
  #   stream_name=REDIS_STREAM_NAME,
  #   consumer_group=REDIS_CONSUMER_GROUP
  # ).consume_done_notification(process_notification)

  def print_arts(arts: list[dict]):
    print("=================")
    print("printing articles")
    print("received ", len(arts))
    for art in arts:
      print(art)
    print("=================")

  import api.scraped_articles.redis_article_consumer as rc
  import api.scraped_articles.article_batcher as ab

  r = rc.RedisScrapedArticleConsumer(
    REDIS_HOST,
    REDIS_PORT,
    stream_name="test",
    consumer_group="test",
  )

  b = ab.ArticleBatcher(
    r,
    max_batch_size=3,
    max_batch_timeout_millis=3000,
  )

  b.consume_batched_articles(print_arts)


  
