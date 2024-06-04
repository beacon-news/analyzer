from analysis.classifier import CategoryClassifier, ModelContainer
from analysis.embeddings import EmbeddingsModelContainer, EmbeddingsModel
from analysis.analyzer import Analyzer

from api.scraped_articles.redis_article_consumer import RedisScrapedArticleConsumer
from api.scraped_articles.article_batcher import ArticleBatcher
from api.redis_handler import RedisHandler

from domain import *
from utils import log_utils
from repository.analyzer import *
from utils.check_env import check_env


# ML models
CAT_CLF_MODEL_PATH = check_env('CAT_CLF_MODEL_PATH')
EMBEDDINGS_MODEL_PATH = check_env('EMBEDDINGS_MODEL_PATH')

# Redis
REDIS_HOST = check_env('REDIS_HOST', 'localhost')
REDIS_PORT = int(check_env('REDIS_PORT', 6379))

# Redis stream consumer
REDIS_CONSUMER_GROUP = check_env('REDIS_CONSUMER_GROUP', 'article_analyzer')
REDIS_STREAM_NAME = check_env('REDIS_STREAM_NAME', 'scraped_articles')

# Elasticsearch
ELASTIC_USER = check_env('ELASTIC_USER', 'elastic')
ELASTIC_PASSWORD = check_env('ELASTIC_PASSWORD')
ELASTIC_CONN = check_env('ELASTIC_HOST', 'https://localhost:9200')
ELASTIC_CA_PATH = check_env('ELASTIC_CA_PATH', 'certs/_data/ca/ca.crt')
ELASTIC_TLS_INSECURE = bool(check_env('ELASTIC_TLS_INSECURE', False))

# Article batcher
MAX_BATCH_SIZE = int(check_env('MAX_BATCH_SIZE', 300))
MAX_BATCH_TIMEOUT_MILLIS = int(check_env('MAX_BATCH_TIMEOUT_MILLIS', 5000))


log = log_utils.create_console_logger("Main")
log.info(f"Initializing dependencies")

category_classifier = CategoryClassifier(ModelContainer.load(CAT_CLF_MODEL_PATH))
embeddings_model = EmbeddingsModel(EmbeddingsModelContainer.load(EMBEDDINGS_MODEL_PATH))

repository: AnalyzerRepository = ElasticsearchRepository(
  ELASTIC_CONN, 
  ELASTIC_USER, 
  ELASTIC_PASSWORD, 
  ELASTIC_CA_PATH, 
  not ELASTIC_TLS_INSECURE
)

redis_handler = RedisHandler(
  REDIS_HOST,
  REDIS_PORT,
)

redis_consumer = RedisScrapedArticleConsumer(
  redis_handler,
  stream_name=REDIS_STREAM_NAME,
  consumer_group=REDIS_CONSUMER_GROUP,
)

article_batcher = ArticleBatcher(
  redis_consumer,
  max_batch_size=MAX_BATCH_SIZE,
  max_batch_timeout_millis=MAX_BATCH_TIMEOUT_MILLIS,
)

analyzer = Analyzer(repository, category_classifier, embeddings_model)


if __name__ == '__main__':
  article_batcher.consume_batched_articles(analyzer.process)
