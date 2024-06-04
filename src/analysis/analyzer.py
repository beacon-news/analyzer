from utils import log_utils
from repository.analyzer import AnalyzerRepository
from .classifier import CategoryClassifier
from .embeddings import EmbeddingsModel
from domain import ScrapedArticle, ScrapedArticleMetadata, Article, Category
from datetime import datetime
import hashlib


class Analyzer:

  def __init__(
    self, 
    repository: AnalyzerRepository,
    category_classifier: CategoryClassifier,
    embeddings_model: EmbeddingsModel,
  ):
    self.log = log_utils.create_console_logger(__class__.__name__)
    self.repository = repository
    self.category_classifier = category_classifier
    self.embeddings_model = embeddings_model
  

  def process(self, docs: list[dict]) -> list[str]:

    scraped_articles = []
    prepared_texts = []

    try: 
      for doc in docs:

        scraped_article = self.__map_to_article(doc)
        if scraped_article is None:
          continue
      
        scraped_articles.append(scraped_article)

      prepared_texts = self.__extract_text(scraped_articles) 
      if len(prepared_texts) == 0:
        self.log.warning(f"no text found in documents in scraped batch, skipping batch")
        return []

      # run analysis for the batch
      category_labels, embeddings = self.analyze_batch(prepared_texts)

      (categories, articles) = self.__create_categories_and_articles(
        scraped_articles, category_labels, embeddings
      )

      # store the categories if they don't exist
      cat_ids = self.repository.store_categories(list(categories.values()))
      self.log.info(f"stored {len(cat_ids)} categories")

      # store the articles
      ids = self.repository.store_analyzed_articles(articles)
      self.log.info(f"done storing batch of {len(articles)} articles")

      return ids

    except Exception:
      self.log.exception(f"error trying to analyze doc: {doc}")

  def __map_to_article(self, doc: dict) -> ScrapedArticle | None:

    if 'id' not in doc:
      self.log.error(f"no 'id' in doc: {doc}, skipping analysis")
      return None
    id = doc['id']

    if 'url' not in doc:
      self.log.error(f"no 'url' in doc: {doc}, skipping analysis")
      return None
    url = doc['url']

    # metadata is optional, can be None
    art_meta = ScrapedArticleMetadata()
    metadata = doc.get('metadata', None)
    if metadata:
      art_meta.source = metadata.get('source', None)
      art_meta.categories = metadata.get('categories', [])

    if 'components' not in doc:
      self.log.error(f"no 'components' in doc: {doc}, skipping analysis")
      return None
    
    comps = doc['components']
    if 'article' not in comps:
      self.log.error(f"'components.article' not found in doc: {doc}, skipping analysis")
      return None
    
    comps = comps['article']
    if not isinstance(comps, list):
      self.log.error(f"'components.article' is not an array: {doc}, skipping analysis")
      return None
    
    # should only contain 1 title, and 1 paragraphs section, but just in case
    titles = [] 
    paras = [] 
    authors = []
    publish_date = None
    image = None
    for component in comps:
      if 'title' in component:
        titles.append(component['title'])
      elif 'paragraphs' in component:
        if not isinstance(component['paragraphs'], list):
          self.log.error(f"'components.article.paragraphs' is not an array: {doc}, skipping analysis")
          return None
        paras.extend(component['paragraphs'])
      elif 'author' in component:
        if type(component['author']) == list:
          authors.extend(component['author'])
        else:
          authors.append(component['author'])
      elif 'publish_date' in component:
        publish_date = component['publish_date']
        publish_date = datetime.fromisoformat(publish_date).replace(second=0, microsecond=0)
      elif 'image' in component:
        image = component['image']
    
    # verify essential attributes
    if publish_date is None:
      self.log.error(f"'publish_date' not found in doc: {doc}, skipping analysis")
      return None

    if len(titles) == 0:
      self.log.error(f"'title' not found in doc: {doc}, skipping analysis")
      return None
    
    if len(paras) == 0:
      self.log.error(f"'paragraphs' not found in doc: {doc}, skipping analysis")
      return None

    # transform the scraped format into a more manageable one
    return ScrapedArticle(
      id=id,
      url=url,
      metadata=art_meta,
      publish_date=publish_date,
      image=image,
      author=authors,
      title=titles,
      paragraphs=paras,
    )
  
  def __extract_text(self, scraped_articles: list[ScrapedArticle]) -> list[str]:
    texts = []
    for article in scraped_articles:
      # extract text for analysis for each document
      text = '\n'.join(article.title) + '\n'.join(article.paragraphs)
      texts.append(text)
    return texts


  def analyze_batch(self, texts: list[str]) -> list[tuple[list[str], list[float], list[str]]]:
    # classify the text
    labels = self.category_classifier.predict_batch(texts)

    # create embeddings
    embeddings = self.embeddings_model.encode(texts)

    return (labels, embeddings.tolist())
  
  def __create_categories_and_articles(
    self, 
    scraped_articles: list[ScrapedArticle],
    predicted_categories: list[list[str]],
    embeddings: list[list[float]],
  ) -> tuple[list[Category], list[Article]]:

    # gather the categories and articles
    articles = []
    unique_category_names = set()
    categories = {}
    analyze_time = datetime.now()
    for scr_art, art_categories, art_embeddings in zip(scraped_articles, predicted_categories, embeddings):
      
      # gather the categories per article
      # categories separately from the metadata and the predictions
      meta_categories = [cat.strip().lower() for cat in scr_art.metadata.categories]
      meta_and_predicted_cat = set([*art_categories, *meta_categories])

      # add missing category objects to all, unique categories
      for cat_name in meta_and_predicted_cat.difference(unique_category_names):
        # create the Category
        categories[cat_name] = Category(
          id=hashlib.sha1(cat_name.encode()).hexdigest(), 
          name=cat_name
        )
      unique_category_names = unique_category_names.union(meta_and_predicted_cat)

      # get the category objects for the categories from metadata and predictions
      meta_and_predicted_cat = [categories[name] for name in meta_and_predicted_cat]
      predicted_categories = [categories[name] for name in art_categories]

      # create the Article
      articles.append(
        Article(
          id=scr_art.id,
          url=scr_art.url,
          source=scr_art.metadata.source,
          publish_date=scr_art.publish_date,
          image=scr_art.image,
          author=scr_art.author,
          title=scr_art.title,
          paragraphs=scr_art.paragraphs,
          categories=meta_and_predicted_cat,
          analyze_time=analyze_time,
          analyzed_categories=predicted_categories,
          embeddings=art_embeddings,
          topics=None
        )
      )

    return (categories, articles)
