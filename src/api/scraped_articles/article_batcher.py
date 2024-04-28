from typing import Callable
from api.scraped_articles.article_consumer import ScrapedArticleConsumer
from threading import Thread, Event, RLock
from time import sleep
from utils import log_utils

class ArticleBatcher:

  def __init__(
    self, 
    consumer: ScrapedArticleConsumer, 
    max_batch_size: int = 1000, 
    max_batch_timeout_millis: int = 5000
  ):
    self.log = log_utils.create_console_logger(
      self.__class__.__name__,
    )
    self.consumer = consumer
    self.max_batch_size = max_batch_size
    self.max_batch_timeout_millis = max_batch_timeout_millis

    self.queue = []
    self.interval_thread = None

  
  def add_article(self, article: dict):

    # skip an iteration of the timeout interval thread, this function only gets called
    # if there wan an event
    self.skip_iteration_flag.set()

    try:
      self.queue_lock.acquire()
      if len(self.queue) < self.max_batch_size:
        self.log.info("adding article to queue")
        self.queue.append(article)
      else:
        # consume and skip interval
        self.log.info(f"max batch size of {self.max_batch_size} reached, calling callback")
        self.consume_callback(self.queue, *self.consume_args)
        self.queue = []
    finally:
        self.queue_lock.release()
  
  
  def consume_batched_articles(self, callback: Callable[[list[dict]], None], *callback_args) -> None:
    
    self.consume_callback = callback
    self.consume_args = callback_args

    if self.interval_thread is not None:
      self.interval_thread.stop_flag.set()
      self.interval_thread.join()
      self.interval_thread = None

    self.skip_iteration_flag = Event()
    self.stop_flag = Event()
    self.queue_lock = RLock()

    def callback_wrapper():
      try:
        self.queue_lock.acquire()
        if len(self.queue) > 0:
          self.consume_callback(self.queue, *self.consume_args)
        self.queue = []
      finally:
        self.queue_lock.release()

    self.interval_thread = IntervalThread(
      self.max_batch_timeout_millis,
      self.skip_iteration_flag,
      self.stop_flag,
      target=callback_wrapper
    )     
    self.interval_thread.start()

    self.consumer.consume_article(lambda article: self.add_article(article))
    

class IntervalThread(Thread):

  def __init__(self, interval_millis: int, skip_iteration_flag: Event, stop_flag: Event, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.log = log_utils.create_console_logger(
      self.__class__.__name__,
    )
    self.interval_seconds = interval_millis / 1000
    self.skip_iteration_flag = skip_iteration_flag
    self.stop_flag = stop_flag
    self.daemon = True
  
  def run(self) -> None:
    self.log.info("starting interval thread")
    while True:
      if (self.stop_flag.is_set()):
        break

      sleep(self.interval_seconds)

      if not self.skip_iteration_flag.is_set():
        self.log.debug("calling target")
        self._target()
      else: 
        self.log.debug("interval thread skipped iteration")

      self.skip_iteration_flag.clear()


