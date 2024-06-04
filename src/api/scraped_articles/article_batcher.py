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
    self.__consumer = consumer
    self.__max_batch_size = max_batch_size
    self.__max_batch_timeout_millis = max_batch_timeout_millis

    self.__queue = []
    self.__interval_thread = None
    self.__acks_to_call = []
  
  def consume_batched_articles(self, callback: Callable[[list[dict]], None], *callback_args) -> None:
    
    self.__consume_callback = callback
    self.__consume_args = callback_args

    if self.__interval_thread is not None:
      self.__interval_thread.stop_flag.set()
      self.__interval_thread.join()
      self.__interval_thread = None

    self.__skip_iteration_flag = Event()
    self.__stop_flag = Event()
    self.__queue_lock = RLock()

    def callback_wrapper():
      try:
        self.__queue_lock.acquire()
        if len(self.__queue) > 0:
          self.__consume_callback(self.__queue, *self.__consume_args)

        # ack the messages on successful processing
        self._ack_messages()

        self.__queue = []
      finally:
        self.__queue_lock.release()

    self.__interval_thread = IntervalThread(
      self.__max_batch_timeout_millis,
      self.__skip_iteration_flag,
      self.__stop_flag,
      target=callback_wrapper
    )     
    self.__interval_thread.start()

    self.__consumer.consume_article(self._add_article)
    
  def _add_article(self, article: dict, ack: Callable[[], None]) -> None:

    # skip an iteration of the timeout interval thread, this function only gets called
    # if there was an event
    self.__skip_iteration_flag.set()

    try:
      self.__queue_lock.acquire()

      self.log.info("adding article to queue")
      self.__queue.append(article)

      # add the 'ack' function to call after the message is processed by the batch function
      self.__acks_to_call.append(ack)

      if len(self.__queue) == self.__max_batch_size:
        # consume and skip interval
        self.log.info(f"max batch size of {self.__max_batch_size} reached, calling callback")
        self.__consume_callback(self.__queue, *self.__consume_args)

        # ack the messages on successful processing
        self._ack_messages()
        self.__queue = []
    finally:
        self.__queue_lock.release()
  
  def _ack_messages(self) -> None:
    while len(self.__acks_to_call) > 0:
      ack = self.__acks_to_call.pop()
      ack()
  

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
