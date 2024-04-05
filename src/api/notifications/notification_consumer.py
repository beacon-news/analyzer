from abc import ABC, abstractmethod
from typing import Callable
from api.notifications.scraper_notification import ScraperDoneNotification


class NotificationConsumer(ABC):
  """Called when a notification is received."""

  @abstractmethod
  def consume_scraper_done(self, callback: Callable[[ScraperDoneNotification], None], *callback_args) -> None:
    """Consume a list of scraper done notifications."""
    raise NotImplementedError
  