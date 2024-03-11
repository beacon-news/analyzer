from article_store.article_store import ArticleStore
from article_store.file_store import FileArticleStore, FileArticleStoreFactory
from article_store.noop_store import NoOpArticleStore, NoOpArticleStoreFactory
from article_store.redis_stream_store import RedisStreamArticleStore, RedisStreamArticleStoreFactory
import click
from cli_aware import ClickCliAware

store_factories = {
  "noop": NoOpArticleStoreFactory,
  "file": FileArticleStoreFactory,
  "redis_streams": RedisStreamArticleStoreFactory
}

class ArticleStoreFactory(ClickCliAware):

  config = {}

  def register_cli_options() -> list:
    opts = []
    
    store_opt = click.Option(
      param_decls=["--store"],
      type=click.Choice(store_factories.keys()),
      default="noop",
      show_default=True,
      envvar="STORE_TYPE",
      show_envvar=True,
      callback=lambda ctx, param, value: ArticleStoreFactory.config.update({'type': value})
    )
    opts.append(store_opt)
    for factory in store_factories.values():
      opts.extend(factory.register_cli_options())
    return opts

  @staticmethod
  def create() -> ArticleStore:
    return store_factories[ArticleStoreFactory.config['type']].create()