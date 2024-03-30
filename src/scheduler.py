import importlib
import schedule
import sys
import time
import yaml
import json
import multiprocessing as mp
from datetime import datetime
from typing import Callable
from utils import log_utils


def job_func():
  print("Hello world")

def start_process(target_func):
  # TODO: fire and forget, shouldn't we check periodically if the process finished?
  print(f"{datetime.now().isoformat()}: starting new process with callable {target_func}")
  proc = mp.Process(target=target_func)
  proc.start()

def get_callable(callable_str: str) -> Callable:
  callable_l = callable_str.split(':')
  if len(callable_l) != 2:
    raise ValueError(f"Invalid callable string '{callable_str}', must have the form module:callable (e.g. 'main:do_stuff')")

  module_str, func_str = callable_l
  module = importlib.import_module(module_str)

  if not hasattr(module, func_str):
    raise ValueError(f"Module {module_str} doesn't have callable {func_str}")

  func = getattr(module, func_str)
  if not callable(func):
    raise ValueError(f"{func_str} is not callable on module {module_str}")
  
  return func

# creates a 'schedule' Job from a string which looks like it's interface
# this is possible thanks to it's fluent interface
# e.g. 
# every(5).to(10).seconds
# every().day.at(10:00)
def create_schedule_job(schedule_str: str) -> schedule.Job:
  job = schedule

  attribute_names: list[str] = schedule_str.split('.')
  for attr_name in attribute_names:

    params = []

    # get the function args
    p_begin = attr_name.find('(')
    if p_begin != -1:

      func_name = attr_name[:p_begin]
      p_end = attr_name.find(')')
      if p_end == -1:
        raise ValueError(f"Function missing closing parenthesis {attr_name}")

      # convert to integer if it looks like an int
      params = [int(x) if x.isdecimal() else x for x in attr_name[p_begin+1:p_end].split(',')]
      attr_name = func_name

    if not hasattr(job, attr_name):
      raise ValueError(f"Attribute {attr_name} not found on object {job}")
      
    attr = getattr(job, attr_name)
    if callable(attr):
      job = attr(*params)
    else:
      job = attr

  return job


if __name__ == '__main__':

  log = log_utils.create_console_logger("Scheduler")

  if len(sys.argv) != 2:
    print(f"usage: {sys.argv[0]} <config_path>")
    exit(1)

  config_path = sys.argv[1]
  ending = config_path.split('.')[-1]

  with open(config_path) as f:
    if ending == "json":
      config = json.load(f)
    elif ending == "yaml" or ending == "yml":
      config = yaml.safe_load(f)

  for job_config in config['jobs']:
    callable_str: str = job_config['callable']
    func = get_callable(callable_str) 

    for sched_str in job_config['schedule']:
      job = create_schedule_job(sched_str)
      job.do(func)
      log.info(f"scheduled job: {sched_str} - {callable_str}")

  while True:
    try:
      schedule.run_pending()
      time.sleep(1)
    except KeyboardInterrupt:
      log.info(f"stopping scheduler, no new jobs will be started, existing jobs will finish")
      exit(0)


  # parse config (cron string + query)
  date_min = datetime.fromtimestamp(0).isoformat()
  date_max = datetime.now().isoformat()

  q = {
    "publish_date": {
      "from": date_min,
      "to": date_max,
    },
  }

  # transform query in config to db query
  query = {
    "bool": {
      "filter": {
        "range": {
          "article.publish_date": {
            "gte": q["publish_date"]["from"],
            "lte": q["publish_date"]["to"],
          }
        }
      }
    }
  }

  log.info(f"running topic modeling with config {q}")

  # query the db, only what is needed
  docs = es.es.search(
    index="articles",
    query=query,
  )

  # transform 
  dt = [{
    "_id": d["_id"],
    "analyzer": {
      "embeddings": d["_source"]["analyzer"]["embeddings"],
    },
    "article": {
      **d["_source"]["article"],
    }
  } for d in docs["hits"]["hits"]]

  # run topic modeling, insert topics into db, update documents with topics
  model_topics(dt)