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


# WARNING: this changes sys.argv
def start_process(log, target_func, sys_argv=[]):
  # hacky way to set cli args for every process
  sys.argv = sys_argv

  # TODO: fire and forget, shouldn't we check periodically if the process finished?
  log.info(f"{datetime.now().isoformat()}: starting new process with callable {target_func}")
  proc = mp.Process(target=target_func)
  proc.start()

def get_callable(callable_str: str) -> Callable:
  # TODO: do this part in the process, don't load modules into memory here
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

  with open(config_path) as f:
    if config_path.endswith(".json"):
      config = json.load(f)
    elif config_path.endswith(".yaml") or config_path.endswith(".yml"):
      config = yaml.safe_load(f)

  for job_config in config['jobs']:
    callable_str: str = job_config['callable']
    func = get_callable(callable_str) 
    args = job_config.get('args', [])

    for sched_str in job_config['schedule']:
      job = create_schedule_job(sched_str)
      job.do(start_process, log, func, args)
      log.info(f"scheduled job: {sched_str} - {callable_str}")

  while True:
    try:
      schedule.run_pending()
      time.sleep(1)
    except KeyboardInterrupt:
      log.info(f"stopping scheduler, no new jobs will be started, existing jobs will finish")
      exit(0)
