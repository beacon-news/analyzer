import schedule
import subprocess as sp
import shlex
import os
import sys
import time
import yaml
import json
from datetime import datetime
from utils import log_utils

def detach_from_parent():
  os.setpgrp()

def spawn_process(log, args, env): 
  # TODO: fire and forget, shouldn't we check periodically if the process finished?
  log.info(f"{datetime.now().isoformat()}: starting new process with args: {args}")
  sp.Popen(args=args, env=env, preexec_fn=detach_from_parent)

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
    # parse command into list of system arguments
    cmd = job_config.get("cmd")
    args = shlex.split(cmd)

    # add any environment variables
    env = os.environ.copy() | job_config.get("env", {})

    for sched_str in job_config['schedule']:
      job = create_schedule_job(sched_str)
      job.do(spawn_process, log, args, env)
      log.info(f"scheduled job: {sched_str} - {cmd}")

  while True:
    try:
      schedule.run_pending()
      time.sleep(1)
    except KeyboardInterrupt:
      log.info(f"stopping scheduler, no new jobs will be started, existing jobs will finish")
      exit(0)
