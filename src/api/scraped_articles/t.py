from typing import Callable
from threading import Thread, Event
from time import sleep

class IntervalThread(Thread):

  def __init__(self, interval_millis: int, skip_iteration_flag: Event, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.interval_seconds = interval_millis / 1000
    self.skip_iteration_flag = skip_iteration_flag
    self.daemon = True
  
  def run(self) -> None:
    while True:
      sleep(self.interval_seconds)

      if not self.skip_iteration_flag.is_set():
        self._target()
        
      self.skip_iteration_flag.clear()

i = 0
def p():
  global i
  i += 1
  print("hello", i)
        
e = Event()
t = IntervalThread(1000, e, target=p)
t.start()

m = 2300
sleep(m / 1000)
e.set()
i = 0

sleep(10000)

