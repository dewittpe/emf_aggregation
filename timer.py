import time
import datetime

class TimerError(Exception):
    """
    A custom exception specifically for errors arrising from the use of the
    Timer class
    """

class Timer():
    def __init__(self):
        self._start_time = []
        self._task = []

    def tic(self, task = ""):

        if task != "":
            t = ' ' * (len(self._task) * 2)
            t = t + "Started  task: " + task
            print(t)

        if len(self._start_time) == 0:
            self._start_time = [time.perf_counter()]
            self._task       = [task]
        else:
            self._task.extend([task])
            self._start_time.extend([time.perf_counter()])

    def toc(self):
        if len(self._start_time) == 0:
            raise TimerError("No timer is not running.  Use .tic() to start one.")

        srt = self._start_time.pop()
        tsk = self._task.pop()

        elapsed_time = str(datetime.timedelta(seconds = time.perf_counter() - srt))

        if tsk == "":
            print("Elapsed time: " + elapsed_time)
        else:
            t = ' ' * (len(self._task) * 2)
            print(t + f"Finished task: {tsk}: " + elapsed_time)
