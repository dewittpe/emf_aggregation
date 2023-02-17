import time
import datetime

class TimerError(Exception):
    """
    A custom exception specifically for errors arrising from the use of the
    Timer class
    """

class Timer():
    def __init__(self, task = "", verbose = False):
        self._start_time = []
        self._task = []
        self._verbose = verbose
        if task != "":
            self.tic(task, verbose)

    def __enter__(self):
        if len(self._task) == 0:
            self.tic()
        return self

    def __exit__(self, *exc_info):
        self.toc()

    def tic(self, task = "", verbose = True):

        if task != "":
            t = ' ' * (len(self._task) * 2)
            t = t + "Started  task: " + task
            if self._verbose:
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

        if self._verbose:
            if tsk == "":
                print("Elapsed time: " + elapsed_time)
            else:
                t = ' ' * (len(self._task) * 2)
                print(t + f"Finished task: {tsk}: " + elapsed_time)

        return {"task" : tsk, "time" : elapsed_time}
