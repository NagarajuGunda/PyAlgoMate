import asyncio
import threading
import time
from datetime import datetime, timedelta

from pyalgotrade import dispatchprio, observer, utils


# This class is responsible for dispatching events from multiple subjects, synchronizing them if necessary.
class Dispatcher(object):
    def __init__(self):
        self.__subjects = []
        self.__stop = False
        self.__startEvent = observer.Event()
        self.__idleEvent = observer.Event()
        self.__currDateTime = None

    # Returns the current event datetime. It may be None for events from realtime subjects.
    def getCurrentDateTime(self):
        return self.__currDateTime

    def getStartEvent(self):
        return self.__startEvent

    def getIdleEvent(self):
        return self.__idleEvent

    def stop(self):
        self.__stop = True

    def getSubjects(self):
        return self.__subjects

    def addSubject(self, subject):
        # Skip the subject if it was already added.
        if subject in self.__subjects:
            return

        # If the subject has no specific dispatch priority put it right at the end.
        if subject.getDispatchPriority() is dispatchprio.LAST:
            self.__subjects.append(subject)
        else:
            # Find the position according to the subject's priority.
            pos = 0
            for s in self.__subjects:
                if (
                    s.getDispatchPriority() is dispatchprio.LAST
                    or subject.getDispatchPriority() < s.getDispatchPriority()
                ):
                    break
                pos += 1
            self.__subjects.insert(pos, subject)

        subject.onDispatcherRegistered(self)

    # Return True if events were dispatched.
    def __dispatchSubject(self, subject, currEventDateTime):
        ret = False
        # Dispatch if the datetime is currEventDateTime of if its a realtime subject.
        if not subject.eof() and subject.peekDateTime() in (None, currEventDateTime):
            ret = subject.dispatch() is True
        return ret

    # Returns a tuple with booleans
    # 1: True if all subjects hit eof
    # 2: True if at least one subject dispatched events.
    def __dispatch(self):
        smallestDateTime = None
        eof = True
        eventsDispatched = False

        # Scan for the lowest datetime.
        for subject in self.__subjects:
            if not subject.eof():
                eof = False
                smallestDateTime = utils.safe_min(
                    smallestDateTime, subject.peekDateTime()
                )

        # Dispatch realtime subjects and those subjects with the lowest datetime.
        if not eof:
            self.__currDateTime = smallestDateTime

            for subject in self.__subjects:
                if self.__dispatchSubject(subject, smallestDateTime):
                    eventsDispatched = True
        return eof, eventsDispatched

    def run(self):
        try:
            for subject in self.__subjects:
                subject.start()

            self.__startEvent.emit()

            while not self.__stop:
                eof, eventsDispatched = self.__dispatch()
                if eof:
                    self.__stop = True
                elif not eventsDispatched:
                    self.__idleEvent.emit()
                time.sleep(0.01)
        finally:
            # There are no more events.
            self.__currDateTime = None

            for subject in self.__subjects:
                subject.stop()
            for subject in self.__subjects:
                subject.join()


class AsyncDispatcher:
    def __init__(self, strategy):
        self.strategy = strategy
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self.scheduled_tasks = {}
        self.thread.start()

    def _run_event_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def run(self, coroutine, callback=None):
        def done_callback(future):
            if callback:
                result = future.result()
                callback(result)

        future = asyncio.run_coroutine_threadsafe(coroutine, self.loop)
        if callback:
            future.add_done_callback(done_callback)
        return future

    def schedule(self, coroutine, when, task_id=None):
        if task_id in self.scheduled_tasks:
            self.cancel_task(task_id)

        self.scheduled_tasks[task_id] = (coroutine, when)
        return task_id

    def check_scheduled_tasks(self, current_time):
        tasks_to_run = []
        for task_id, (coroutine, scheduled_time) in list(self.scheduled_tasks.items()):
            if current_time >= scheduled_time:
                tasks_to_run.append((task_id, coroutine))

        for task_id, coroutine in tasks_to_run:
            del self.scheduled_tasks[task_id]
            self.run(coroutine)

    def cancel_task(self, task_id):
        if task_id in self.scheduled_tasks:
            del self.scheduled_tasks[task_id]

    def stop(self):
        self.scheduled_tasks.clear()
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join()


class LiveAsyncDispatcher(AsyncDispatcher):

    def __init__(self, strategy):
        super().__init__(strategy)
        self.check_interval = 0.01
        self.is_running = True
        self.check_task = self.run(self.continuous_check())

    async def continuous_check(self):
        while self.is_running:
            current_time = datetime.now()
            self.check_scheduled_tasks(current_time)
            await asyncio.sleep(self.check_interval)

    def stop(self):
        self.is_running = False
        if self.check_task:
            self.check_task.cancel()
        super().stop()


class BacktestingAsyncDispatcher(AsyncDispatcher):
    def __init__(self, strategy):
        super().__init__(strategy)
        self.strategy.getFeed().getNewValuesEvent().subscribe(self.on_bars)

    def on_bars(self, dateTime, bars):
        self.check_scheduled_tasks(
            dateTime + timedelta(seconds=self.strategy.getFeed().getFrequency())
        )
