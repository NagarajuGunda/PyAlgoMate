import asyncio
import contextlib
import logging
import os
import platform
import signal
import sys
import time
from datetime import datetime, timedelta
from threading import Thread
from typing import (
    Any,
    Callable,
    Coroutine,
    Dict,
    NoReturn,
    Optional,
    Tuple,
)

from pyalgotrade import dispatchprio, observer, utils

try:
    # unix / macos only
    from signal import SIGABRT, SIGHUP, SIGINT, SIGTERM

    SIGNALS = (SIGABRT, SIGINT, SIGTERM, SIGHUP)
except (ImportError, ModuleNotFoundError):
    from signal import SIGABRT, SIGINT, SIGTERM

    SIGNALS = (SIGABRT, SIGINT, SIGTERM)

if not platform.system().lower().startswith("win") and sys.version_info >= (
    3,
    8,
):  # noqa E501
    try:
        import uvloop
    except (ImportError, ModuleNotFoundError):
        os.system(f"{sys.executable} -m pip install uvloop")
        import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
else:
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logger = logging.getLogger(__name__)

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


class ProgramKilled(Exception):
    """ProgramKilled Checks the ProgramKilled exception"""

    pass  # type: ignore


def singleton(cls):
    _instance = None
    def get_instance(*args, **kwargs):
        nonlocal _instance
        if _instance is None:
            _instance = cls(*args, **kwargs)
        return _instance
    return get_instance


class AsyncDispatcher:

    def __init__(self):
        self.scheduled_coroutines: Dict[Any, Tuple[datetime, Coroutine]] = {}
        self.recurring_tasks: Dict[Any, asyncio.Task] = {}
        self.__initialize_loop()

    @staticmethod
    def start_background_loop(
        loop: asyncio.AbstractEventLoop,
    ) -> Optional[NoReturn]:
        asyncio.set_event_loop(loop)
        try:
            loop.run_forever()
        except (KeyboardInterrupt, SystemExit, ProgramKilled):
            loop.run_until_complete(loop.shutdown_asyncgens())
            if loop.is_running():
                loop.stop()
            if not loop.is_closed():
                loop.close()

    def __initialize_loop(self) -> None:
        if platform.system().lower().find("win") == -1:
            self.loop = uvloop.new_event_loop()
        else:
            self.loop = asyncio.new_event_loop()
        if platform.system().lower().startswith("win"):
            with contextlib.suppress(ValueError):
                for sig in (SIGABRT, SIGINT, SIGTERM):
                    signal.signal(sig, self.handle_stop_signals)
        else:
            with contextlib.suppress(ValueError):
                for sig in (SIGABRT, SIGINT, SIGTERM, SIGHUP):
                    self.loop.add_signal_handler(
                        sig, self.handle_stop_signals
                    )  # noqa E501
        self.event_thread = Thread(
            target=self.start_background_loop,
            args=(self.loop,),
            name=f"{self.__class__.__name__}_event_thread",
            daemon=True,
        )
        self.event_thread.start()
        logger.debug("Asyncio Event Loop has been initialized.")

    def handle_stop_signals(self, *args, **kwargs):
        try:
            self.graceful_exit()
        except Exception as err:
            logger.error(str(err))
        else:
            raise SystemExit

    def graceful_exit(self) -> None:
        with contextlib.suppress(RuntimeError, RuntimeWarning):
            asyncio.run_coroutine_threadsafe(self.loop.shutdown_asyncgens(), self.loop)
            if self.loop.is_running():
                self.loop.stop()
            if not self.loop.is_closed():
                self.loop.close()

    def run(
        self, coroutine: Coroutine, callback: Optional[Callable] = None
    ) -> asyncio.Future:
        def done_callback(future: asyncio.Future) -> None:
            if callback:
                result = future.result()
                callback(result)

        future: asyncio.Future = asyncio.run_coroutine_threadsafe(coroutine, self.loop)
        if callback:
            future.add_done_callback(done_callback)
        return future

    def schedule(
        self, coroutine: Coroutine, when: datetime, task_id: Any = None
    ) -> None:
        if task_id in self.scheduled_coroutines:
            del self.scheduled_coroutines[task_id]

        self.scheduled_coroutines[task_id] = (when, coroutine)

    def check_scheduled_tasks(self, current_time: datetime) -> None:
        coroutines_to_run = []
        for task_id, (scheduled_time, coroutine) in list(
            self.scheduled_coroutines.items()
        ):
            if current_time >= scheduled_time:
                coroutines_to_run.append((task_id, coroutine))

        for task_id, coroutine in coroutines_to_run:
            del self.scheduled_coroutines[task_id]
            self.loop.call_soon_threadsafe(lambda c=coroutine: self.loop.create_task(c))

    def cancel_task(self, task_id: Any) -> None:
        if task_id in self.scheduled_coroutines:
            when, coroutine = self.scheduled_coroutines[task_id]
            if asyncio.iscoroutine(coroutine):
                self.loop.call_soon_threadsafe(self._cancel_coroutine, coroutine)
            del self.scheduled_coroutines[task_id]
            logger.info(f"Cancelled scheduled task with id {task_id}")
        elif task_id in self.recurring_tasks:
            task = self.recurring_tasks[task_id]
            self.loop.call_soon_threadsafe(task.cancel)
            del self.recurring_tasks[task_id]
            logger.info(f"Cancelled recurring task with id {task_id}")
        else:
            logger.info(f"No task found with id {task_id}")

    def _cancel_coroutine(self, coroutine: Coroutine) -> None:
        task = self.loop.create_task(coroutine)
        task.cancel()

    def stop(self) -> None:
        for _, coroutine in self.scheduled_coroutines.values():
            if asyncio.iscoroutine(coroutine):
                self.loop.call_soon_threadsafe(self._cancel_coroutine, coroutine)
        self.scheduled_coroutines.clear()

        for task in self.recurring_tasks.values():
            self.loop.call_soon_threadsafe(task.cancel)
        self.recurring_tasks.clear()

        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join()

    def schedule_recurring(
        self, coroutine: Callable[[], Coroutine], interval: float, task_id: Any = None
    ):
        if task_id in self.recurring_tasks:
            logger.info(
                f"Recurring task with id {task_id} already exists. Skipping scheduling."
            )
            return

        async def recurring_task():
            while True:
                await coroutine()
                await asyncio.sleep(interval)

        task = self.loop.create_task(recurring_task())
        self.recurring_tasks[task_id] = task


@singleton
class LiveAsyncDispatcher(AsyncDispatcher):

    def __init__(self):
        super().__init__()
        self.check_interval: float = 0.01
        self.is_running: bool = True
        self.check_task: asyncio.Future = self.run(self.continuous_check())

    async def continuous_check(self) -> None:
        while self.is_running:
            current_time = datetime.now()
            self.check_scheduled_tasks(current_time)
            await asyncio.sleep(self.check_interval)

    def stop(self) -> None:
        self.is_running = False
        if self.check_task:
            self.check_task.cancel()
        super().stop()


@singleton
class BacktestingAsyncDispatcher(AsyncDispatcher):

    def __init__(self, feed):
        super().__init__()
        self.feed = feed
        feed.getNewValuesEvent().subscribe(self.on_bars)

    def on_bars(self, dateTime: datetime, bars: Any) -> None:
        self.check_scheduled_tasks(
            dateTime + timedelta(seconds=self.feed.getFrequency())
        )
