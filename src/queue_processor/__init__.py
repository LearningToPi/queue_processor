'''
PyPi Package: queue_processor
Homepage: https://www.learningtopi.com/python-modules-applications/queue_processor/
Git: https://github.com/LearningToPi/queue_processor

Description:
This package is a simple threading based queue manager.  A queue can be created
with a specific maximum length as well as default command and final callback functions.
When an item is added to the queue, the queue_processor will create a thread to execute
the command function that was either provided when the item was added, or the default
function for the queue.  Arguments will be passed to the function and the return
value will be captured.  If a funal callback function is provided, the return value
as well as the status will be returned along with a copy of the arguments.

The purpose of this class is to provide a modular and reusable queue that can be
used in many different projects for any async tasks that need to be performed in
sequence.  The queue_processor operates as a FIFO queue and can provide a delay
between tasks (i.e. for an IR transmitter where a pause is needed).

Example:

from queue_processor import QueueManager, STATUS_OK, STATUS_TIMEOUT, STATUS_EXPIRED, STATUS_QUEUE_FULL, STATUS_EXCEPTION

def task(arg1, arg2):
    print(f'executing task with {arg1}, {arg2}')
    return arg1

def finished_callback(return_value, status, *args, **kwargs):
    print(f'Completed task return value: {return_value}, status {status}, args: {args}, kwargs: {kwargs}')

queue = QueueManager(name='test1', command_func=task, callback_func=finished_callback)
queue.add(kwargs={'arg1': 1, 'arg2': 2})


MIT License

Copyright (c) 2022 LearningToPi <contact@learningtopi.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'''
from threading import Lock, Thread, current_thread
from time import time, sleep
from types import FunctionType
from typing import TypeVar, Callable
from parameter_verification import verify_params, ParameterError
from logging_handler import create_logger, INFO, DEBUG

VERSION = (1,0,0)

# STATUS constants are returned to the finished callback
STATUS_OK = 'OK'
STATUS_TIMEOUT = 'TIMEOUT'
STATUS_EXPIRED = 'EXPIRED'
STATUS_QUEUE_FULL = 'QUEUE_FULL'
STATUS_EXCEPTION = 'EXCEPTION'


class QueueCommandError(Exception):
    """ Exception to represent a problem that occured with the queue """
    def __init__(self, value):
        super().__init__()
        self.value = value

    def __str__(self):
        return f'Queue Error: {self.value}'


class QueueCommand:
    """ Represents a queued command, this is an internal object and should not be called directly """
    def __init__(self, max_age:int, command_func:Callable, kwargs:dict, args:list, delay:int=0, finished_callback=None):
        """ Create an object for the queue, this is an internal object and should not be called directly """
        self.expire_time = time() + max_age
        self.command_func = command_func
        self.finished_callback = finished_callback
        self.args = args
        self.kwargs = kwargs
        self.timestamp = time()
        self.delay = delay
        self.ret_value = None
        self._lock = Lock()

    def __str__(self) -> str:
        """ return the object as a string """
        return f"function: {self.command_func.__name__}, timestamp: {round(self.timestamp, 0)}, expire in: {round(self.expire_time - time(), 2)}, " \
            f"expire-time: {self.expire_time}, kwargs: {str(self.kwargs)[0:60]}{'...' if len(str(self.kwargs))> 60 else ''}, args: {str(self.args)[0:60]}{'...' if len(str(self.args))>60 else ''}"

    @property
    def expired(self):
        """ Return true if max age reached """
        return False if time() <= self.expire_time else True

    def execute(self):
        ''' Execute the command function and get the return value '''
        try:
            ret_value = self.command_func(*self.args, **self.kwargs)
            with self._lock:
                self.ret_value = ret_value
        except Exception as e:
            self.ret_value = e
            raise e

    def execute_callback(self, status):
        ''' Execute the callback and pass the return value '''
        if isinstance(self.finished_callback, Callable):
            with self._lock:
                self.finished_callback(self.ret_value, status, *self.args, **self.kwargs)


class QueueManager:
    """ 
    Class to enact a queue to execute tasks in sequence (FIFO)

    Attributes
    ----------
    name : str
    depth : int
    delay : int
    max_age : int
    timeout : int
    command_func : Callable
    callback_func : Callable
    raise_queue_full : bool
    length : int 
        (readonly) 
    busy : bool 
        (readonly)
    """
    def __init__(self, name:str, command_func:Callable, depth:int=5, delay_ms:int=50, max_age:int=5, timeout:int=5, log_level=INFO, \
                 callback_func=None, raise_queue_full=False):
        ''' Init a Queue Manager class.
        Parameters
        ----------
            name : str
                Name for the Queue (used in logging)
            command_func : Callbable 
                Default function to all when a queue item is executed
            depth : int
                max size of the queue (default 5)
            delay_ms : int
                ms delay between commands (can decrease to zero if desired, used to space commands), 
                delay is AFTER command is executed (default 50ms)
            max_age : int
                max number of seconds an entry should remain in queue before timing out. 0 means no limit
                (default 5 seconds)
            timeout : int
                timeout for the command function (default 5 seconds)
            callback_func : Callable
                OPTIONAL - Function to call AFTER execution completes (default None)
            raise_queue_full :bool
                Raise an QueueCommandError if an item is added and the queue is full, if False only prints an error
                (default False)  NOTE: If queue is full, the new item is dropped 
            log_level : str
                Logging level to use (default INFO)
        '''
        self._logger = create_logger(log_level, name=f'Queue-{name}')
        try:
            verify_params(name=(name,str), depth=(depth,int,'>=0'), delay=(delay_ms,int,'>=0'), max_age=(max_age, int, '>=0'), timeout=(timeout, int, '>=0'),
                          raise_queue_full=(raise_queue_full,bool))
        except ParameterError as param_err:
            self._logger.critical('Unable to initialize queue.  Parameter error: %s', param_err)
        self.name = name # Queue name for logging purposes
        self.depth = depth # configured max depth of the queue
        self.delay = delay_ms # default delay between executing commands
        self.max_age = max_age # Max length in seconds a command can remain in the queue before being discareded
        self.timeout = timeout # Command timeout in seconds
        self.command_func = command_func # Default function to call if a specific function is not provided with the add
        self.callback_func = callback_func # Function to call after a command is completed
        self._queue = [] # list to hold the commands in queue
        self._lock = Lock()
        self._queue_exec_thread = None # object to hold the currently active thread
        self._logger.info('Queue initialized.')
        self.raise_queue_full = raise_queue_full

    def __del__(self):
        self.close()

    def close(self):
        """ Close the queue, stop associated tasks and clear objects """
        self._logger.info("Closing queue...")
        try:
            if self._queue_exec_thread is not None and self._queue_exec_thread.is_alive():
                self._queue_exec_thread.join(timeout=self.timeout)
        except RuntimeError as run_err:
            self._logger.critical('Runtime error attempting to close queue: %s', run_err)
            raise QueueCommandError(f'Runtime error attempting to close queue: {run_err}') from run_err
        self._queue_exec_thread = None
        self._queue = []

    @property
    def length(self):
        """ returns the length of the queue in the queue (includes any currently running tasks) """
        with self._lock:
            return len(self._queue)

    @property
    def busy(self):
        """ Returns true if there are any queued commands or commands currently being executed """
        if (isinstance(self._queue_exec_thread, Thread) and self._queue_exec_thread.is_alive()) or self.length > 0:
            return True
        return False

    def add(self, command_func=None, args=None, kwargs=None, delay=None, finished_func=None):
        """ 
        Add a comand to the queue

        Parameters
        ----------
            command_func : Callable|None
                Function to call when executing task, if None default function specified
                when the queue was created is used. (default None)
            args : list|None
                List of positional arguments to pass to the 'command_func' (default None)
            kwargs : dict|None
                List of key:value pairs to pass to the 'command_func' (default None)
            delay : int|None
                time in ms to wait after the function is called before continuing to another
                task in the queue. NOTE: finished_func will be executed before the delay.
                If None, the default provided with the queue will be used. (default None)

        Raises
        ------
            QueueCommandError
                IF raise_queue_full is set to True, QueueCommandError will be raised if
                an item is added to a queue that is full, otherwise a log message will
                be generated.

        Returns
        -------
            None
        """
        if self.length < self.depth:
            command_func = command_func if command_func is not None else self.command_func
            command_delay = delay if delay is not None else self.delay
            with self._lock:
                self._queue.append(QueueCommand(max_age=self.max_age,
                                                    command_func=command_func,
                                                    kwargs=kwargs if kwargs is not None else {},
                                                    args=args if args is not None else [],
                                                    delay=command_delay,
                                                    finished_callback=finished_func if finished_func is not None else self.callback_func))
                if not isinstance(self._queue_exec_thread, Thread) or not self._queue_exec_thread.is_alive():
                    self._queue_exec_thread = Thread(target=self._queue_exec, name=self.name + '_queue_exec', daemon=True)
                    self._queue_exec_thread.start()
                self._logger.debug(f"Added {self._queue[-1]} to queue.")
        else:
            self._logger.error(f"Error adding to queue.  Queue full! {command_func} with paramters: {str(args)[0:60]}{'...' if len(str(args)) > 60 else ''}: to queue...")
            callback_func = finished_func if finished_func is not None else self.callback_func
            if isinstance(callback_func, Callable):
                callback_func(None, STATUS_QUEUE_FULL, *args if args is not None else [], **kwargs if kwargs is not None else {})
            if self.raise_queue_full:
                raise QueueCommandError(f"Error adding to queue.  Queue full! {command_func} with paramters:" \
                    + f" {str(args)[0:60]}{'...' if len(str(args)) > 60 else ''}: to queue...")

    def clear(self):
        """ Clears the current queue """
        if self.length > 0:
            self._logger.info(f"Clearing queue with {self.length} items...")
            with self._lock:
                self._queue = []

    def _queue_exec(self):
        """ Starts a background thread to process and send all queued commands """
        while self.length > 0:
            with self._lock:
                queue_temp = self._queue.pop(0)
            if queue_temp.expired:
                self._logger.error(f"Queue item exired: {queue_temp}")
                try:
                    Thread(target=queue_temp.execute_callback, kwargs={'status': STATUS_EXPIRED}, daemon=True, name=self.name + '_queue_finish_callback').start()
                except RuntimeError as run_err:
                    self._logger.error('%s: Runtime error attempting to run finished command %s: %s', self.name, queue_temp.callback, run_err)
                continue

            self._logger.debug(f"Executing queue for: {queue_temp}")
            try:
                exec_thread = Thread(target=queue_temp.execute, name=self.name + '_queue_exec', daemon=True)
                exec_thread.start()
                exec_thread.join(timeout=self.timeout)
                if exec_thread.is_alive():
                    self._logger.error(f"Error executing queue for: {queue_temp}")

                    # Exec the callback with the timeout
                    try:
                        Thread(target=queue_temp.execute_callback, kwargs={'status': STATUS_TIMEOUT}, daemon=True, name=self.name + '_queue_finish_callback').start()
                        sleep(float(queue_temp.delay) / 1000.0)
                        continue
                    except RuntimeError as run_err:
                        self._logger.error('%s: Runtime error attempting to run finished command %s: %s', self.name, queue_temp.callback, run_err)
                else:
                    self._logger.debug(f"Completed executing queue for: {queue_temp}")
                exec_thread = None

                # Exec the callback with success and wait delay before continuing to next
                try:
                    Thread(target=queue_temp.execute_callback, kwargs={'status': STATUS_EXCEPTION if isinstance(queue_temp.ret_value, Exception) else STATUS_OK},
                           daemon=True, name=self.name + '_queue_finish_callback').start()
                    sleep(float(queue_temp.delay) / 1000.0)
                except RuntimeError as run_err:
                    self._logger.error('%s: Runtime error attempting to run finished command %s: %s', self.name, queue_temp.callback, run_err)
            except RuntimeError as run_err:
                self._logger.error('Runtime error attempting to run queue command %s with kwargs %s, args %s: %s', queue_temp.command_func, str(queue_temp.kwargs)[0:60],
                                   str(queue_temp.args)[0:60], run_err)
