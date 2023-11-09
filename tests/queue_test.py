import unittest
from time import sleep
from threading import Lock
from logging_handler import create_logger, INFO, DEBUG, WARNING, CRITICAL
import queue_processor


class TestObj:
    ''' Class to hold the test info '''
    def __init__(self, name, queue_depth, items_to_queue, call_func, finished_func=None, ret_value=True, delay_ms=50, max_age=5, timeout=5, clear=False):
        self.queue = queue_processor.QueueManager(name=name, depth=queue_depth,
                                                  command_func=getattr(self, call_func),
                                                  callback_func=getattr(self, finished_func) if finished_func is not None else None,
                                                  delay_ms=delay_ms,
                                                  max_age=max_age,
                                                  timeout=timeout,
                                                  log_level=CRITICAL)
        self.queue_data = [{'started': False, 'complete': False, 'callback': False, 'status': None, 'error': False} for x in range(items_to_queue)]
        self._lock = Lock()
        self._logger = create_logger(CRITICAL)
        for x in range(items_to_queue):
            self.queue.add(args=[x, ret_value])
            with self._lock:
                self.queue_data[x]['started'] = True

        # test clearing the queue
        if clear:
            self._logger.info('Clearing the queue...')
            self.queue.clear()

        # wait until the queue is empty
        while True:
            sleep(1)
            if self.queue.length == 0:
                return

    def ok_immediate(self, iteration, ret_value=True):
        self._logger.debug(f"Iteration {iteration} complete")
        with self._lock:
            self.queue_data[iteration]['complete'] = True
        return ret_value

    def ok_delay(self, iteration, delay=3, ret_value=True):
        sleep(delay)
        self._logger.debug(f"Iteration {iteration} complete")
        with self._lock:
            self.queue_data[iteration]['complete'] = True
        return ret_value

    def callback(self, ret_value, status, iteration, *args, **kwargs):
        self._logger.debug(f"Iteration {iteration} CALLBACK, return: {ret_value}, status: {status}")
        with self._lock:
            self.queue_data[iteration]['callback'] = ret_value
            self.queue_data[iteration]['status'] = status

    def no_end(self, iteration, ret_value=True):
        self._logger.debug(f"Iteration {iteration} will now hang...")
        sleep(9999)

    def fail_return(self, iteration, ret_value=False):
        ''' Sample function that returns a failed status '''
        with self._lock:
            self.queue_data[iteration]['complete'] = True
        self._logger.debug(f"Iteration {iteration} returning a fail...")
        return ret_value
    
    def fail_raise(self, iteration, ret_value=False):
        ''' Sample function that raises an exception '''
        self._logger.debug(f"Iteration {iteration} will now raise an error...")
        raise ValueError(f"Iteration {iteration} value error")
    
    def tests_passed(self, iterations=None):
        ''' Return True if passed iterations were successful '''
        if iterations is None:
            iterations = list(range(len(self.queue_data)))
        for x in iterations:
            if not self.queue_data[x].get('complete') or self.queue_data[x].get('error'):
                self._logger.error(f"Iteration {x} should have passed: {self.queue_data[x]}")
                return False
        return True
    
    def tests_callback(self, iterations=None, value=True):
        ''' Return True if passed iterations have passed value as a callback '''
        if iterations is None:
            iterations = list(range(len(self.queue_data)))
        for x in iterations:
            if self.queue_data[x].get('callback') != value:
                return False
        return True

    def tests_status(self, iterations=None, status=None):
        ''' Return True if passed iterations have passed value as a callback '''
        if iterations is None:
            iterations = list(range(len(self.queue_data)))
        for x in iterations:
            if self.queue_data[x].get('status') != status:
                return False
        return True
    
    def status_count(self, status):
        ''' Return a count matching the status '''
        return len([x for x in self.queue_data if x.get('status') == status])

    @property
    def passed_count(self):
        return len([x for x in self.queue_data if x.get('complete') and not x.get('error')])

    def tests_failed(self, iterations=None):
        ''' Return True if passed iterations failed '''
        if iterations is None:
            iterations = list(range(len(self.queue_data)))
        for x in iterations:
            if self.queue_data[x].get('complete'):
                self._logger.error(f"Iteration {x} should have failed: {self.queue_data[x]}")
                return False
        return True

    @property
    def failed_count(self):
        return len([x for x in self.queue_data if x.get('error') or not x.get('complete')])


class QueueTester(unittest.TestCase):
    ''' Exceute the unit tests in the queue processor class '''
    def test_1_queue_10_ok(self):
        ''' Create queue and queue up 10 items without a finished function, wait for completion and run again with a finished function '''
        count = 10
        test1 = TestObj(name='test1-no-finished', queue_depth=count, items_to_queue=count, call_func='ok_immediate')
        self.assertTrue(test1.tests_passed())
        test2 = TestObj(name='test2-w-finished', queue_depth=count, items_to_queue=count, call_func='ok_immediate', finished_func='callback')
        self.assertTrue(test2.tests_passed())
        self.assertTrue(test2.status_count(queue_processor.STATUS_OK) == count)

    def test_2_queue_100_ok(self):
        ''' Create queue and queue up 100 items without a finished function, wait for completion and run again with a finished function '''
        count = 100
        test1 = TestObj(name='test1-no-finished', queue_depth=count, items_to_queue=count, call_func='ok_immediate', max_age=10)
        self.assertTrue(test1.tests_passed())
        test2 = TestObj(name='test2-w-finished', queue_depth=count, items_to_queue=count, call_func='ok_immediate', finished_func='callback', max_age=10)
        self.assertTrue(test2.tests_passed())
        self.assertTrue(test2.status_count(queue_processor.STATUS_OK) == count)

    def test_3_queue_1000_ok(self):
        ''' Create queue and queue up 1000 items without a finished function, wait for completion and run again with a finished function '''
        count = 1000
        test1 = TestObj(name='test1-no-finished', queue_depth=count, items_to_queue=count, call_func='ok_immediate', delay_ms=0)
        self.assertTrue(test1.tests_passed())
        test2 = TestObj(name='test2-w-finished', queue_depth=count, items_to_queue=count, call_func='ok_immediate', finished_func='callback', delay_ms=0)
        self.assertTrue(test2.tests_passed())
        self.assertTrue(test2.status_count(queue_processor.STATUS_OK) == count)

    def test_4_queue_1000_overflow(self):
        ''' Create a queue with a max of 10 and add 1000 items '''
        count = 1000
        queue_depth = 10
        test1 = TestObj(name='test1-no-finished', queue_depth=queue_depth, items_to_queue=count, call_func='ok_immediate')
        print(test1.passed_count, test1.failed_count)
        self.assertTrue(test1.tests_passed(list(range(queue_depth))))
        self.assertTrue(test1.passed_count <= 20)
        test2 = TestObj(name='test2-w-finished', queue_depth=queue_depth, items_to_queue=count, call_func='ok_immediate', finished_func='callback')
        print(test2.passed_count, test2.failed_count)
        self.assertTrue(test2.tests_passed(list(range(queue_depth))))
        self.assertTrue(test2.status_count(queue_processor.STATUS_QUEUE_FULL) >= queue_depth)
        self.assertTrue(test2.passed_count <= 20)

    def test_5_queue_10_timeout(self):
        ''' Create a queue and queue up 10 items that will not complete '''
        count = 10
        queue_depth = 10
        max_age = 120
        timeout=1
        test1 = TestObj(name='test1-no-finished', queue_depth=queue_depth, items_to_queue=count, call_func='no_end', max_age=max_age, timeout=timeout)
        print(test1.passed_count, test1.failed_count)
        self.assertTrue(test1.passed_count == 0)
        test2 = TestObj(name='test2-w-finished', queue_depth=queue_depth, items_to_queue=count, call_func='no_end', finished_func='callback', max_age=max_age, timeout=timeout)
        sleep(3) # wait for last timeout callback
        print(test2.passed_count, test2.failed_count, test2.status_count(queue_processor.STATUS_TIMEOUT))
        self.assertTrue(test2.status_count(queue_processor.STATUS_TIMEOUT) == count)

    def test_6_queue_10_fail_raise(self):
        ''' Create a queue and queue up 10 items that will fail with a raise '''
        count = 10
        queue_depth = 10
        max_age = 10
        timeout=1
        test1 = TestObj(name='test1-no-finished', queue_depth=queue_depth, items_to_queue=count, call_func='fail_raise', max_age=max_age, timeout=timeout)
        print(test1.passed_count, test1.failed_count)
        self.assertTrue(test1.passed_count == 0)
        test2 = TestObj(name='test2-w-finished', queue_depth=queue_depth, items_to_queue=count, call_func='fail_raise', finished_func='callback', max_age=max_age, timeout=timeout)
        sleep(3) # wait for last timeout callback
        print(test2.passed_count, test2.failed_count, test2.status_count(queue_processor.STATUS_EXCEPTION))
        self.assertTrue(test2.status_count(queue_processor.STATUS_EXCEPTION) == count)

    def test_7_queue_10_fail_return(self):
        ''' Create a queue and queue up 10 items that will fail with a return that triggers failure, repeat with different return triggers '''
        count = 10
        queue_depth = 10
        max_age = 10
        timeout=1
        test1 = TestObj(name='test1-no-finished', queue_depth=queue_depth, items_to_queue=count, call_func='fail_return', max_age=max_age, timeout=timeout, ret_value=False)
        print(test1.passed_count, test1.failed_count)
        # self.assertTrue(test1.passed_count == 0) Can't check passed count since we aren't getting a return
        test2 = TestObj(name='test2-w-finished', queue_depth=queue_depth, items_to_queue=count, call_func='fail_return', finished_func='callback', max_age=max_age, timeout=timeout, ret_value=False)
        sleep(3) # wait for last timeout callback
        print(test2.passed_count, test2.failed_count, test2.status_count(queue_processor.STATUS_OK))
        self.assertTrue(test2.status_count(queue_processor.STATUS_OK) == count) # STATUS is OK because failure is a return value NOT an exception
        self.assertTrue(test2.tests_callback(None, False)) # check that callback was FALSE

    def test_6_queue_clear(self):
        ''' Creat a queue and queue up 1000 items, then call to clear the queue '''
        count = 10
        queue_depth = 1000
        max_age = 30
        timeout= 2
        test1 = TestObj(name='test1-no-finished', queue_depth=queue_depth, items_to_queue=count, call_func='ok_delay', max_age=max_age, timeout=timeout, ret_value=False, clear=True)
        print(test1.passed_count, test1.failed_count)
        self.assertTrue(test1.passed_count == 1)
        test2 = TestObj(name='test2-w-finished', queue_depth=queue_depth, items_to_queue=count, call_func='ok_delay', finished_func='callback', max_age=max_age, timeout=timeout, ret_value=False, clear=True)
        sleep(3) # wait for last timeout callback
        print(test2.passed_count, test2.failed_count, test2.status_count(queue_processor.STATUS_OK))
        self.assertTrue(test1.passed_count == 1)


if __name__ == '__main__':
    unittest.main()
