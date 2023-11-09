# queue_processor Python3 Queue
Homepage: https://www.learningtopi.com/python-modules-applications/queue_processor/

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

## Use Example

    from queue_processor import QueueManager, STATUS_OK, STATUS_TIMEOUT, STATUS_EXPIRED, STATUS_QUEUE_FULL, STATUS_EXCEPTION

    def task(arg1, arg2):
        print(f'executing task with {arg1}, {arg2}')
        return arg1

    def finished_callback(return_value, status, *args, **kwargs):
        print(f'Completed task return value: {return_value}, status {status}, args: {args}, kwargs: {kwargs}')

    queue = QueueManager(name='test1', command_func=task, callback_func=finished_callback)
    queue.add(kwargs={'arg1': 1, 'arg2': 2})

## Command Function

The command function can be any callable function (including a function that is part of a class).  The function will be passed all the positional and keyword arguments that are supplied when adding the instance to the queue.

## Callback Function

The callback function can be any callable function (including a function that is part of a class).  This OPTIONAL function is called after the command function either completes or times out.  The callback will provide the return value of the command function (if any) as well as a status that will be one of the following:
- OK: Command function completed (may or may not be successful in your view, but the function completed and did not raise any errors)
- TIMEOUT: If the command function did not complete within the timeout period.
- EXPIRED: The item was NOT executed as it sat in the queue longer than the max time permitted.
- QUEUE_FULL: This is returned if a callback is provided when an item is attempted to be added but the queue is full.  This item is NOT executed.
- EXCEPTION: An exception was raised during the execution of the callback.  The exception is returned as the "return_value"

### Callback function parameters

The callback function must accept the "return_value" and "status" as positional arguments in that order.  It is strongly recommended to include *args and **kwargs to catch all positional and keyword arguments that are sent after "return_value" and "status".  The queue processor will send ALL arguments to the callback function that were also sent to the command function.

NOTE: You may send objects as parameters that will be updated by the command function!  You may also have the command function be a member of a class instance.  This allows you to act on data within an instance of a class so passing objects may not be required.

!!IMPORTANT!!  The queue processor uses Python threads.  In your command and callback function be sure to use thread locks appropriately if modifying data that may be accessed by a different thread.  Also be sure not to create deadlocks!