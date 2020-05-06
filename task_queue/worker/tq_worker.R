# Worker Script - This will be executed in a separate container from the task_queue.
# The worker script uses later to check for any updates to the task object hosted on redis.
# Every interval it will update the object.

library(rworker)

con = redux::hiredis()

# RWorker Process Logic
# This checks that the update key is TRUE, then pulls the RWork object and unserializes it from Redis, and 
# sends the tasks found in tasks list to Redis, which are executed by the worker process(es).

tq_worker = rworker(name='celery', queue='redis://localhost:6379', backend='redis://localhost:6379', workers=2)

tq_worker$consume_tasks()


