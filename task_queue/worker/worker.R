# Worker Script - This will be executed in a separate container from the task_queue.
# The worker script uses later to check for any updates to the task object hosted on redis.
# Every interval it will update the object.

# Then, if it is different, it will restart the callR session to enable the R worker while
# executing the updated tasks list. 

library(rworker)
library(callr)
library(redux)
library(later)

con = redux::hiredis()

# RWorker Process Logic
# This checks that the update key is TRUE, then checks for the persistant
# R session status. If it's running, the tree is killed, then restarted. The
# persistant session pulls the RWork object and unserializes it from Redis, then 
# sends the tasks found in tasks list to Redis, which are executed by the rworker subprocesses.

rworker_process <- function() {
  if (con$GET('update')){
    if (exists('rs') && rs$get_state() == "busy") {
      rs$kill_tree()
      print("Tree Killed")
      Sys.sleep(10)
    }
    
  if (!is.null(con$GET('rwork'))){
    rs <- r_session$new()
    
    rs$call(function() {
      library(rworker)
      library(redux)
      
      con <- redux::hiredis()
      rwork_object <- con$GET("rwork")
      rwork <- unserialize(rwork_object)
      # Print the list of tasks
      rwork$tasks
      
      for (task in rwork$tasks){
        task_to_send <- task$task_structure
        
        con$RPUSH('celery', task_to_send)
      }
      
      rwork$consume()
      
      con$SET('update', 'FALSE')
    })
  }
  }
}


# NEED TO TEST/CHANGE - WIP
# Heartbeat Process - This uses the later package as a heartbeat to recursively 
# execute the Rworker logic at a scheduled interval every 10 seconds. This might need 
# some finetuning, but it serves as a solution for the time being.

i = 0
f = function() {
  if (i %% 1000 == 0) {
    print(i)
    rworker_process()
  }
  i <<- i + 1
  later::later(f, 0.001)
}
f()



