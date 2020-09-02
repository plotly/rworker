#R6 Class for Task

# Notes: 
# Create a task using $create_task(), or directly supply a task function to
# $queue_task(). Otherwise, supplying a task name matching one in tasks_list() will 
# also be pushed to the Redis queue. After that, use TaskQueueR::rwork$consume() to 
# spawn the worker processes to execute the tasks. 


# For Demo Purposes:
library(R6)
library(rworker)
library(redux)
library(tidyr)
library(jsonlite)


library(dash)
library(dashCoreComponents)
library(dashHtmlComponents)

###################################################################################

R6_Task_QueueR <- R6Class('tq_client', 
                          public = list(
                            con = redux::hiredis(),
                            worker_list = list(),
                            tasks_list = list(),
                            rwork = rworker(name='celery', queue='redis://localhost:6379', backend='redis://localhost:6379', workers=2),

                            create_task = function(fun, task_name = id_generator(1)) {
                              self$tasks_list[[task_name]] <- fun
                              invisible(self)
                            },
                            
                            queue_task = function(task = NULL, task_name = private$id_generator(1), priority = 0, delivery_tag = 'default_delivery'){
                              if(!is.function(task) && !is.null(task)) stop('Task must be defined as a function.')
                              
                              task_structure <- list(
                                'body' = 'W1tdLCB7fSwgeyJjYWxsYmFja3MiOiBudWxsLCAiZXJyYmFja3MiOiBudWxsLCAiY2hhaW4iOiBudWxsLCAiY2hvcmQiOiBudWxsfV0=',
                                'content-encoding' = 'utf-8',
                                'content-type' = 'application/json',
                                'headers' = list(
                                  'lang' = 'r',
                                  'task' = task_name,
                                  'id' = task_name,
                                  'shadow' = NULL,
                                  'eta' = NULL,
                                  'expires' = NULL,
                                  'group' = NULL,
                                  'retries' = 0,
                                  'timelimit' = c(NA, NA),
                                  'root_id' = task_name,
                                  'parent_id' = NULL, 
                                  'argsrepr' = '()',
                                  'kwargsrepr' = '{}',
                                  'origin' = Sys.info()[['nodename']]
                                ),
                                'properties' = list(
                                  'correlation_id' = task_name,
                                  'reply_to' = delivery_tag,
                                  'delivery_mode' = 2,
                                  'delivery_info' = list(
                                    'exchange' = '',
                                    'routing_key' = 'celery'
                                  ),
                                  'priority' = priority,
                                  'body_encoding' = 'base64',
                                  'delivery_tag' = delivery_tag
                                )
                              )
                              
                              task_JSON <- private$to_JSON(task_structure)
                              
                              if(task_name %in% names(self$tasks_list)) {
                                self$rwork$task(FUN = self$tasks_list[[task_name]], name = task_name, task_structure = task_JSON)
                              }
                              else if (!(task_name %in% names(self$tasks_list))) {
                                self$tasks_list[[task_name]] = task
                                self$rwork$task(FUN = task, name = task_name, task_structure = task_JSON)
                              }
                              
                              invisible(self)
                            },
                            
                            task_result = function(task_name) {
                              if (!(task_name %in% names(self$rwork$tasks))) stop('Task does not exist or has not been queued.')
                              
                              tryCatch(
                                expr = {
                                  result_JSON <- self$con$GET(paste0('celery-task-meta-', task_name))
                                  
                                  result_string <- fromJSON(result_JSON)
                                  
                                  return(result_string$result$result)
                                },
                                error = function(e) {
                                  message('Error! Task may have failed or has not been executed yet. See error below.')
                                  print(e)
                                },
                                warning = function(w) {
                                  message('Warning! Check syntax or task structure. See warning below.')
                                  print(w)
                                },
                                finally = {
                                  message('Quitting...')
                                }
                              )
                            },
                            
                            send_tasks = function() {
                              rwork_object <- serialize(self$rwork, connection = NULL)
                              self$con$SET('rwork', rwork_object)
                              self$con$SET('update', 'TRUE')
                            }
                          ),
                          private = list(
                            id_generator = function(n = 1000) {
                              generate_letters <- do.call(paste0, replicate(5, sample(LETTERS, n, TRUE), FALSE))
                              paste0(generate_letters, sprintf('%04d', sample(9999, n, TRUE)), sample(LETTERS, n, TRUE))
                            },
                            
                            to_JSON = function (x, ...) {
                              jsonlite::toJSON(x, digits = 50, auto_unbox = TRUE, force = TRUE, 
                                               null = 'null', na = 'null', ...)
                            }
                          )
)


tq_client <- function(){
  R6_Task_QueueR$new()
}

###################################################################################
# Working example

# Define the class
tq <- tq_client()


# Create and queue tests
tq$queue_task(task = function(){1+1}, task_name = 'my-task')
tq$queue_task(task = function(){6+1}, task_name = 'my-task-4')

tq$queue_task(task=function(){
  library(rworker)
  Sys.sleep(5)
  task_progress(50) # 50% progress
  Sys.sleep(5)
  task_progress(100) # 100% progress
}, task_name='sleep-task')

# View currently queued tasks
tq$tasks_list

# Send tasks to Redis server 
tq$send_tasks()


# Get task results
tq$task_result('my-task')
tq$task_result('my-task-2')


###################################################################################
# Dash App WIP example/test - NOT WORKING AT THE MOMENT, BEING REWORKED

app <- Dash$new()

app$layout(htmlDiv(list(
  htmlP('Task: Find the sum of two numbers.'),
  dccInput(id='input-1', type='number', value = 5),
  dccInput(id='input-2', type='number', value = 2),
  htmlButton(id='submit', children = 'Queue Task', n_clicks = 0),
  htmlButton(id='consume', children = 'Consume Tasks', n_clicks = 0),
  htmlDiv(id='tasks'),
  htmlDiv(id='results'),
  
  dccStore(id='store'),
  # dccInterval(id = 'interval', interval = 5*1000, n_intervals = 0),
  htmlDiv(id='trash')
)))


app$callback(
  output(id='tasks', property='children'),
  params = list(
    input(id='submit', property = 'n_clicks'),
    state(id='input-1', property = 'value'),
    state(id='input-2', property = 'value')
  ),
  
  queue_task <- function(n_clicks, input1, input2) {
    if(n_clicks > 0) {
      task_name = sprintf('sum_of_%s_and_%s', input1, input2)
      a = input1
      b = input2
      tq$queue_task(task = function(){a + b}, task_name = task_name)
    }
    return(names(tq$tasks_list))
  }
)

app$callback(
  output(id = 'trash', property = 'children'),
  params = list(
    input(id = 'consume', property = 'n_clicks')
  ),
  consume_tasks <- function(n_clicks){
    if (n_clicks > 0) {
      tq$consume()
      return('Consuming')
    }
    return('Standing By')
  }
)


# app$callback(
#   output(id = 'store', property = 'data'),
#   params = list(
#     input(id = 'interval', property = 'n_intervals')
#   ),
#   update_store <- function(n){
#     results <- c()
#     
#     for (n in names(tq$tasks_list)){
#       task_name <- paste0('celery-task-meta-', n)
#       results <- c(results, tq$task_result(task_name))
#     }
#     
#     return(results)
#   }
# )


# app$callback(
#   output(id= 'results', property = 'children'),
#   params = list(
#     input(id = 'store', property = 'data'),
#     input(id = 'interval', property = 'n_intervals')
#   ),
#   
#   update_results <- function(data, n){
#     print(data)
#     return(data[[1]])
#   }
# )



# app$run_server(showcase = TRUE, debug = FALSE)












