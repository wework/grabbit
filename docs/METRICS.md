# Metrics

grabbit exposes and reports the following metrics to Prometheus

|  Namespace    | Subsystem     | Name                              | Description                                                                 |
| ------------- | ------------- | ----------------------------------| --------------------------------------------------------------------------- |
| grabbit       | handlers      | [name of message handler]_result  | records and counts each successful or failed execution of a message handler |
| grabbit       | handlers      | [name of message handler]_latency | records the execution time of each handler                                  |
| grabbit       | handlers      | result                            | records and counts each run of a handler, having the handler's name, message type and the result as labels|
| grabbit       | handlers      | latency                           | records the execution time of each run of a handler, having the handler's name, message type as labels|
| grabbit       | messages      | rejected_messages                 | increments each time a message gets rejected                                |
| grabbit       | saga          | timedout_sagas                   | counting the number of timedout saga instances                              |
