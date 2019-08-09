# Metrics

grabbit exposes and reports operational the following metrics to Prometheus

|  Namespace    | Subsystem     | Name                              | Description                                                                 |
| ------------- | ------------- | ----------------------------------| --------------------------------------------------------------------------- |
| grabbit       | handler       | [name of message handler]_result  | records and counts each succesfull or failed execution of a message handler |
| grabbit       | handler       | [name of message handler]_latency | records the execution time of each handler                                  |
| grabbit       | messages      | rejected_messages                 | increments each time a message gets rejected                                |
| grabbit       | saga          | timedout_sagas                   | counting the number of timedout saga instances                              |
