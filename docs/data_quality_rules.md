\# Data Quality Rules



Based on profiling yellow\_tripdata\_2024-01.parquet (2,964,624 rows).



\## Rows rejected (removed from the main pipeline)

| Rule | Reason |

|---|---|

| dropoff time < pickup time | Impossible trip |

| pickup outside the file's month | Wrong period or bad clock |

| trip\_distance <= 0 | Cancelled or faulty trip |

| fare\_amount <= 0 | Refund, void or dispute |

| total\_amount <= 0 | Refund, void or dispute |



Rejected rows are saved separately so we can count and review them.



\## Rows kept and fixed

| Issue | Fix |

|---|---|

| RatecodeID is NULL | Set to 99 (unknown) |

| passenger\_count is 0 | Set to NULL |

| passenger\_count is NULL | Keep as NULL |

| congestion\_surcharge / Airport\_fee NULL | Keep as NULL |

| payment\_type = 0 (flex fare) | Keep, add code 0 to dim\_payment\_type |



\## Added columns

\- trip\_duration\_minutes = (dropoff - pickup) in minutes

\- pickup\_hour = hour of the pickup time

\- source\_file = name of the Parquet file

