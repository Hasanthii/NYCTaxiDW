\# Star Schema Design - NYC Taxi Data Warehouse



\## Business purpose

Analyse NYC Yellow Taxi trips to answer questions such as:

\- How do trips and revenue change by day, month and hour?

\- Which pickup zones and boroughs are busiest?

\- How do payment types affect tip amounts?



\## Grain

\*\*One row in `fact\_trip` = one taxi trip.\*\*



\## Fact table: `fact\_trip`

\*\*Foreign keys\*\*

\- pickup\_date\_key

\- vendor\_key

\- rate\_code\_key

\- payment\_type\_key

\- pickup\_location\_key

\- dropoff\_location\_key



\*\*Measures\*\*

\- passenger\_count

\- trip\_distance

\- fare\_amount

\- extra

\- mta\_tax

\- tip\_amount

\- tolls\_amount

\- improvement\_surcharge

\- congestion\_surcharge

\- airport\_fee

\- total\_amount

\- trip\_duration\_minutes (calculated from pickup and dropoff times)



\## Dimension tables

| Table | Description |

|---|---|

| dim\_date | Date, day, month, year, day of week, weekend flag |

| dim\_vendor | VendorID and vendor name |

| dim\_rate\_code | RatecodeID and its meaning (standard, JFK, Newark, etc.) |

| dim\_payment\_type | payment\_type and its meaning (credit card, cash, etc.) |

| dim\_location | Taxi zone ID, borough, zone name |



`dim\_location` is a \*\*role-playing dimension\*\*: it is used twice in

`fact\_trip`, once for the pickup location and once for the dropoff location.



\## Source

TLC Yellow Taxi Trip Records (Parquet files), starting with January 2024.

