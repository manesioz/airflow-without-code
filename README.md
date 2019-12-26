# airflow-without-code
Dynamically generate DAGs to run SQL files and ingest into BigQuery with one line of "code" at the header. 


### Example usage 

Given the directory: 


    .
    ├── ...
    ├── etl                          # ETL directory 
    │   ├── sql_script_1.sql         # first sql file to ingest into BigQuery
    │   ├── sql_script_2.sql         # second sql file to ingest into BigQuery
    │   └── airflow_without_code.py  # airflow-without-code main python file
    └── ...


Now you can easily manage simple `DAGs` for both `sql_script_1.sql` and `sql_script_2.sql` by including the headers: 

```sql
--{"schedule_interval": "@weekly", "author": "Zachary Manesiotis", "catchup": true, "destination_table": "project.dataset.table"} 

with fake_sql as (
    select col 
      from table
)
...
```
