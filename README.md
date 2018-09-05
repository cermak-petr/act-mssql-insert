# act-mssql-insert

Apify act for inserting crawler results into a remote MSSQL table.

This act fetches all results from a specified Apifier crawler execution and inserts them into
a table in a remote MSSQL database.

The act does not store its state, i.e. if it crashes it restarts fetching all the results.
Therefore you should only use it for executions with low number of results.


**INPUT**

Input is a JSON object with the following properties:

```javascript
{
    "_id": "YOUR_EXECUTION_ID",
    "data": {
        "connection": "MSSQL_CONNECTION_STRING",
        "table": "DB_TABLE_NAME"
    }
}
```

__The act can be run with a crawler finish webhook, in such case fill just the contents of data 
attribute into a crawler finish webhook data.__

Alternatively you can directly specify the rows to be inserted (i.e. not fetching them from crawler execution).
```javascript
{
    // rows to be inserted
    "rows": [
        {"column_1": "value_1", "column_2": "value_2"},
        {"column_1": "value_3", "column_2": "value_4"},
        ...
    ],

    // MSSQL connection credentials
    "data": "connection_credentials"
}
```
