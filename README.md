# hbase-javaAPI

This java file is written for the purpose of an assignment. In the assignment, we are to compare the tools of big data such as Hive, Pig and Spark.
The comparisons of the tools are in their runtime of the queries on the data.
Since HBase could not run scripts like SQL, a java application is needed to run and process the data.

This java application is written to query the following SQL query on a HBase table
```
  SELECT country, SUM(sales) AS total_sales
  FROM superstore
  GROUP BY country
  ORDER BY total_sales DESC
  LIMIT 1;
```
