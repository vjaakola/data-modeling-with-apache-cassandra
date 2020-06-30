This project is part of Udacity's Data Engineer Nanodegree program.

### Modeling NoSQL database or Apache Cassandra database

Project steps:
1. Design tables to answer the queries outlined in the project template
2. Write Apache Cassandra ``CREATE KEYSPACE`` and ``SET KEYSPACE`` statements
3. Develop ``CREATE`` statement for each of the tables to address each question
4. Load the data with ``INSERT`` statement for each of the tables
5. Include ``IF NOT EXISTS`` clauses in your ``CREATE`` statements to create tables only if the tables do not already exist. Include ``DROP TABLE`` statement for each table for to run drop and create tables whenever needed, to reset the database and test ETL pipeline
6. Test by running the proper select statements with the ``WHERE`` clause


### Build ETL Pipeline
1. Implement the logic in section Part I to iterate through each event file in ``event_data`` to process and create a new CSV file in Python
2. Make necessary edits to Part II to include Apache Cassandra ``CREATE`` and ``INSERT`` statements to load processed records into relevant tables in your data model
3. Test by running ``SELECT`` statements after running the queries on the database


### Author
Vesa Jaakola

### Acknowledgements
Must give credit to Udacity
