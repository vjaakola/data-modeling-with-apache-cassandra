
# # Part I. ETL Pipeline for Pre-Processing the Files

# #### Import Python packages 


# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables


# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# uncomment the code below to get total number of rows 
#print(len(full_data_rows_list))
# uncomment the code below to check to see what the list of event data rows will look like
#print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length','level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# # Part II. Apache Cassandra coding portion of the project. 
# 
# ## The event_datafile_new.csv contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId


# #### Creating a Cluster


# Connection to a Cassandra instance local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()


# #### Create Keyspace

#Create a Keyspace 
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)


# #### Set Keyspace


# Set KEYSPACE to the keyspace specified above
try:
    session.set_keyspace('udacity')

except Exception as e:
    print(e)


# ### Create tables to run the following queries. 
# ## Create queries to ask the following three questions of the data
# 
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# 
# 
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
#     
# 
# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 


## Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4
query = "DROP TABLE IF EXISTS session_activity"
try:
    session.execute(query)
except Exception as e:
    print(e)

# Primary key: partition key sessionId and clustering key itemInSession
# Partition by sessionId and rows are ordered by itemInSession

query = "CREATE TABLE IF NOT EXISTS session_activity "
query = query + "(session_id int, item_in_session int, artist text, song_title text, song_length float, PRIMARY KEY (session_id, item_in_session))"

try:
    session.execute(query)
except Exception as e:
    print(e)


# Processing the CSV -file for Apache Cassandra
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## Assign the INSERT statements into the `query` variable
        query = "INSERT INTO session_activity (session_id, item_in_session, artist, song_title, song_length)"
        query = query + " VALUES (%s, %s, %s, %s, %s)"
        
        ## Assign which column element should be assigned for each column in the INSERT statement.
        session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))
        

#Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4

query = "select artist, song_title, song_length from session_activity WHERE session_id = 338 and item_in_session = 4"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.artist, row.song_title, row.song_length)


## Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182
query = "DROP TABLE IF EXISTS user_activity"
try:
    session.execute(query)
except Exception as e:
    print(e)


# Primary key: partition key user_id and session_id and clustering key itemInSession
# Partition by user_id and session_id rows are ordered by itemInSession
query = "CREATE TABLE IF NOT EXISTS user_activity "

query = query + "(user_id int, session_id int, item_in_session int, artist text, song_title text, first_name text, last_name text," 
query = query + " PRIMARY KEY (user_id, session_id, item_in_session))"

try:
    session.execute(query)
except Exception as e:
    print(e)
    
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
    ## Assign the INSERT statements into the `query` variable    
        query = "INSERT INTO user_activity (user_id, session_id, item_in_session, artist, song_title, first_name, last_name)"
        query = query + " VALUES (%s, %s, %s, %s, %s, %s, %s)"
        session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))
        
query = "select artist, song_title, first_name, last_name from user_activity WHERE user_id = 10 and session_id = 182" 

try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.artist, row.song_title, row.first_name, row.last_name)


##Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

query = "DROP TABLE IF EXISTS song_activity"
try:
    session.execute(query)
except Exception as e:
    print(e)

## Primary Key: partition key song_title and clustering key user_id.
## Partition by song_title and rows are ordered by the user_id.

query = "CREATE TABLE IF NOT EXISTS song_activity "
query = query + "(song_title text, user_id int, first_name text, last_name text," 
query = query + " PRIMARY KEY (song_title, user_id))"
try:
    session.execute(query)
except Exception as e:
    print(e)

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO song_activity (song_title, user_id, first_name, last_name)"
        query = query + " VALUES (%s, %s, %s, %s)"
        session.execute(query, (line[9], int(line[10]), line[1], line[4]))

query = "select first_name, last_name from song_activity WHERE song_title = 'All Hands Against His Own'"

try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.first_name, row.last_name)



##Drop the table before closing out the sessions

query = "DROP TABLE IF EXISTS session_activity"
try:
    session.execute(query)
except Exception as e:
    print(e)

query = "DROP TABLE IF EXISTS user_activity"
try:
    session.execute(query)
except Exception as e:
    print(e)

query = "DROP TABLE IF EXISTS song_activity"
try:
    session.execute(query)
except Exception as e:
    print(e)


# ### Close the session and cluster connection¶

# In[31]:


session.shutdown()
cluster.shutdown()



