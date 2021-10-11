
# **What is the purpose of this Project?**

The purpose of this Database is to have a well-designed schema that helps to make use of the sas data in analysing US immigration inbound travellers data for the Month of April 2016. . Having a dataset in form of a Postgres database tables will allow us to analyze travellers coming to the US and to get data reports to analyse travellers behaviour. Another purpose of this database is to have an ETL pipeline that helps inserting new data to the tables so that we have an updated table with new data on inbound trafic to US each time we run the ETL pipeline.


# **How are we going to do it?**

Database schema design is a Snowflake-Schema; here we have one fact-table called immigrations and four dimension tables which are race, demographics, airlines and temperature. This allows us to have denormalized tables to run simplified queries that require aggregation of data. Also, the steps developed in the ETL pipeline help to process the CSV metadata of the Inbound traffic activity logs and demographics, airlines and temperature data, and to replicate it into our fact and dimension tables. As the inbound traffic on different US port of enteries get generated frequently, the need of an ETL pipeline is extremely important in our case as it enables us to have accurate data every time when we run queries.


# **ETL Process:**

The ETL process is defined in the etl.py file. The process involves the following steps:

## For Fact Table (immigrations)
1. In The first step, the port_codes.csv file is read to a data frame and rows are dropped with code as null value.
2. The location column from the ports_df is split into two columns namely city and state.
3. The parquet files which has already been created from the original sas file using spark.write.parquet is read into a new data frame.
4. For null values in i94mode, the nulls are replaced by code 9 which means "mode not reported."
5. Then the port_df and the new data frame are merged to add a new city column to our new data frame.
6. The data type of arrdate collumn is converted to YYYYMMDD Format.
7. Then the new data frame rows are added to the immigrations table.

## For Dimension Tables (race, temperature, demographics, airlines )

**Race**
1. Demographics csv file is read in a data frame.
2. The dataframe is then aggregated on State, State Code and Race, and the Count collumn is then added to get total no. of different races. 
3. All the unique values of the Race are selected and then added as columns with their corresponding race count values for a specific state.
4. Then the new data frame rows are added to the race table.


**Demographics**
1. The race and count columns are removed as they are not needed in this table   
2. Duplicated values are removed.
3. A Unique column for indexing is added called City id
4. Then the new data frame rows are added to the demographics table.


**Airlines**
1. Airlines.csv data is read to a data frame.
2. The rows where iata column is not null are selected
3. IATA, Airline ID, Name, Country and Active columns are filtered out.
4. The rows with active flag 'Y' are selected.
5. All rows with duplicate IATA Code are dropped
6. Then the new data frame rows are added to the airlines table.

**Temperature**
1. Temperature csv is read into the dataframe 
2. Data only for United States is filtered out.
3. Data with non null values of Average Temperature is selected.
4. Data on dt and city is aggregated to get average values of temperature  and temperature uncertainity
5. Then the new data frame rows are added to the temperatur table.




# The choice of tools, technologies, and data model.

**Postgres**:

- Having a dataset in form of a Postgres database tables will allow us to analyze travellers arrival activity to the United States and to get data reports relevant to the goverment.
- Postgres Database allows us to have denormalized tables to run simplified queries that require aggregation of data. Also, the steps developed in the ETL pipeline help to process the csv metadata of the traveller activity logs and state Demographics, Airlines and Temperature data, and to replicate it into our fact and dimension tables. As the new traveller activity logs and temperature get generated frequently, the need of an ETL pipeline is extremely important in our case as it enables us to have accurate data every time when we run queries.

**Data Model**:

- A *snow-flake schema* database model is being used as it helps us to store the demographics data in two tables race and state demographics as it helps us to avoid redundant data in a one single demographics table. This allows us to have two simpler dimension tables for faster SQL queries. 
- We have our main fact table immigrations as a transaction fact table wehere the fact data related to arrival events in US is organised and is in centre of the schema. And the rest dimension tables include temperature data for arrival dates and airline information in airlines table.

**Pandas**:

- Pandas forms a core component of our ETL pipeline. The features provided by pandas is best suited for the data analysis we need to do on our data. Data frames that we used help us to easily represent data in a concise manner. For example reading the csv file here takes one line of code, while doing it JAVA/C/C++ would require multi-lines of code.
- The data frames we have created provides us easy subsetting and filtering of the data, that are important part of data analysis.
- In addition pandas is built upon NumPy libraries and therefore we get performance benefits which can be seen in execute_values function when loading approximately 3 million data frame rows to the our postgres immigration sql table , as it took few minutes to load millions of rows to the table. Which can take hours otherwise to load that many rows. The goal of the schema is to speed up read queries and analysis for massive amount of data. The primary key in all the tables helps us to eliminate any duplicate information. This makes writing the data to the table faster.

- Because all of the data connects through the fact table the multiple dimension tables are treated as one large table of information, you can see some queries run on the data for analysis in the test.ipynb notebook to shows how this model simplifies the process of puling reports and travel behaviours analysis.


# A logical approach to this project under the certain scenarios:

**The data was increased by 100x.**
- We can use Amazon RDS Aurora edition as it is compatible with PostgreSQL in place of a local postgres database. Amazon RDS with aurora edition has upto 3 times throughput of Postgres SQL.And upto 64 TB of auto scaling that allows you to Scale up within minutes in the cloud;
- It’s speed, featureset, and data integrity make it the perfect contender for the increased data scenario.
- Amazon has a new service called AWS Database Migration Service (DMS for short), which integrates beautifully with their RDS service, making migration a breeze without any downtime.

**The pipelines would be run on a daily basis by 7 am every day.**
- Running `create_table.py` file drops and create new tables and make it ready for new updated data to be inserted.
- We can reset the data using the `create_table.py` and then run the etl.py to fill in the tables with new updated data.
- We can test this functionality by changing thr parquet filepath argument in `etl.py` from 'sas_apr_16' with a new month sas file like 'sas_may_16'.
- After running the above step we will have the new updated data in our tables.


**The database needed to be accessed by 100+ people.**
- In this case our database needs a better read performance, AWS RDS provides read replicas to horizontally scale our database. Read replicas will allow us to create read-only copies that are synchronized with your master database.


# **Project Repository Files**

- ## *sql_queries.py*
This file has all the sql queries needed to define our fact and dimension tables.

- ## *create_tables.py*
This file helps us to reset and create our tables in the databse and imports the queries from the sql_queries.py file.

- ## *US Immigrations Analysis.ipynb*
This jupyter notebook provides a step by step break down of our etl process. And also has Data Quality checks steps.

- ## *etl.py*
This file is derived from the etl.ipynb file and is run to to extract data from all the csv and parquet files in data folder and load them into the tables.

- ## *test.ipynb*
This jupyter notebook shows tables with all the data loaded in them and allows us to run analytical queries on the data.



# **_How to Run this Project?_**
#### 1. Open **Terminal**.
#### 2. Make sure to run `pip install -U fastparquet` command as it installs fastparquet engine to read parquet data.
#### 3. Run *"python create_tables.py"* to create database and reset our tables - clearing any pre-existing data.
#### 4. Run *"python etl.py"* to extract and load data in our tables.
#### 5. Open **test.ipynb** and run the cells to view data in the table and run queries to analyse data.


# Analysing Data to Answer questions (Sample Queries)



### Which are the TOP FIVE Airlines with flights landed in Chicago on April 10th, 2016 ?

**Query**
SELECT COUNT(pa.name), 
    pa.name 
FROM (
    SELECT a.name 
    FROM immigrations i 
    JOIN airlines a 
    ON i.airline = a.iata  
    WHERE i.arrdate = '2016-04-10' AND i.city ='Chicago' 
    ) as pa 

GROUP BY 2 

ORDER BY 1 DESC 
LIMIT 5;

**Result**:

count    | name
---------|----------------
1164     |   United Airlines
592      |   American Airlines
319      |   British Airways
300      |   Scandinavian Airlines System
275      |   All Nippon Airways




### Caculating the percentage of Asian Community of the most visited U.S. state by Indian Citizens Country - `Code:'213'`

**Query**


SELECT d.state, 
    r.asian, 
    SUM(d.total_population) as total_pop, 
    r.asian / SUM(d.total_population)*100 as asian_pct 
    
FROM (
    SELECT i94addr state_code, 
    COUNT(i94cit) as visits 
    FROM immigrations 
    WHERE i94cit = '213' 
    GROUP BY 1 
    ORDER BY 2 DESC 
    LIMIT 1
    ) as mvs 

JOIN race r 

ON r.state_code = mvs.state_code 

JOIN demographics d 

ON d.state_code = r.state_code 

GROUP BY 1,2 

ORDER BY 4 DESC;

**Result**

state    | asian       |total_pop   | asian_pct
---------|-------------|------------|-----------------
1164     | 4543730.0   |24822460.0  | 18.3049141785303