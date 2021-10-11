# DROP TABLES

immigrations_table_drop = "DROP TABLE IF EXISTS immigrations"
ports_table_drop = "DROP TABLE IF EXISTS ports"
temperature_table_drop = "DROP TABLE IF EXISTS temperature"
race_table_drop = "DROP TABLE IF EXISTS race"
airlines_table_drop = "DROP TABLE IF EXISTS airlines"
demographics_table_drop = "DROP TABLE IF EXISTS demographics"

# CREATE TABLES

immigrations_table_create = ("""
CREATE TABLE IF NOT EXISTS immigrations(
cicid FLOAT PRIMARY KEY,
i94cit FLOAT, 
i94res FLOAT,
i94port VARCHAR,
city VARCHAR,
i94addr VARCHAR,
arrdate DATE,
i94mode FLOAT,
airline VARCHAR
)
""")

ports_table_create = ("""
CREATE TABLE IF NOT EXISTS ports (
i94port VARCHAR PRIMARY KEY,
city VARCHAR,
state_code VARCHAR
)
""")

temperature_table_create = ("""
CREATE TABLE IF NOT EXISTS temperature (
dt DATE,
city VARCHAR,
average_temperature FLOAT,
average_temperature_uncertainty FLOAT,
PRIMARY KEY (dt, city)
)
""")

race_table_create =("""
CREATE TABLE IF NOT EXISTS race(
state VARCHAR,
state_code VARCHAR PRIMARY KEY,
americanindian_and_alaskanative FLOAT,
asian FLOAT,
black_or_africanamerican FLOAT,
hispanic_or_latino FLOAT,
white FLOAT
)
""")

airlines_table_create =("""
CREATE TABLE IF NOT EXISTS airlines(
iata VARCHAR PRIMARY KEY,
airline_id INT,
name VARCHAR,
country VARCHAR,
active VARCHAR
)
""")

demographics_table_create =("""
CREATE TABLE IF NOT EXISTS demographics(
city_id INT PRIMARY KEY,
city VARCHAR,
state VARCHAR,
median_age FLOAT,
male_population FLOAT,
female_population FLOAT,
total_population FLOAT,
number_of_veterans FLOAT,
foreign_born FLOAT,
average_household_size FLOAT,
state_code VARCHAR
)
""")

# INSERT RECORDS

immigrations_table_insert = ("""
INSERT INTO immigrations
(
    cicid, i94cit, i94res, i94port, city, i94addr, arrdate, i94mode, airline 
)   
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)

""")

ports_table_insert = ("""
INSERT INTO ports
(
  i94port, city, state_code
)   
VALUES(%s, %s, %s)

""")


temperature_table_insert = ("""
INSERT INTO temperature
(
    dt, city, average_temperature, average_temperature_uncertainty
)   
VALUES(%s, %s, %s, %s)

""")

race_table_insert = ("""
INSERT INTO race
(
     state, state_code, americanindian_and_alaskanative, asian, black_or_africanamerican, hispanic_or_latino, white
)   
VALUES(%s, %s, %s, %s, %s, %s, %s)

""")

airlines_table_insert = ("""
INSERT INTO airlines
(
    iata, airline_id, name, country, active 
)   
VALUES(%s, %s, %s, %s, %s)

""")

demographics_table_insert = ("""
INSERT INTO demographics
(
    city_id, city, state, median_age, male_population, female_population, total_population, number_of_veterans, foreign_born, average_household_size, state_code
    
)   
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

""")


# QUERY LISTS

create_table_queries = [immigrations_table_create, ports_table_create, temperature_table_create, race_table_create, airlines_table_create, demographics_table_create]

drop_table_queries = [immigrations_table_drop, ports_table_drop, temperature_table_drop, race_table_drop, airlines_table_drop, demographics_table_drop]