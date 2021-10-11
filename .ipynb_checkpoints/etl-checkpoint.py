import psycopg2
import pandas as pd
from sql_queries import *
import os
import glob
import psycopg2.extras as extras



    

def demo_race_table(conn, filepath):
    """
    This function processes the demographics.csv, the path of which is provided in the filepath argument.
    It extracts the data from the csv file and transformed to pandas dataframe.
    The data is processed transformed and then stored into pandas dataframce race_df and cdemo_df.
    Then the data of data_frame is stored in race and demographics table.
        
    INPUTS
    * conn - conn variable for connecting to database
    * filepath - the path of the csv file for demograpics data set stored in data_sets
    """
    
    # Read in the data here
    demo_df = pd.read_csv(filepath, delimiter = ';')
    
    
    
    #Aggregating the dataframe on State, State Code and Race, and adding the Count collumn to get total no. of different races.
    
    agg_df=(demo_df.groupby(['State','State Code','Race'], as_index=False)
    .agg({'Count':'sum'})
    .rename(columns={'Count':'Race Count'})
    )
    
    
    
    #Selecting all the unique values of the Race and adding them as columns with their corresponding race count values for a specific state
    pt = agg_df.pivot_table(values='Race Count', columns='Race', 
                            index=['State','State Code',])

    pt.columns.name=None

    race_df = pt.reset_index()

    race_df.rename(columns={'State Code': 'state_code', 'American Indian and Alaska Native': 'americanindian_and_alaskanative', 'Black or African-American': 'black_or_africanamerican', 'Hispanic or Latino':'hispanic_or_latino'}, inplace=True)

    
    
    
    #Removing the race and count columns as they are not needed in this table   
    cdemo_df = demo_df.drop(['Race', 'Count'], axis=1)
    
    #Sorting the data by City 
    cdemo_df.sort_values("City",inplace=True)

    #The data does contain some duplicate values which are being removed
    cdemo_df.drop_duplicates(inplace=True)
    
    #Adding a unique column for indexing and naming it City id
    cdemo_df.reset_index(level=0, inplace=True)
   
    cdemo_df.rename(columns={'index':'city_id', 'Median Age':'median_age', 'Male Population':'male_population','Female Population':'female_population', 'Total Population':'total_population' ,'Number of Veterans':'number_of_veterans' ,'Foreign-born':'foreign_born', 'Average Household Size':'average_household_size', 'State Code':'state_code'}, inplace=True )
    
    # adding Race_df data values to postgres sql table "race"
    execute_values(conn, race_df, 'race')
    
    print('***Race table processed. Waiting for the next table ... ')
    
    # adding cdemo_df data values to postgres sql table "race"
    execute_values(conn, cdemo_df, 'demographics')
    
    print('***Demographics table processed. Waiting for the next table ... ')
    
    
    
    
    
    
def air_table(conn, filepath):
    
    """
    This function processes the airlines.csv, the path of which is provided in the filepath argument.
    It extracts the data from the csv file and transformed to pandas dataframe.
    The data is processed transformed and then stored into pandas dataframes namely air_df.
    Then the data of data_frame is stored in race and airlines table.
        
    INPUTS
    * conn - conn variable for connecting to database
    * filepath - the path of the csv file for airlines data set stored in data_sets
    """
    
    # Reading the data here to a data frame
    airlines_df = pd.read_csv(filepath)
    
    #Only selecting the rows where iata column is not null
    ana_df = airlines_df[airlines_df['IATA'].notna()]

    #Selecting IATA, Airline ID, Name, Country and Active columns for our table
    iata_df = ana_df[['IATA','Airline ID','Name','Country','Active']]

    #Filtering the data for only the active flights with active flag 'Y'
    af_df=iata_df.loc[iata_df['Active'] == 'Y']
    
    #Droping all rows with duplicate IATA Code to avoind any descrepancies in our data.
    air_df = iata_df.drop_duplicates(subset='IATA', keep=False,)

    # renaming the column
    air_df.rename(columns={'Airline ID':'airline_id'}, inplace=True )
    
    # Moving rows to sql table
    execute_values(conn, air_df, 'airlines')
    
    print('Airlines table Processed. Waiting for the next table ... ')
    
    
    
    
    
def temp_table(conn, filepath):
    
    """
    This function processes the temperature data, the path of which is provided in the filepath argument.
    It extracts the data from the csv file and transformed to pandas dataframe.
    The data is processed transformed and then stored into pandas dataframes namely gtemp_df.
    Then the data of data_frame is stored in race and temperature table. 
    
    The process follows below steps:
    1. Filtering out data only for United States
    2. Selecting data with non null values of Average Temperature
    3. aggregating data on dt and city for average values of temperature  and temperature uncetainity to get unique aggregated collumns to avoid any duplicate values
        
    INPUTS
    * conn - conn variable for connecting to database
    * filepath - the path of the csv file for temperature data 
    """
    
    # Reading the data into the dataframe here
    temp_df = pd.read_csv(filepath)
    
    #Filtering out data only for United States
    ust_df= temp_df.loc[temp_df['Country'] == 'United States']
    temp_c = ust_df[['dt','AverageTemperature','AverageTemperatureUncertainty','City' ]]
    
    #Selecting data with non null values of Average Temperature
    avgt = temp_c[ust_df['AverageTemperature'].notna()]
    
    #ggregating data on dt and city for average values of temperature  and temperature uncetainity
    gtemp_df=(avgt.groupby(['dt','City'], as_index=False)
    .agg({'AverageTemperature':'mean','AverageTemperatureUncertainty':'mean'})
    )
    
    gtemp_df.rename(columns={'AverageTemperature':'average_temperature','AverageTemperatureUncertainty':'average_temperature_uncertainty'}, inplace=True )
    
    
    # Loading data to postgres table
    execute_values(conn, gtemp_df, 'temperature')
    
    print('Temperature table processed. Waiting for the next table ... ')
    
    
    
    
    
    
def fact_table(conn, parquet_filepath):
    """
    This function gets the all the sas parquet files in from the path provided in filepath argument.
    The process follows high-level steps below:
    1. Created ports_df which has port codes with the city and state
    2. Data is read from the parquet file path passed in the argument
    3. Merging the data frame with the port table data frame to add a new column for city, Also droping the state column.
    4. Converting fomat of `arrdate` from SAS Format to date format
    
    INPUTS
    * conn - connection to the database
    * filepath -  the path of the directory that contains the json files
    """
    # reading the port codes file
    port_df = pd.read_csv('data_sets/port_codes.csv')
    
    #Dropunf the rows with null values for code
    port_df.dropna(subset =['Code'],inplace=True)
    
    # Splting the city name and state code in the location column
    df = port_df['Location'].str.split(',', n=1, expand=True)
    
    #Creating new columns for the splited values 
    port_df["City"] = df[0]

    port_df["State"] = df[1]
    
    # Droping the location column
    port_df.drop(columns=["Location"],inplace=True)
    
    #renaming the column name
    port_df.rename(columns={'Code': 'i94port', 'State': 'state_code', 'City': 'city'}, inplace=True)
    
    
    
    
    # reading the parquet file provided in the filepath argument
    label = pd.read_parquet(parquet_filepath, engine='fastparquet')
    
    # Selecting the columns needed for the table
    labelf = label [['cicid','i94cit', 'i94res', 'i94port', 'arrdate', 'i94mode' , 'i94addr', 'i94visa', 'airline']]
    
    # adding code 9 for the i94 mode where its values is null
    labelf["i94mode"].fillna("9.0", inplace = True)
    
    # merging port_Df and label_df to add city column in the immigration data frame
    merge = labelf.merge(port_df,how='left')
    immig_df=merge.drop(["state_code"],1)
    it_df = immig_df[['cicid','i94cit','i94res','i94port','city','i94addr','arrdate','i94mode','airline']]
    
    # Changing the arrdate from sas date format to the YYYYMMDD Format
    it_df['arrdate'] = pd.to_timedelta(it_df["arrdate"], unit='D') + pd.Timestamp('1960-1-1')
    print('The date format converted Sucessfully. Please wait for immigration data to process ... ')
    
    #Adding the data from data frame to immigration table
    execute_values(conn, it_df, 'immigrations')
    
    print('Immigrations table processed. Tables processing compeleted.')
    
    
    
def execute_values(conn, df, table):
    """
    This function defines the process to adding rows from a dataframe to sql tables.
    Using psycopg2.extras.execute_values() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()


    
    
    
    
def main():
    """
    This is the main function that defines the cursor variable and connection to the database.
    It connects to the databases using the database name and credentials.
    Then it runs the functions defined above for processing files and storing them in tables.
    Lastly it closes the connection to the database.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=immigrationsdb user=student password=student")
    cur = conn.cursor()
    
    demo_race_table(conn, filepath = 'data_sets/us-cities-demographics.csv')
    air_table(conn, filepath= 'data_sets/airlines.csv')
    temp_table(conn, filepath = '../../data2/GlobalLandTemperaturesByCity.csv')
    fact_table(conn, parquet_filepath = 'sas_apr_16')
    
    conn.close()


if __name__ == "__main__":
    main()