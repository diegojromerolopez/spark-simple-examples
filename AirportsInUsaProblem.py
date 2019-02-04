from pyspark.sql import SparkSession

if __name__ == "__main__":

    '''
    Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
    and output the airport's name and the city's name to out/airports_in_usa.text.

    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:
    "Putnam County Airport", "Greencastle"
    "Dowagiac Municipal Airport", "Dowagiac"
    ...
    '''

    spark = SparkSession.builder.master("local[3]").appName("Airports")\
                .getOrCreate()
    
    spark.sparkContext.setLogLevel('WARN')

    airports_df = spark.read.csv("in/airports.text", header=True)

    # Casting
    airports_df = airports_df.withColumn("Latitude", airports_df["Latitude"].cast("double"))
    airports_df = airports_df.withColumn("Longitude", airports_df["Longitude"].cast("double"))
    airports_df = airports_df.withColumn("Altitude", airports_df["Altitude"].cast("double"))

    # Airports in USA
    usa_airports_df = airports_df.filter(airports_df["Country"] == 'United States')

    print("Number of airports in World: {}".format(airports_df.count()))
    print("Number of airports in USA: {}".format(usa_airports_df.count()))

    usa_airports_df.toPandas().to_csv('airports_in_usa.csv', index=False)

    # Write in only one CSV file in directory airports_in_usa_csv
    # usa_airports_df.coalesce(1).write.csv('airports_in_usa_csv')
    # 
    # Write in several CSV files in directory airports_in_usa_csv
    # usa_airports_df.write.csv('airports_in_usa_csv')

    # Airports in contigous USA 
    northern_most_latitude = 48.987386
    southern_most_latitude = 18.005611
    western_most_longitude = -124.626080
    eastern_most_longitude = -62.361014

    airports_in_contiguos_usa = usa_airports_df.filter(
        (usa_airports_df["Latitude"] <= northern_most_latitude) & 
        (usa_airports_df["Latitude"] >= southern_most_latitude) & 
        (usa_airports_df["Longitude"] >= western_most_longitude) & 
        (usa_airports_df["Longitude"] <= eastern_most_longitude)
    )

    airports_in_contiguos_usa.toPandas().to_csv('airports_in_contigous_usa.csv', index=False)
    print("Number of airports in contigous USA: {}".format(airports_in_contiguos_usa.count()))

    # Airport at highest altitude by country
    airports_df.createOrReplaceTempView("airports")
    highest_airport_by_countries_df = spark.sql("""
        SELECT MAX(Altitude) AS max_altitude, `Main city` AS city, country
        FROM airports
        GROUP BY `Main city`, country
        ORDER BY max_altitude DESC 
    """)
    highest_airport_by_countries_df.show()
     