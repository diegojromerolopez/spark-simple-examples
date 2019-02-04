from pyspark.sql import SparkSession

if __name__ == "__main__":
    '''
    "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
    "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
    Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
    Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

    Example output:
    vagrant.vf.mmc.com
    www-a1.proxy.aol.com
    .....    

    Keep in mind, that the original log files contains the following header lines.
    host    logname    time    method    url    response    bytes

    Make sure the head lines are removed in the resulting RDD.
    '''
    spark = SparkSession.builder.master("local[3]").appName("SameHostLogs").getOrCreate()
    
    spark.sparkContext.setLogLevel('WARN')

    nasa_07_df = spark.read.csv("in/nasa_19950701.tsv", header=True, sep='\t')
    nasa_08_df = spark.read.csv("in/nasa_19950801.tsv", header=True, sep='\t')
    
    nasa_df = nasa_07_df.union(nasa_08_df)
    
    nasa_df.createOrReplaceTempView("nasa_logs")

    hosts_df = spark.sql("""
        SELECT DISTINCT(host)
        FROM nasa_logs
    """)

    hosts_df.show()
    
    hosts_df.toPandas().to_csv('nasa_1995_hosts.csv', index=False)
