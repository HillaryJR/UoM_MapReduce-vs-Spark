import argparse
from pyspark.sql import SparkSession
import time


def execute_query(spark, query):
    """
    Executes a SQL query using the provided SparkSession.

    :param spark: The SparkSession.
    :param query: The SQL query to execute.
    :return: The DataFrame resulting from the query execution.
    """
    return spark.sql(query)

def calculate_carrier_delay(data_source, output_uri):
    """
    Processes airline data and writes the loop result to the output URI.

    :param data_source: Not used in this function.
    :param output_uri: The URI where output is written.
    """
    with SparkSession.builder.appName("Calculate Year-wise Carrier Delays").getOrCreate() as spark:

        # Load the airline data CSV
        if data_source is not None:
            flight_delay_df = spark.read.option("header", "true").csv(data_source)

        # Create an in-memory DataFrame to query
        flight_delay_df.createOrReplaceTempView("delay_flights")

        queries = [
            "SELECT Year, avg((CarrierDelay/ArrDelay)*100) as Year_wise_carrier_delay from delay_flights GROUP BY Year ORDER BY Year DESC",
            "SELECT Year, avg((NASDelay/ArrDelay)*100) as Year_wise_carrier_delay from delay_flights GROUP BY Year ORDER BY Year DESC",
            "SELECT Year, avg((WeatherDelay/ArrDelay)*100) as Year_wise_carrier_delay from delay_flights GROUP BY Year ORDER BY Year DESC",
            "SELECT Year, avg((LateAircraftDelay/ArrDelay)*100) as Year_wise_carrier_delay from delay_flights GROUP BY Year ORDER BY Year DESC",
            "SELECT Year, avg((SecurityDelay/ArrDelay)*100) as Year_wise_carrier_delay from delay_flights GROUP BY Year ORDER BY Year DESC"
        ]

        data = []
        for query in queries:
            query_row_data = [queries.index(query) + 1]  # Add query number
            for _ in range(5):  # Execute each query 5 times
                start_time = time.time()
                execute_query(spark, query)
                end_time = time.time()
                execution_time = end_time - start_time
                query_row_data.append(execution_time)
            data.append(query_row_data)
        
        # Define column names
        columns = ["Query_Number", "Time_Iteration_1", "Time_Iteration_2", "Time_Iteration_3", "Time_Iteration_4","Time_Iteration_5"]


        # Create a Spark DataFrame from the sample data
        spark_df = spark.createDataFrame(data, columns)

        # Write the results to the specified output URI
        spark_df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_source', help="Not used in this function")
    parser.add_argument('--output_uri', help="The URI where output is saved")
    args = parser.parse_args()

    calculate_carrier_delay(args.data_source, args.output_uri)
