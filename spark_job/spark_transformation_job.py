import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,count,avg,when,lit,expr
import logging
import sys


#Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getlogger(__name__)

def main(env, bq_project, bq_dataset, transformed_table, route_insights_table, origin_insights_table):
    try:
        #Intialiaze SparkSession
        spark = SparkSession.builder \
                .appName("FlightBookingAnalysis") \
                .config("spark.sql.catalogImplementation", "hive") \
                .getorCreate()
        
        logger.info("Spark Session initialized.")


        #Resolve GCS path based on enviroment
        input_path = f"-{env}"
        logger.info(f"Input path resolved: {input_path}")

        #Read the data from GCS
        data = spark.read.csv(input_path, header=True, inferSchema=True)
        logger.info("Data read from GCS")
        
        #Data transformations
        logger.info("Starting data transformations.")

        #Add Derived columns
        transformed_data = data.withColumn(
            "is_weekend", when(col("flight_day").isin("Sat","Sun"),lit(1)).otherwise(lit(0))
        ).withColumn(
            "lead_time_category", when(col("purchase_lead") < 7,lit("last-minute"))
                                  .when((col("purchase_lead") >= 7) & (col("purchase_lead") < 30), lit("Short_term"))
                                  .otherwise(lit("Long-Term"))
        ).withColumn(
            "booking_sucess_rate", expr("booking_complete / num_passengers")
        )

        #Aggregations for insights
        route_insights = transformed_data.groupBy("route").agg(
            count("*").alias("total_bookings"),
            avg("flight_duration").alias("avg_flight_duration"),
            avg("Length_of_stay").alias("avg_purchase_lead")
        )

        booking_origin_insights = transformed_data.groupBy("booking_origin").agg(
            count("*").alias("total_bookingfs"),
            avg("booking_success_rate").alias("success_rate"),
            avg("purchase_lead").alias("avg_purchase_lead")
        )
        logger.info("Data Transformation completed.")

        #Write transformed data to Bigquery
        logger.info(f"Writing transformed data to Bigquery table: {bq_project}:{bq_dataset}.{transformed_table}")
        transformed_data.write \
            .format("bigquery") \
            .option("table", f"{bq_project}:{bq_dataset}.{transformed_table}") \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()
        
        #Write route insights to Bigquery
        logger.info(f"Writing troute insights to Bigquery table: {bq_project}:{bq_dataset}.{route_insights_table}")
        route_insights.write \
            .format("bigquery") \
            .option("table", f"{bq_project}:{bq_dataset}.{route_insights_table}") \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()
        
        #Write booking origin insights to Bigquery
        logger.info(f"Writing booking origin insights to Bigquery table: {bq_project}:{bq_dataset}.{origin_insights_table}")
        booking_origin_insights.write \
            .format("bigquery") \
            .option("table", f"{bq_project}:{bq_dataset}.{origin_insights_table}") \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()
        
        logger.info("Data written to Bigquery successfully.")

    except Exception as e:
        logger.error(f"An error occured: {e}")
        sys.exit(1)

    finally:
        #Stop spark Session
        spark.stop()
        logger.info("Spark session stopped.")
        

if __name__ == "__main__":
    #Parse command lines arguments
    parser = argparse.ArgumentParser(description="Process flight booking data and write to Bigquery.")
    parser.add_argument("--env", required=True, help="Envirnoment (e. g. dev,prod)")
    parser.add_argument("--bq_project", required=True, help="Bigquery project ID")
    parser.add_argument("--bq_datasett", required=True, help="Bigquery dataset name")
    parser.add_argument("--tranformed_table", required=True, help="Bigquery table for transformed data")
    parser.add_argument("--route_insights_table", required=True, help="Bigquery table for route insights")
    parser.add_argument("--origin_insights_table", required=True, help="Bigquery table for booking origin insights")

    args = parser.parse_args()

    #çall the main function with parsed arguments

    main(
        env=args.env,
        bq_project=args.bq_project,
        bq_dataset=args.bq_dataset,
        transformed_table=args.transformed_table,
        route_insights_table=args.route_insights_table,
        origin_insights_table=args.origin_insights_table
    )




