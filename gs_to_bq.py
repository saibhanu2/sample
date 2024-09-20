import sys
import argparse

# import findspark  # TODO: remove this
#
# findspark.init()  # TODO: Remove this

from pyspark.sql import functions as sf
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import substring, count, col, current_date, sum, round, when, lit
from pyspark.sql.types import StringType, IntegerType, DoubleType


def trim_zipcodes(df):
    df = df.selectExpr("NPI", "cast(Zip_Code as string) Zip_Code")
    df = df.withColumn("Zip_Code", substring("Zip_Code", 1, 5))
    return df


def join_df(npi, medicare):
    joined = medicare.join(npi, on="NPI", how="left")
    joined = joined.selectExpr("NPI", "Brnd_Name", "Gnrc_Name", "Tot_Day_Suply", "Tot_Drug_Cst", "Tot_Benes", \
                               "Tot_30day_Fills", "Zip_Code", "Prscrbr_Type", "Tot_Clms")
    joined = joined.drop_duplicates()
    joined = joined.repartition(48)
    return joined


def process(df):
    df = df.withColumn("ZipCode", when(df.Zip_Code.isNotNull(), df.Zip_Code) \
                       .when(df.Zip_Code.isNull(), "ZipCode missing") \
                       .when(df.NPI.isNull(), "NPI missing")
                       )
    df = df.drop("Zip_Code")
    return df


def calculate_kpis(df):
    df.printSchema()
    df_agg1 = df.groupBy("Brnd_Name", "Gnrc_Name", "Prscrbr_Type", "ZipCode") \
        .agg(count("NPI").alias("No_of_distinct_providers"),
             sum("Tot_Clms").alias("Claims"), \
             sum("Tot_Day_Suply").alias("Day_Supply"), round(sum("Tot_Drug_Cst"), 2).alias("Cost"), \
             sum("Tot_Benes").alias("Beneficiaries"), round(sum("Tot_30day_Fills"), 2).alias("30D_fills")
             )

    return df_agg1


def calculate_kpis_bns(df):
    # For beneficiaries not supressed
    df_agg2 = df.where(col("Tot_Benes").isNotNull()) \
        .groupBy("Brnd_Name", "Gnrc_Name", "Prscrbr_Type", "ZipCode") \
        .agg(count("NPI").alias("No_of_distinct_providers_bns"),
             sum("Tot_Clms").alias("claims_bns"), \
             sum("Tot_Day_Suply").alias("Day_Supply_bns"), round(sum("Tot_Drug_Cst"), 2).alias("Cost_bns"), \
             sum("Tot_Benes").alias("Beneficiaries_bns"), round(sum("Tot_30day_Fills"), 2).alias("30D_fills_bns")
             )

    return df_agg2


def get_all_kpis(df1, df2):
    final = df1.join(df2, on=["Brnd_Name", "Gnrc_Name", "Prscrbr_Type", "ZipCode"], how="left")
    final = final.withColumn("ingestion_date", current_date()) \
                .withColumn("medicare_year", lit("2022"))

    final = final.withColumnRenamed("Brnd_Name", "Brand_Name") \
        .withColumnRenamed("Gnrc_Name", "Generic_Name")

    final.printSchema()
    return final


# TODO: To check for null condition
def get_npi_schema(npi):
    npi = npi.select("NPI", "Provider Business Practice Location Address Postal Code") \
        .withColumnRenamed("Provider Business Practice Location Address Postal Code", "Zip_Code") \
        .withColumn("NPI", col("NPI").cast(StringType()))

    return npi


# TODO: TO check for null condition
def get_medicare_schema(medicare):
    medicare = medicare.withColumnRenamed("Prscrbr_NPI", "NPI")
    medicare = medicare.select("Brnd_Name", "Gnrc_Name", "NPI", "Prscrbr_Type", "Tot_30day_Fills", \
                               "Tot_Benes", "Tot_Clms", "Tot_Day_Suply", "Tot_Drug_Cst")
    # medicare = medicare.na.drop(subset=["NPI"])
    medicare = medicare.withColumn("Tot_Clms", col("Tot_Clms").cast(DoubleType())) \
        .withColumn("Tot_Benes", col("Tot_Benes").cast(IntegerType())) \
        .withColumn("Tot_Day_Suply", col("Tot_Day_Suply").cast(DoubleType())) \
        .withColumn("Tot_Drug_Cst", col("Tot_Drug_Cst").cast(DoubleType())) \
        .withColumn("Tot_30day_Fills", col("Tot_30day_Fills").cast(DoubleType())) \
        .withColumn("NPI", col("NPI").cast(StringType()))

    return medicare


def get_extract_date(filename):
    return filename.split("/")[-1].split(".")[-2].split("-")[-1]


if __name__ == "__main__":
    # NPI_File = "../input/npidata_pfile_20050523-20240908.csv"
    # Medicare_File = "../input/medicare.json"  # gs://custom-bid/Medicare/medicare.csv
    # Output_File = "../output/med_v2/"  # gs://custom-bid/output/

    NPI_File = sys.argv[1]  # gs://custom-bid/NPI/npidata_pfile_20050523-20240512_10k.csv
    Medicare_File = sys.argv[2]  # gs://custom-bid/Medicare/medicare.csv
    Output_File = sys.argv[3]  # gs://custom-bid/output/
    staging_bucket = Output_File

    print("NPI is", NPI_File)
    print("Medicare is:", Medicare_File)
    print("Output is:", Output_File)
    # print("Job is...", sys.argv[0])

    print("Args: --- ", NPI_File, Medicare_File, Output_File)
    # TODO: Partition data
    extract_date = get_extract_date(NPI_File)

    # conf = SparkConf().setAppName("Spark-demo") \
    #     .setMaster("local[*]")  # TODO: Change here to "yarn"
    conf = SparkConf().setAppName("Spark-demo") \
        .setMaster("yarn")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    # spark = SparkSession.builder \
    #     .master("yarn") \
    #     .appName("spark-demo") \
    #     .config("spark.jars.packages", "com.google.cloud.spark:spark-3.5-bigquery:0.41.0") \
    #     .getOrCreate()
    # spark.conf.set('temporaryGcsBucket', staging_bucket)

    df_npi = spark.read.format("csv").option("inferSchema", "true").option("header", "true") \
        .option("path", NPI_File).load()  # npi

    df_npi = get_npi_schema(df_npi)

    df_npi = trim_zipcodes(df_npi)
    df_npi.printSchema()

    df_medicare = spark.read.format("csv").option("inferSchema", "true").option("header", "true") \
        .option("path", Medicare_File).load()

    df_medicare = get_medicare_schema(df_medicare)
    df_medicare.printSchema()

    df_medicare.printSchema()

    df_joined = join_df(df_npi, df_medicare)
    df_joined = process(df_joined)

    df_joined.printSchema()
    df_kpis = calculate_kpis(df_joined)

    df_kpis_bns = calculate_kpis_bns(df_joined)
    df_final = get_all_kpis(df_kpis, df_kpis_bns)
    df_final = df_final.withColumn("NPI_extract_date", lit(extract_date))

    # df_final.repartition(1).write.option("header", True). \
    #     mode("overwrite").csv(Output_File + "medicare_KPIs_v2.csv")

    df_final.write.format('bigquery') \
            .option('table', 'google_atf.medicare_KPIs_v3') \
            .option("temporaryGcsBucket", staging_bucket) \
            .mode("append") \
            .save()
