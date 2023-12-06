import os
import hashlib
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

DOWNLOADS_FOLDER = "/home/anderdam/gdrive_downloads"

# Create spark session


def move_duplicates(source_folder):
    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .appName("list_duplicates")
        .getOrCreate()
    )
    # Check if source folder exists, otherwise raise an exception
    if not os.path.exists(source_folder):
        raise Exception("Source folder does not exist")

    # Check if there are any files in the source folder
    list_of_files = os.listdir(source_folder)
    if not list_of_files:
        print("No files found in the source folder")
        exit()

    # Create a Spark Dataframe from the file information
    file_infos = []
    for root, dirs, files in os.walk(source_folder):
        for file in files:
            full_path = os.path.join(root, file)
            filename = os.path.basename(file)
            # size = os.path.getsize(full_path)
            # Calculate file hash using MD5
            hasher = hashlib.md5()
            with open(full_path, "rb") as fp:
                for chunk in iter(lambda: fp.read(4096), b""):
                    hasher.update(chunk)
            file_hash = hasher.hexdigest()

            file_infos.append((full_path, filename, file_hash))
    df = spark.createDataFrame(file_infos, ["full_path", "filename", "hash"])

    duplicates = (
        df.groupBy("filename", "hash")
        .agg(f.count("hash").alias("duplicates"))
        .where(f.col("duplicates") > 1)
        .orderBy(f.col("duplicates").desc())
    )

    # Identify duplicates using Spark SQL
    duplicates.show(truncate=False)
    duplicates.select(f.sum("duplicates")).show()

    spark.stop()


if __name__ == "__main__":
    move_duplicates(source_folder=DOWNLOADS_FOLDER)
