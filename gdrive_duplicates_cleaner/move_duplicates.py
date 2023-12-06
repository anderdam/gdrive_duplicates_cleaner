import os
import hashlib
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

DOWNLOADS_FOLDER = "/home/anderdam/gdrive_downloads"
DUPLICADOS_FOLDER = "/home/anderdam/drive_duplicados"

# Create spark session
spark = (
    SparkSession.builder.master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .appName("list_duplicates")
    .getOrCreate()
)


def move_duplicates(source_folder, target_folder):
    # Check if source folder exists, otherwise raise an exception
    if not os.path.exists(source_folder):
        raise Exception("Source folder does not exist")

    # Check if there are any files in the source folder
    list_of_files = os.listdir(source_folder)
    if not list_of_files:
        print("No files found in the source folder")
        exit()

    # Create target folder if it doesn't exist
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    # Create a Spark Dataframe from the file information
    file_infos = []
    for root, dirs, files in os.walk(source_folder):
        for file in files:
            full_path = os.path.join(root, file)
            filename = os.path.basename(file)
            size = os.path.getsize(full_path)

            # Calculate file hash using MD5
            hasher = hashlib.md5()
            with open(full_path, "rb") as fp:
                for chunk in iter(lambda: fp.read(4096), b""):
                    hasher.update(chunk)
            file_hash = hasher.hexdigest()

            file_infos.append((full_path, filename, size, file_hash))
    df = spark.createDataFrame(file_infos, ["full_path", "filename", "size", "hash"])

    duplicates = (
        df.groupBy("filename", "size", "hash")
        .agg(f.count("hash").alias("duplicates"))
        .where(f.col("duplicates") > 1)
        .orderBy(f.col("duplicates").desc())
        .collect()
    )

    # Identify duplicates using Spark SQL
    if len(duplicates) > 0:
        # Move duplicates to target folder
        c = 1
        for root, dirs, files in os.walk(source_folder):
            for duplicate_name in duplicates:
                for file in files:
                    if duplicate_name["filename"] == file:
                        duplicate_file_path = os.path.join(root, file)
                        shutil.move(duplicate_file_path, target_folder + f".dupe{c}")
                        print(
                            f"{c} - Moved {duplicate_file_path} to {target_folder}/{file}.dupe{c}"
                        )
                        c += 1
    else:
        print("No duplicates found")

    spark.stop()


if __name__ == "__main__":
    move_duplicates(source_folder=DOWNLOADS_FOLDER, target_folder=DUPLICADOS_FOLDER)
