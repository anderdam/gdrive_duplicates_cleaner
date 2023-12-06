import hashlib
import os
import shutil
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from tqdm import tqdm
import subprocess

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


def downloader():
    file = "list.txt"

    if os.path.exists(file):
        os.remove(file)
        os.system("gdrive files list > list.txt")
    else:
        os.system("gdrive files list > list.txt")

    with open("list.txt") as txt:
        lines = [[item for item in line.split() if item] for line in txt]

    with open("list.csv", "w") as out:
        out.write("\n".join([",".join(line) for line in lines]))

    # Set paths
    download_path = DOWNLOADS_FOLDER
    # duplicados_path = constants.DUPLICADOS_FOLDER

    if not os.path.exists(download_path):
        os.makedirs(download_path)

    # Create a Spark Dataframe from the file information
    arquivos_gdrive = (
        spark.read.csv("list.csv", sep=",", header=True)
        .withColumnRenamed("Id", "id")
        .withColumnRenamed("Name", "name")
        .withColumnRenamed("Type", "type")
        .withColumnRenamed("Size", "dt_create")
        .withColumnRenamed("Created", "hr_create")
        .withColumn("dt_time_created", f.concat(f.col("dt_create"), f.col("hr_create")))
        .withColumn(
            "dt_time_created",
            f.to_timestamp(f.col("dt_time_created"), "yyyy-MM-ddHH:mm:ss"),
        )
        .drop("dt_create", "hr_create")
        # .select(f.col("Id"), f.col("Name"))
    )

    # Track total time
    start = time.time()

    file_download_times, file_download_sizes = [], []

    arquivos_gdrive.count()

    # Download files
    with tqdm(total=arquivos_gdrive.count()) as pbar:
        start_time = time.time()
        for row in arquivos_gdrive.collect():
            print(f" --> Downloading: {row.name}\n")
            command = f"cd {download_path}; gdrive files download --recursive {row.id}"
            subprocess.run(command, shell=True)
            end_time = time.time()

            # Track individual file download time and size
            download_time = end_time - start_time
            file_size = os.path.getsize(os.path.join(download_path, row.name))
            file_download_times.append(download_time)
            file_download_sizes.append(file_size)

            # Update progress bar
            pbar.update(1)

    # Calculate the total download size
    total_download_size = sum(file_download_sizes)

    end = time.time()
    total_time = end - start

    # Print the individual file download times and sizes
    print("Individual file download times:")
    for file_time in file_download_times:
        print(file_time)

    print("Individual file download sizes:")
    for file_size in file_download_sizes:
        print(file_size)

    # Print the total download time and size
    print(f"Total download time: {total_time}")
    print(f"Total download size: {total_download_size}")

    # Delete lists files
    os.remove("list.txt")
    os.remove("list.csv")


# Move duplicates to target folder
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
        for item in files:
            full_path = os.path.join(root, item)
            filename = os.path.basename(item)
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
        c = 0
        for root, dirs, files in os.walk(source_folder):
            for duplicate_name in duplicates:
                for file in files:
                    if duplicate_name["filename"] == file:
                        duplicate_file_path = os.path.join(root, file)
                        shutil.move(duplicate_file_path, target_folder + f".dupe{c}")
                        print(f"Moved {duplicate_file_path} to {target_folder}.dupe{c}")
                        c += 1
    else:
        print("No duplicates found")


if __name__ == "__main__":
    downloader()
    move_duplicates(DOWNLOADS_FOLDER, DUPLICADOS_FOLDER)
    spark.stop()
