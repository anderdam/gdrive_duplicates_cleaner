import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from tqdm import tqdm
import subprocess
import pandas as pd
import constants

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
download_path = constants.DOWNLOADS_FOLDER
# duplicados_path = constants.DUPLICADOS_FOLDER

if not os.path.exists(download_path):
    os.makedirs(download_path)

# Create spark session
spark = (
    SparkSession.builder.master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .appName("list_duplicates")
    .getOrCreate()
)

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
