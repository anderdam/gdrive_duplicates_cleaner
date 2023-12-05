import os
import shutil
import re
import tkinter
import json
import os
import pandas as pd
import distutils
import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from tkinter import filedialog

# Create spark session
spark = (
    SparkSession.builder.master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .appName("list_duplicates")
    .getOrCreate()
)

logging.basicConfig(filename="app.log", level=logging.INFO)
logging.info("Starting duplicate detection")

source_folder = "/home/anderdam/gdrive_downloads"
target_folder = "/home/anderdam/drive_duplicados/"

if not os.path.exists(source_folder):
    raise Exception("Source folder does not exist")

if not os.path.exists(target_folder):
    os.makedirs(target_folder)

files_dict = {
    "paths": [],
    "names": [],
    "sizes": [],
    "hashes": [],
}

# Recursively search source folder
for root, dirs, files in os.walk(source_folder):
    for file in files:
        full_dir = os.path.join(root, file)
        split_dir = full_dir.split("/")

        full_path = "/".join(split_dir[0:-1])
        filename = split_dir[-1]
        size = os.path.getsize(os.path.join(root, file))
        file_hash = hash(
            (os.path.basename(file), os.path.getsize(os.path.join(root, file)))
        )

        files_dict["paths"].append(full_path)
        files_dict["names"].append(filename)
        files_dict["sizes"].append(size)
        files_dict["hashes"].append(file_hash)

pd.set_option("display.max.colwidth", None)
df = pd.DataFrame(data=files_dict)

ds = spark.createDataFrame(data=df)

duplicados = (
    (
        ds.groupBy("names", "sizes", "hashes")
        .agg(f.count("hashes").alias("duplicates"))
        .filter(f.col("duplicates") > 1)
        .orderBy(f.col("duplicates"))
    )
    .repartition("duplicates")
    .toPandas()
)
print(duplicados)
print(duplicados.size)
