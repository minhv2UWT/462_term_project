import pandas as pd
import glob
import shutil
import os

# Get a list of all Parquet files in the directory
parquet_files = glob.glob('/Users/aryan/Documents/tcss462/proj/462_term_project/*.parquet')
destination = "/Users/aryan/Documents/tcss462/proj/462_term_project/"

# Ensure the destination directory exists
os.makedirs(destination, exist_ok=True)

# Loop through each Parquet file
for file in parquet_files:
    # Read the Parquet file into a DataFrame
    df = pd.read_parquet(file)

    # Convert the DataFrame to CSV
    csv_file = file.replace('.parquet', '.csv')
    df.to_csv(csv_file, index=False)

    # Move the CSV file to the destination directory
    shutil.move(csv_file, os.path.join(destination, os.path.basename(csv_file)))

print("Conversion and moving of files complete.")
