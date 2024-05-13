# ETL Excel to SQL

This Python script performs an ETL (Extract, Transform, Load) process to read data from Excel files, process it, and load it into a PostgreSQL database.

## Dependencies

The script uses the following Python libraries:

- pandas
- numpy
- datetime
- os
- sqlalchemy

## How it works

The script consists of two main functions:

1. `data_folder()`: This function reads the data from the specified folder path, extracts the date from the folder names, and returns the path of the file that matches the current date.

2. `insert_data()`: This function reads the data from the file returned by `data_folder()`, performs some transformations (like dropping unnecessary columns, filtering rows, converting data types, etc.), and then loads the transformed data into a PostgreSQL database.

The script is designed to be run as a standalone script. When run, it calls the `insert_data()` function.

## Usage

To run the script, navigate to the directory containing the script in your terminal and run the following command:

```bash
python etl_exel_sql.py