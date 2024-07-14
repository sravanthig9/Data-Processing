# Data Anonymizer

This project contains a Python script to generate and anonymize large datasets. The script is designed to efficiently handle data generation and anonymization using multiple processes and the Dask library.
## Files

- `data_anonymize.py`: Script to generate and anonymize data in a CSV file.
- `Dockerfile`: Docker configuration to run the scripts.
- `requirements.txt`: List of dependencies.

# Usage

## Run in Local
1. Install the required libraries:
   ```sh
   pip install -r requirements.txt
2. Run the program
   ```sh
   python data_anonymize.py
- `Choose mode`:
- `default`: Generates a smaller dataset (default is 1 million records).
- `2GB`: Generates a larger dataset (approximately 2GB in size).
Enter Number of Records
If you choose the default mode, you will be prompted to enter the number of records. If you leave it blank, the default is 1 million records.

## Run in Docker
1. docker build -t data-anonymizer 
2. docker run -it --rm data-anonymizer

### Explanation
- `Data Generation`:Splits the data generation task into multiple processes to use the system's CPU capabilities and generates fake data using the faker library and writes it to CSV files in chunks.
- `Merging CSV Files`: Merges the generated CSV files into a single file.
- `Data Anonymization:`: 
1. Uses Dask to read the merged CSV file.
2. Anonymizes the data by replacing the first_name, last_name, and address fields with randomly generated values.
3. Writes the anonymized data to a new CSV file.

### Dask
I used Dask in this program because it is designed to handle large datasets efficiently. Dask provides parallel computing capabilities and allows for the processing of data that does not fit into memory. By using Dask, we can anonymize large datasets quickly and efficiently without running into memory limitations.


