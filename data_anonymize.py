import csv
import faker
import time
import os
import concurrent.futures
import multiprocessing
import dask.dataframe as dd
from dask.distributed import Client
import random

fake = faker.Faker()

# Function to generate data and write to a CSV file in chunks
def generate_data_part(file_id, records_per_process, batch_size):
    csv_filename = f'data_chunk_{file_id}.csv'
    with open(csv_filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=["first_name", "last_name", "address", "date_of_birth"])
        writer.writeheader()
        for i in range(0, records_per_process, batch_size):
            batch_data = [
                {
                    "first_name": fake.first_name(),
                    "last_name": fake.last_name(),
                    "address": fake.address().replace('\n', ', '),
                    "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=90)
                }
                for _ in range(min(batch_size, records_per_process - i))
            ]
            writer.writerows(batch_data)
            print(f"File {csv_filename}: {i + len(batch_data)} records written...")
    return csv_filename

# Function to combine multiple CSV files into a single CSV file
def combine_csv_files(process_count, final_output):
    with open(final_output, mode='w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=["first_name", "last_name", "address", "date_of_birth"])
        writer.writeheader()
        for i in range(process_count):
            with open(f'data_chunk_{i}.csv', mode='r') as infile:
                reader = csv.DictReader(infile)
                for row in reader:
                    writer.writerow(row)
            os.remove(f'data_chunk_{i}.csv')
    print(f"All chunk files merged into {final_output}")

# Function to calculate the optimal number of parallel processes based on CPU count
def calculate_process_count(utilization=0.7):
    cpu_total = multiprocessing.cpu_count()
    max_parallelism = cpu_total * 5  # 5 processes per CPU
    process_count = max(1, int(max_parallelism * utilization))
    print(f"Utilizing {process_count} parallel tasks across {cpu_total} CPUs (70% utilization).")
    return process_count

# Function to execute the data generation workflow
def execute_data_generation(record_count, execution_mode):
    process_count = calculate_process_count()
    records_per_process = record_count // process_count
    batch_size = 100000  
    print(f"Executing in {execution_mode} mode with {process_count} processes...")

    start_time = time.time()
    print("Generating data...")
    with concurrent.futures.ProcessPoolExecutor(max_workers=process_count) as executor:
        futures = [executor.submit(generate_data_part, i, records_per_process, batch_size) for i in range(process_count)]
        for future in concurrent.futures.as_completed(futures):
            print(f"Completed: {future.result()}")

    end_time = time.time()
    print(f"CSV files with {record_count} records created in {end_time - start_time:.2f} seconds.")

    output_filename = 'data_sample.csv' if execution_mode == 'default' else 'data_2gb.csv'
    combine_csv_files(process_count, output_filename)

    anonymize_csv_data(output_filename, f'anonymized_{output_filename}')

# Function to anonymize data in the CSV using Dask
def anonymize_csv_data(input_filename, output_filename):
    print(f"Starting anonymization for {input_filename}...")
    
    client = Client()

    ddf = dd.read_csv(input_filename)

    start_time = time.time()

    ddf['first_name'] = ddf['first_name'].apply(lambda x: f"Anon_{random.randint(1, 100000)}", meta=('x', 'object'))
    ddf['last_name'] = ddf['last_name'].apply(lambda x: f"Anon_{random.randint(1, 100000)}", meta=('x', 'object'))
    ddf['address'] = ddf['address'].apply(lambda x: f"Addr_{random.randint(1, 100000)}", meta=('x', 'object'))

    ddf.to_csv(output_filename, single_file=True, index=False)

    end_time = time.time()
    print(f"Anonymized data saved to {output_filename} in {end_time - start_time:.2f} seconds.")

# Main function to select mode and initiate the process
if __name__ == '__main__':
    mode = input("Select mode (default/2GB): ").strip().lower()

    if mode == '2gb':
        if os.path.exists('data_2gb.csv'):
            print("data_2gb.csv already exists. Using the existing file.")
            anonymize_csv_data('data_2gb.csv', 'anonymized_data_2gb.csv')
        else:
            print("2GB mode selected. This will take some time...")
            record_count = 30000000  
            execute_data_generation(record_count, mode)
    else:
        print("Default mode selected.")
        record_count = input("Enter the number of records (default is 1 million): ").strip()
        if not record_count.isdigit():
            record_count = 1000000
        else:
            record_count = int(record_count)
        execute_data_generation(record_count, mode)
