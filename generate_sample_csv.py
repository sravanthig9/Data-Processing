import csv
from faker import Faker

fake = Faker()

def generate_sample_csv(file_name, num_rows):
    """
    Generate a CSV file with sample data.
    """
    with open(file_name, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['first_name', 'last_name', 'address', 'date_of_birth'])
        for _ in range(num_rows):
            writer.writerow([fake.first_name(), fake.last_name(), fake.address().replace("\n", " ")])

if __name__ == "__main__":
    # Generate a sample CSV with 1 million rows
    generate_sample_csv('sample_data.csv', 1000000)
    print("Sample CSV file generated successfully.")
