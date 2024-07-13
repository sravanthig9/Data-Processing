# Data Anonymizer

This project generates a large sample CSV file and anonymizes specified columns using Dask for distributed computing.

## Files

- `generate_sample_csv.py`: Script to generate a sample CSV file.
- `anonymize_data.py`: Script to anonymize data in the CSV file.
- `Dockerfile`: Docker configuration to run the scripts.
- `requirements.txt`: List of dependencies.

## Usage

### Run with Python

1. **Generate Sample Data**:
   ```sh
   python generate_sample_csv.py
