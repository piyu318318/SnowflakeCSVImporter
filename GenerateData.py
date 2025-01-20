import csv
import random
from faker import Faker
import boto3
import configparser


config = configparser.ConfigParser()
config.read('SnowflakeConfig.properties')
bucket_name = config.get('s3', 'bucket_name')
s3_file_path = config.get('s3', 's3_file_path')

fake = Faker()
s3_client = boto3.client('s3')

def generateEmployeeData(num_rows):
    employee_data = []
    for _ in range(num_rows):
        employee = {
            "Employee_ID": random.randint(1000, 9999),
            "First_Name": fake.first_name(),
            "Last_Name": fake.last_name(),
            "Email": fake.email(),
            "Phone_Number": fake.phone_number(),
            "Hire_Date": fake.date_this_decade(),
            "Department": random.choice(["HR", "Finance", "Engineering", "Sales", "Marketing"]),
            "Position": random.choice(["Manager", "Engineer", "Analyst", "Sales Rep", "Director"]),
            "Salary": random.randint(50000, 150000),
        }
        employee_data.append(employee)
    return employee_data

# Generate employee data and write to CSV
employee_data = generateEmployeeData(100000)
csv_file_path = "employee_data.csv"

with open(csv_file_path, mode="w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=employee_data[0].keys())
    writer.writeheader()
    writer.writerows(employee_data)

print(f"CSV file with 100,000 employee records has been generated at {csv_file_path}")



try:
    with open(csv_file_path, "rb") as data:
        s3_client.upload_fileobj(data, bucket_name, s3_file_path)
    print(f"CSV file uploaded successfully to S3 bucket {bucket_name} at {s3_file_path}")
except Exception as e:
    print(f"Error: ",e)

