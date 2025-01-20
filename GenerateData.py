import csv
import random
from faker import Faker

fake = Faker()

def generateEmployeeData(num_rows):
    employee_data = []
    for _ in range(num_rows):
        employee = {
            "Employee ID": random.randint(1000, 9999),
            "First Name": fake.first_name(),
            "Last Name": fake.last_name(),
            "Email": fake.email(),
            "Phone Number": fake.phone_number(),
            "Hire Date": fake.date_this_decade(),
            "Department": random.choice(["HR", "Finance", "Engineering", "Sales", "Marketing"]),
            "Position": random.choice(["Manager", "Engineer", "Analyst", "Sales Rep", "Director"]),
            "Salary": random.randint(50000, 150000),
        }
        employee_data.append(employee)
    return employee_data

employee_data = generateEmployeeData(100000)
csv_file_path = "employee_data.csv"

with open(csv_file_path, mode="w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=employee_data[0].keys())
    writer.writeheader()
    writer.writerows(employee_data)

print(f"CSV file with 100,000 employee records has been generated at {csv_file_path}")
