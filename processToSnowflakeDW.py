import csv
import snowflake.connector
import configparser
import boto3
import os

config = configparser.ConfigParser()
config.read('SnowflakeConfig.properties')

sf_account = config.get('snowflake', 'sf_account')
sf_user = config.get('snowflake', 'sf_user')
sf_password = config.get('snowflake', 'sf_password')
sf_warehouse = config.get('snowflake', 'sf_warehouse')
sf_database = config.get('snowflake', 'sf_database')
sf_schema = config.get('snowflake', 'sf_schema')
sf_table = config.get('snowflake', 'sf_table')

bucket_name = config.get('s3', 'bucket_name')
s3_file_path = config.get('s3', 's3_file_path')

s3_client = boto3.client('s3')


def connectToSnowflake():
    try:
        conn = snowflake.connector.connect(
            user=sf_user,
            password=sf_password,
            account=sf_account,
            warehouse=sf_warehouse,
            database=sf_database,
            schema=sf_schema
        )
        return conn
    except Exception as e:
        print(f"Error while connecting to Snowflake: {e}")
        raise


def download_csv_from_s3(bucket_name, s3_file_path, local_file_path):
    try:
        s3_client.download_file(bucket_name, s3_file_path, local_file_path)
        print(f"CSV file downloaded from S3 to {local_file_path}")
    except Exception as e:
        print(f"Error while downloading the file from S3: {e}")
        raise


def read_csv_and_prepare_data(csv_file_path):
    data_batch = []
    try:
        with open(csv_file_path, mode="r") as file:
            reader = csv.DictReader(file)
            for row in reader:
                employee_id = int(row["Employee ID"]) if row["Employee ID"] else None
                first_name = row["First Name"] if row["First Name"] else None
                last_name = row["Last Name"] if row["Last Name"] else None
                email = row["Email"] if "@" in row["Email"] else ""  # Empty string if '@' is missing
                phone_number = row["Phone Number"] if row["Phone Number"] else None
                hire_date = row["Hire Date"] if row["Hire Date"] else None
                department = row["Department"] if row["Department"] else None
                position = row["Position"] if row["Position"] else None
                salary = int(row["Salary"]) if row["Salary"] else None

                employee_data = (
                    employee_id,
                    first_name,
                    last_name,
                    email,
                    phone_number,
                    hire_date,
                    department,
                    position,
                    salary,
                )
                data_batch.append(employee_data)
        return data_batch
    except Exception as e:
        print(f"Error while reading the CSV file: {e}")
        raise


def insert_data_into_snowflake(cur, data_batch):
    insert_query = f"""
        INSERT INTO {sf_table} (EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, DEPARTMENT, POSITION, SALARY)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        cur.executemany(insert_query, data_batch)
        print(f"Successfully inserted {len(data_batch)} records into {sf_table} table.")
    except Exception as e:
        print(f"Error during batch insert: {e}")
        raise


def main():
    local_csv_file_path = '/tmp/employee_data.csv'
    download_csv_from_s3(bucket_name, s3_file_path, local_csv_file_path)
    conn = connectToSnowflake()
    cur = conn.cursor()
    data_batch = read_csv_and_prepare_data(local_csv_file_path)
    insert_data_into_snowflake(cur, data_batch)
    cur.close()
    conn.close()
    os.remove(local_csv_file_path)
    print(f"Temporary file {local_csv_file_path} has been deleted.")


if __name__ == "__main__":
    main()
