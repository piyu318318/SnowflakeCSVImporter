import csv
import snowflake.connector
import configparser

config = configparser.ConfigParser()
config.read('SnowflakeConfig.properties')
sf_account = config.get('snowflake', 'sf_account')
sf_user = config.get('snowflake', 'sf_user')
sf_password = config.get('snowflake', 'sf_password')
sf_warehouse = config.get('snowflake', 'sf_warehouse')
sf_database = config.get('snowflake', 'sf_database')
sf_schema = config.get('snowflake', 'sf_schema')
sf_table = config.get('snowflake', 'sf_table')
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

def read_csv_and_prepare_data(csv_file_path):
    data_batch = []
    try:
        with open(csv_file_path, mode="r") as file:
            reader = csv.DictReader(file)
            for row in reader:
                employee_data = (
                    int(row["Employee ID"]),
                    row["First Name"],
                    row["Last Name"],
                    row["Email"],
                    row["Phone Number"],
                    row["Hire Date"],
                    row["Department"],
                    row["Position"],
                    int(row["Salary"]),
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
    conn = connectToSnowflake()
    cur = conn.cursor()
    csv_file_path = 'employee_data.csv'
    data_batch = read_csv_and_prepare_data(csv_file_path)
    insert_data_into_snowflake(cur, data_batch)
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
