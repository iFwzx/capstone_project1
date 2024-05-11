import os
import requests
import snowflake.connector as sf
import toml

def lambda_handler(event, context):
    # Load configuration settings from a TOML file.
    proj_config = toml.load('config.toml')
    
    # Retrieve project-specific URLs and path settings from the configuration.
    url = proj_config['url']
    destination_folder = proj_config['destination_folder']
    file_name = proj_config['file_name']
    local_file_path = proj_config['local_file_path']
    stage_name = proj_config['stage_name']

    # Fetch environment variables for Snowflake database credentials.
    account = os.environ['account']
    warehouse = os.environ['warehouse']
    database = os.environ['database']
    schema = os.environ['schema']
    table = os.environ['table']
    user = os.environ['user']
    password = os.environ['password']
    role = os.environ['role']
    
    # Download the file from the provided URL and check for any errors during the request.
    response = requests.get(url)
    response.raise_for_status()
    
    # Save the downloaded file to the specified local path.
    file_path = os.path.join(destination_folder, file_name)
    with open(file_path, 'wb') as file:
        file.write(response.content)
    
    # Read and print the contents of the file for verification.
    with open(file_path, 'r') as file:
        file_content = file.read()
        print("File Content:")
        print(file_content)
    
    # Connect to Snowflake using the provided credentials.
    conn = sf.connect(user=user, password=password, account=account, 
                      warehouse=warehouse, database=database, schema=schema, role=role)

    cursor = conn.cursor()
    
    # Set the current schema in Snowflake for operations.
    use_schema = f"use schema {schema};"
    cursor.execute(use_schema)
    
    # Create or replace the CSV file format in Snowflake for data loading.
    create_csv_format = "CREATE or REPLACE FILE FORMAT COMMA_CSV TYPE ='CSV' FIELD_DELIMITER = ',';"
    cursor.execute(create_csv_format)
    
    # Create or replace a stage in Snowflake for temporary storage of files.
    create_stage_query = f"CREATE OR REPLACE STAGE {stage_name} FILE_FORMAT = COMMA_CSV"
    cursor.execute(create_stage_query)
    
    # Upload the file to the Snowflake stage.
    copy_into_stage_query = f"PUT 'file://{local_file_path}' @{stage_name}"
    cursor.execute(copy_into_stage_query)
    
    # List files in the stage to verify upload.
    list_stage_query = f"LIST @{stage_name}"
    cursor.execute(list_stage_query)
    
    # Clear existing data in the target table to prepare for new data upload.
    truncate_table = f"truncate table {schema}.{table};"  
    cursor.execute(truncate_table)
    
    # Copy data from the stage into the Snowflake table.
    copy_into_query = f"COPY INTO {schema}.{table} FROM @{stage_name}/{file_name} FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '\"') ON_ERROR = CONTINUE;"
    cursor.execute(copy_into_query)

    print("File uploaded to Snowflake successfully.")

    # Return a success response indicating the process completion.
    return {
        'statusCode': 200,
        'body': 'File downloaded and uploaded to Snowflake successfully.'
    }
