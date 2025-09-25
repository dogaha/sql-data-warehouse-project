import os
import time
import pypyodbc
import re
import json
# Define connection parameters
script_dir = os.path.dirname(os.path.abspath(__file__))
config_file= os.path.abspath(os.path.join(script_dir,'../../config.json'))
dataset_dir = os.path.abspath(os.path.join(script_dir,'../../datasets'))
with open (config_file,'r') as fh:
    config = json.load(fh)

driver_name = 'SQL SERVER'
server = config['server']
database = 'DataWarehouse'

connection_string = f"""
    DRIVER={{{driver_name}}};
    SERVER={server};
    DATABASE={database};
    Trust_Connection=yes;
"""

def name_table(file_type,file_name):
    return f"bronze.{file_type}_{file_name}"

def insert_data(table_name, file_path):
    insert_start = time.time()
    db = pypyodbc.connect(connection_string)
    cursor = db.cursor()
    try:
        # Clear Table then bulk insert
        insert_query = f"""
        TRUNCATE TABLE {table_name};
		BULK INSERT {table_name}
		FROM '{file_path}'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
        """
        #execute queries
        cursor.execute(insert_query)
        db.commit()
        print(f'Loaded Table {table_name} (Elapsed Time: {time.time() - insert_start})')

    except pypyodbc.Error as e:
        sqlstate = e.args[1]
        print("AN ERROR HAS OCCURED")
        print(f"SQLState: {sqlstate}")
    finally:
        cursor.close()
        db.close()

print("=================================================================================")
print(f"LOADING BRONZE LAYER...")
start_time = time.time()
for root, dir, files in os.walk(dataset_dir):
    for file in files:
        file_path = os.path.join(root,file)
        file_name = re.search(r"^[^.]+",file).group(0).lower()
        file_type = re.search(r"[^_]+$", root).group(0).lower()
        table_name = name_table(file_type,file_name)
        insert_data(table_name,file_path)
    print("=================================================================================")
print(f"FINISHED LOADING BRONZE LAYER (Elapsed Time: {time.time()-start_time})")  
print("=================================================================================")
