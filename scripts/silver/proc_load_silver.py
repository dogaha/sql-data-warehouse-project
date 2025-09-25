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

def insert_data(table_name, file_path):
    insert_start = time.time()
    db = pypyodbc.connect(connection_string)
    cursor = db.cursor()
    try:
        # Clear Table
        clear_query = f"""
        TRUNCATE TABLE silver.{table_name};
		
        """
        with open(file_path,'r') as file:
            sql_script = file.read()
        #execute queries
        cursor.execute(clear_query)
        cursor.execute(sql_script)
        db.commit()
        print(f'Loaded Table {table_name} (Elapsed Time: {time.time() - insert_start})')

    except pypyodbc.Error as e:
        sqlstate = e.args[1]
        print("AN ERROR HAS OCCURED")
        print(f"SQLState: {sqlstate}")
    finally:
        cursor.close()
        db.close()

script_dir = os.path.dirname(os.path.abspath(__file__))
queries_dir = os.path.abspath(os.path.join(script_dir,'queries'))

print("=================================================================================")
print(f"LOADING SILVER LAYER...")
print("=================================================================================")
start_time = time.time()
for root, dir, files in os.walk(queries_dir):
    for file in files:
        file_path = os.path.join(root,file)
        table_name = re.search(r"^[^.]+",file).group(0).lower()
        insert_data(table_name,file_path)
            
    print("=================================================================================")
print(f"FINISHED LOADING SILVER LAYER (Elapsed Time: {time.time()-start_time})")  
print("=================================================================================")
