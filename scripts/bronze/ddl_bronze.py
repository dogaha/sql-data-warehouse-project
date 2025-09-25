import os
import pandas as pd
from datetime import datetime
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
#Connect to 
db = pypyodbc.connect(connection_string)
cursor = db.cursor()

def check_dtype(value):
    if isinstance(value,float) or isinstance(value,int):
        return 'INT'
    else:
        try:
            datetime.strptime(value, "%Y-%m-%d")
            return 'DATETIME'
        except ValueError:
            return 'NVARCHAR(50)'

def name_table(file_type,file_name):
    return f"bronze.{file_type}_{file_name}"

def CreateTable(table_name, df):
    db = pypyodbc.connect(connection_string)
    cursor = db.cursor()
    try:
        # Check  If Table Exists
        check_query = f"""IF OBJECT_ID('{table_name}', 'U') IS NOT NULL\n\tDROP TABLE {table_name};"""
        
        # Create Table
        create_query = f"CREATE TABLE {table_name} (\n"
        create_query += f'{df.columns[0].lower()} {check_dtype(df.values[0][0])}\n'
        for i in range(1,len(df.columns)):
            create_query += f',{df.columns[i].lower()} {check_dtype(df.values[0][i])}\n'
        create_query+=")"

        #execute queries
        cursor.execute(check_query)
        cursor.execute(create_query)
        db.commit()
        print(f'Created {table_name} Table')

    except pypyodbc.Error as e:
        sqlstate = e.args[1]
        print("Table Could Not Be Created")
        print(f"SQLState: {sqlstate}")
    finally:
        cursor.close()
        db.close()

for root, dir, files in os.walk(dataset_dir):
    for file in files:
        file_path = os.path.join(root,file)
        file_name = re.search(r"^[^.]+",file).group(0).lower()
        file_type = re.search(r"[^_]+$", root).group(0).lower()
        table_name = name_table(file_type,file_name)

        df = pd.read_csv(file_path)
        random_row = df.dropna().sample(n=1)
        CreateTable(table_name, random_row)
            