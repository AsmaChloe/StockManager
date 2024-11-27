#Temp file before migrating it to an airflow pipeline

import pandas as pd
import os
from os import listdir
from os.path import isfile, join
import psycopg2
import warnings
import datetime

relevant_columns = "A:K,CL,CM"

def get_raw_files_paths(path_folder = "./../data/"):
    return [join(path_folder,f) for f in listdir(path_folder) if isfile(join(path_folder,f))]

def connect_db():
    db_user =os.getenv('POSTGRES_USER')
    db_password =os.getenv('POSTGRES_PASSWORD')
    db_name =os.getenv('POSTGRES_DB')
    db_host =os.getenv('POSTGRES_HOST')

    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host
    )
    cursor = conn.cursor()

    return conn, cursor

def disconnect_db(conn, cursor):
    cursor.close()
    conn.close()

def ingest_data(raw_file_path, cursor, df) :
    """
    """
        
    #TODO duplicates de produits ?
    # columns = ['sku bc', 'sku cap', 'libellé article', 'niv 1', 'libellé niveau 1', 'niv 2', 'libellé niveau 2', 'niv 3', 'libellé niveau 3', 'statut article', 'stock', 'value']

    for index, row in df.iterrows() :

        #Insert Categories on labels 1, 2, 3 if they not already exists
        category_ids = []
        for num in range(1,4):
            id  = row[f'niv {num}']
            category_ids.append(id)
            label = row[f'libellé niveau {num}']
            query= f'''
                INSERT INTO dim_category_level_{num}(category_level_{num}_id, category_level_{num}_label)
                VALUES
                    (%s, %s)
                ON CONFLICT (category_level_{num}_id) DO NOTHING;
            '''
            cursor.execute(query, (id, label))
        
        #Check status_id column
        status_id = row['statut article']
        query= f'''
                SELECT status_id FROM dim_product_status
                WHERE
                    status_id = %s;
            '''
        cursor.execute(query, status_id)
        if cursor.fetchone() is None :
            warnings.warn(f'Row at index {index} has invalid value in column \'statut article\' : {status_id}.\n Row Data :\n\t{row}')
            continue

        #Insert product if it does not exists yet
        sku_bc=row['sku bc']
        sku_cap=row['sku cap']
        article_label= row['libellé article']
        status_id = status_id
        category_level_1_id, category_level_2_id, category_level_3_id = tuple(category_ids)
        query= f'''
                INSERT INTO dim_product(
                    sku_bc, 
                    sku_cap, 
                    article_label, 
                    status_id, 
                    category_level_1_id, 
                    category_level_2_id, 
                    category_level_3_id
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (sku_bc) DO NOTHING;
            '''
        cursor.execute(
            query,
            (
                sku_bc,
                sku_cap,
                article_label,
                status_id,
                category_level_1_id,
                category_level_2_id,
                category_level_3_id
            )
        )

        #Insert date if does not exists yet
        date_str = raw_file_path.split('/')[-1][:10]
        date_list = date_str.split('_')

        year_ = int(date_list[0])
        month_ = int(date_list[1])
        day_of_month = int(date_list[2])
        date_ = datetime.date(year_, month_, day_of_month)
        day_of_year = date_.timetuple().tm_yday
        weekday_name = date_.strftime("%A")
        week_ = date_.isocalendar()[1]
        month_name = date_.strftime("%B")
        semester = 1 if month_ <= 6 else 2
        quarter_ = (month_ - 1) // 3 + 1

        query = ''' 
            INSERT INTO dim_date(
                date_,
                day_of_month,
                day_of_year,
                weekday_name,
                week_,
                month_,
                month_name,
                year_,
                semester,
                quarter_
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (date_) DO NOTHING;
        '''
        cursor.execute(query,(date_, day_of_month, day_of_year, weekday_name, week_, month_, month_name, year_, semester, quarter_))

        #Insert stock state for current date
        shop_id = 170
        date_ = date_
        stock_quantity = row['stock']
        valuation = row['value']
        query = '''
        INSERT INTO fact_stock (
            sku_bc,
            shop_id,
            date_,
            stock_quantity,                           
            valuation
        )
        VALUES (%s,%s,%s,%s,%s);
        '''
        cursor.execute(query, (sku_bc, shop_id, date_, stock_quantity, valuation))


def validate_columns(df, columns):
    if not all(df.columns == columns) :
        raise ValueError(f'Dataframe columns are {df.columns}. Columns are supposed to be {columns}')



conn, cursor = connect_db()

raw_files_paths = get_raw_files_paths()

for raw_file_path in raw_files_paths :
    df = pd.read_excel(raw_file_path, skiprows=list(range(0,3)), usecols=relevant_columns, engine_kwargs={"data_only": True})

    # Data transform
    df.rename(columns= {df.columns[-2] : 'Stock', df.columns[-1] : 'Value'}, inplace=True)
    df.rename(columns= {column : column.lower() for column in df.columns}, inplace=True)
    
    df['sku bc'] = df['sku bc'].apply(lambda x : str(x).zfill(10))
    df['stock'] = df['stock'].fillna(0)
    df['value'] = df['value'].fillna(0)
    
    columns = ['sku bc', 'sku cap', 'libellé article', 'niv 1', 'libellé niveau 1', 'niv 2', 'libellé niveau 2', 'niv 3', 'libellé niveau 3', 'statut article', 'libellé statut article', 'stock', 'value']
    validate_columns(df, columns)

    ingest_data(raw_file_path, cursor, df)
    conn.commit()


disconnect_db(conn, cursor)