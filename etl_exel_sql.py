from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import datetime
import os
def data_folder():
    folder_path = r'D:\DX\Gits\daily_dashboard_Data_pipelines\data'
    folder_names = os.listdir(folder_path)
    print("Folder names:", folder_names)
    file_date = []
    
    for folder in folder_names:
        f = folder[:8], folder[9:17]
        file_date.append(f)
    print("File date[0]:", file_date)
    current_date = datetime.date.today().strftime('%Y%m%d')
    print("Today's date:", current_date)
    for key in file_date:
        if key[0] <= current_date <= key[1]:
            print("Key:", key)
            file_path = fr"D:\DX\Gits\daily_dashboard_Data_pipelines\data\{key[0]}x{key[1]}.csv"
            return file_path
            
def insert_data():
    """ Insert data into table """
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/Daily_income')
   
    df_update = pd.read_csv(data_folder(), encoding='ISO-8859-1')
    try:
        df_update = df_update.drop('Unnamed: 15', axis=1)
    except :
        pass
    try:
        df_update = df_update.loc[df_update['Check'] == True]\
                             .drop('Check', axis=1)\
                             .loc[df_update['PRTNO'].str.startswith('TG')]\
                             .dropna()
    except Exception as e:
        print(f"Error occurred: {e}")

    df_update['SHPDT'] = pd.to_datetime(df_update['SHPDT'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
    existing_data = pd.read_sql_query('SELECT * FROM qty_data', engine)
    try:
        existing_data = existing_data.drop('index', axis=1)
    except Exception as e:
        print(f"Error occurred: {e}")
    try:
        existing_data = existing_data.dropna()
    except Exception as e:
        print(f"Error occurred: {e}")

    df_update.columns = df_update.columns.str.lower()

    print("Existing_data =", len(existing_data))
    print(existing_data)
    print("df_update =", len(df_update))
    print(df_update)

    for column in df_update.columns:
        df_update[column] = df_update[column].apply(lambda x: x.item() if isinstance(x, np.int64) else x)

    merged_data = pd.concat([existing_data, df_update], ignore_index=True)
    merged_data = merged_data.drop_duplicates()
    merged_data.columns = merged_data.columns.str.lower()

    print("Merged_data =", len(merged_data))
    print(merged_data)
    try:
        merged_data = merged_data.drop('check', axis=1)
    except:
        pass
    merged_data.to_sql('qty_data', engine, if_exists='replace', index=True, index_label='index')

    print('Data inserted successfully.')


    
if __name__ == '__main__':
    insert_data()
