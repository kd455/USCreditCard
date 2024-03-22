import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from zipfile import ZipFile
import subprocess
import sqlalchemy
from datetime import datetime
from fredapi import Fred

def getEngine():
    DRIVER = 'ODBC Driver 17 for SQL Server'#'SQL Server'
    SERVER = 'DESKTOP-192EHDA'
    DATABASE = 'Project2'

    connectionString = f'DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes'
    #conn = pyodbc.connect(connectionString)
    engine = sqlalchemy.create_engine(f'mssql+pyodbc:///?odbc_connect={connectionString}', fast_executemany=True)
    return engine

def runTruncateQuery(table_name):
    with getEngine().connect() as conn:
        conn.execute(sqlalchemy.text("TRUNCATE TABLE " + table_name + ";"))
        conn.commit()
        conn.close()

def runQuery(query):
    conn = getEngine().connect()
    try:
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()

def table_exists(table_name):
    result = runQuery(f"SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
    return result.iloc[0] > 0
    
def write_data_to_db(df, table_name,truncate = True):
    engine = getEngine()
    conn = engine.raw_connection()
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    try:
        #runTruncateQuery(table_name) if truncate & table_exists(table_name) else None
        df.to_sql(table_name, engine, if_exists='replace' if truncate else 'append', index=False)
    finally:
        cursor.close()
        conn.commit()
        conn.close()        
        
def bcp_insert(file, table_name, database_name, server_name, delimiter=','):
    file_raw = file.absolute().as_posix()
    bcp_command = f"""bcp {database_name}.dbo.{table_name} in \"{file_raw}\"  -c -t "{delimiter}" -r "0x0a" -S {server_name} -T -F 2"""
    print(bcp_command)
    process = subprocess.Popen(bcp_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()
    return output, error

def write_data_using_bcp(table_name , source_path, truncate_table = False):
    runTruncateQuery(table_name) if truncate_table else None
    for file in Path(source_path).rglob('*.csv'):
        print("reading: " + file.name)
        output, error = bcp_insert(file, table_name, 'Project2', 'DESKTOP-192EHDA')
        if error:
            print(error)
            break    
        else:
            file.rename(file.parent.parent / 'Loaded' / file.name)   
    return

def extract_ffiec_data(download_path, call_path, pattern):
    for zip in Path(download_path).rglob(pattern):
        with ZipFile(zip, 'r') as zipObj:
            zipObj.extractall(call_path)
    return

def move_files(source_path, output_path, pattern):
    for file in Path(source_path).rglob(pattern):
        file.rename(output_path + file.name)
    return

def remove_readme_files(call_path, pattern = 'Readme.txt'):
    for read_file in Path(call_path).rglob(pattern):
        read_file.unlink()
    return

def complete_extract(download_path, output_path, pattern):
    extract_ffiec_data(download_path, output_path, pattern)
    remove_readme_files(download_path)
    return

def save_db_data_file(query, output_path, file_name = 'UBPR_Data', delimiter=','):
    df = runQuery(query)
    df.to_csv(output_path / (file_name + '.csv'), index=False, sep=delimiter)
    df.to_parquet(output_path / (file_name + '.parquet'))
    return

def get_measure_files(path, pattern = "*.txt", index_cols = 'IDRSSD'):
    ffiec_files = path.glob(pattern)    
    ffiec_files = {file.stem: pd.read_csv(file, sep="\t",index_col=index_cols, low_memory=False).iloc[:1, :] for file in ffiec_files}
    return ffiec_files

def get_firms(firm_file):
    firms = pd.read_csv(firm_file, sep="\t")
    firms = firms[['IDRSSD','Financial Institution Name']].drop_duplicates()
    return firms
    
def get_measures(template_filepath, pattern, index_cols):
    measure_files = template_filepath.glob(pattern)   
    measure_files = {file.stem: pd.read_csv(file, sep="\t",index_col=index_cols, low_memory=False).iloc[:1, :] for file in measure_files}
    measure_df = pd.DataFrame()
    for key, value in measure_files.items():
        df = value.reset_index(drop=True)
        df = df.T
        df['Source'] = key
        measure_df = pd.concat([measure_df,df])        
    measure_df.dropna(inplace=True)
    measure_df.reset_index(inplace=True)
    measure_df.rename(columns={'index':'Measure', 0:'Label'}, inplace=True)
    measure_df['Label'] = measure_df['Label'].str.strip()
    measure_df.drop_duplicates(subset=['Measure','Label'], inplace=True)
    measure_df.drop_duplicates(subset=['Measure'], keep='first', inplace=True)
    return measure_df

def get_measure_user_label(path, pattern = "*.csv"):
    measure_files = path.glob(pattern)
    measure_df = pd.DataFrame()
    for file in measure_files:
        df = pd.read_csv(file, sep=",")
        measure_df = pd.concat([measure_df,df])
    return measure_df

def get_ffiec_files(path, pattern = "*.txt", index_cols = 'IDRSSD'):
    ffiec_files = path.glob(pattern)    
    ffiec_files = {file.stem: pd.read_csv(file, sep="\t",index_col=index_cols, parse_dates=True, low_memory=False,date_format='%m/%d/%Y %I:%M:%S %p').iloc[1:, :] for file in ffiec_files}
    return ffiec_files

def generated_file_exists(key, output_path):
    file_name = output_path + key + '.csv'
    return Path(file_name).is_file()

def save_ffiec_ubpr_data(ffiec_files, output_path, sub_measures=None, overwrite = False):
    for key, value in ffiec_files.items():
        if ( (generated_file_exists(key, output_path) == False) | overwrite ):
            if sub_measures is not None:
                value = value[sub_measures]    
            long_df = value.melt(ignore_index=False).reset_index()
            long_df.columns = long_df.columns.str.replace(' ', '')
            long_df.rename(columns={'variable':'Measure', 'value':'Raw_Value'}, inplace=True)
            long_df['IDRSSD'] = long_df['IDRSSD'].astype(int)
            long_df['ReportingPeriod'] = long_df['ReportingPeriod'].dt.date
            long_df['Numeric_Value'] = pd.to_numeric(long_df['Raw_Value'], errors='coerce')
            long_df['Created_At']  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            long_df.dropna(inplace=True, subset=['Raw_Value'])
            long_df.to_csv(output_path + key + '.csv', index=False) 
    return

def get_fred_data():
    fred = Fred(api_key='0cc0fbdaba6f0a1bbf98a89565bdbab2')
    #Monthly
    UNRATE = fred.get_series('UNRATE') #	Unemployment Rate
    PCEPI = fred.get_series('PCEPI') #	Personal Consumption Expenditures: Chain-type Price Index
    CPIAUCSL = fred.get_series('CPIAUCSL')#	Consumer Price Index for All Urban Consumers: All Items
    CPILFESL = fred.get_series('CPILFESL')#	Consumer Price Index for All Urban Consumers: All Items Less Food & Energy
    #MEHOI = fred.get_series('MEHOINUSA672N') #	Real Median Household Income in the United States (yearly) MEHOI as it's yearly
    DSPIC96 = fred.get_series('DSPIC96') #	Real Disposable Personal Income
    DSPI = fred.get_series('DSPI') #	Disposable personal income
    #https://www.bea.gov/resources/methodologies/nipa-handbook/pdf/chapter-05.pdf
    PCE = fred.get_series('PCE') #	Personal Consumption Expenditures PCE shows how much of the income earned by households is being spent on current consumption as opposed to how much is being saved for future consumption
    PCEDG = fred.get_series('PCEDG') #	Personal Consumption Expenditures: Durable Goods
    PSAVERT = fred.get_series('PSAVERT') #	Personal saving as a percentage of disposable personal income (DPI), frequently referred to as "the personal saving rate," is calculated as the ratio of personal saving to DPI.
    RRSFS = fred.get_series('RRSFS') #	Real Retail and Food Services Sales
    A229RC0 = fred.get_series('A229RC0') #Disposable Personal Income: Per capita: Current dollars
    B069RC1 = fred.get_series('B069RC1') #Personal interest payments 
    CSCICP03USM665S = fred.get_series('CSCICP03USM665S') #Consumer Opinion Surveys: Confidence Indicators: Composite Indicators: OECD Indicator for United States
    POPTHM = fred.get_series('POPTHM') #Population, Total
    df = pd.concat([PCEPI, A229RC0,B069RC1, UNRATE, CPIAUCSL, CPILFESL, DSPIC96, PCE, PCEDG, PSAVERT, RRSFS, DSPI,CSCICP03USM665S,POPTHM], axis=1)
    df.columns = ['PCEPI', 'A229RC0','B069RC1', 'UNRATE', 'CPIAUCSL', 'CPILFESL', 'DSPIC96', 'PCE', 'PCEDG', 'PSAVERT', 'RRSFS', 'DSPI','CSCICP03USM665S','POPTHM']
    df.index = df.index + pd.offsets.MonthEnd(0)

    #Quarterly
    PSAVE = fred.get_series('PSAVE') # Quarterly Personal Saving 
    CDSP = fred.get_series('CDSP') #Consumer Debt Service Payments as a Percent of Disposable Personal Income
    TDSP = fred.get_series('TDSP') #Total Debt Service Payments as a Percent of Disposable Personal Income
    df_qtr = pd.concat([CDSP, TDSP, PSAVE], axis=1)
    df_qtr.columns = ['CDSP', 'TDSP', 'PSAVE']
    df_qtr.index = df_qtr.index + pd.offsets.QuarterEnd(0)

    df = pd.concat([df, df_qtr], axis=1)
    df.index.name = 'Date'
    df.sort_index(inplace=True)
    
    return df


def get_macro_data(freq = "Q"):
    if Path(f"Data/macro_data_{freq}.csv").is_file():
        print("Reading from csv")
        return pd.read_csv(f"Data/macro_data_{freq}.csv",index_col='Date', parse_dates=True)
    else:
        macro_data = get_fred_data()
        macro_data.to_csv(f'Data/macro_data.csv',index=True)
        macro_data = macro_data.resample(freq).mean()
        macro_data.to_csv(f'Data/macro_data_{freq}.csv',index=True)
        return macro_data        
    
def get_recession_data(freq = "Q"):
    fred = Fred(api_key='0cc0fbdaba6f0a1bbf98a89565bdbab2')
    JHDUSRGDPBR = fred.get_series('JHDUSRGDPBR') 
    JHDUSRGDPBR = JHDUSRGDPBR.resample(freq).ffill()
    JHDUSRGDPBR.to_csv(f'Data/JHDUSRGDPBR_{freq}.csv',index=True)
    return JHDUSRGDPBR

get_recession_data(freq="M")
get_macro_data(freq = "M")