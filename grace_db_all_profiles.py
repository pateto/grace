from pathlib import Path
import sqlite3
import xarray as xr
import pdb

connection = sqlite3.connect('grace.db')

# read all files from the data folder
path = "data"
for file in Path('data').rglob('*.nc'):

    print("reading", file.name)
    df = xr.open_dataset(file).to_dataframe()

    # insert new column
    df['filename'] = file.name

    # insert data in db
    df.to_sql(name='kbrne', con=connection, if_exists='append')
 
connection.close()  