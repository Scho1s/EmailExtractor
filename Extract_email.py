"""
    Title   - Email extractor
    Author  - Valerij Jurkin
    Date    - 27/12/2023
    Updated - 29/12/2023
    This script extracts unique email addresses from notes within GP, Debtor's cards.
    Dataframe creates a table with variable amount of columns. For each email found
    in the note, the dataframe will create an additional column.
"""


import regit 
import pyodbc
import os
import pandas as pd
import numpy as np


def get_query(standard: bool = True) -> str:
    if standard:
        # Default query - retrieve all customers
        return """
                SELECT RTRIM(CUSTNMBR) 'Customer Number',
                TXTFIELD 'Text Field'
                FROM RM00101 R LEFT JOIN SY03900 S ON R.NOTEINDX = S.NOTEINDX
                """

    # Edit below when required and pass False to function
    return """
            SELECT [table].[Customer ID],
            SY03900.TXTFIELD 
            FROM (SELECT DISTINCT RTRIM(S1.CUSTNMBR) 'Customer ID',
            R.NOTEINDX
            FROM SOP30200 S1 
             LEFT JOIN SOP30300 S2 ON S1.SOPNUMBE =  S2.SOPNUMBE
             LEFT JOIN RM00101 R ON S1.CUSTNMBR = R.CUSTNMBR
            WHERE ITEMNMBR = 'KIRKQUART' 
             AND S1.SOPTYPE = 2
             AND VOIDSTTS = 0
             AND DOCDATE BETWEEN '2023-11-02' AND '2023-12-31') [table] 
             LEFT JOIN SY03900 ON [table].NOTEINDX = SY03900.NOTEINDX
            """


conndict = {'DRIVER': [x for x in pyodbc.drivers() if 'ODBC' in x][0],
            'UID': os.environ['GP_USERNAME'],
            'PWD': os.environ['GP_PASSWORD'],
            'SERVER': '192.168.80.7',
            'DATABASE': 'ROWC'}

conn = pyodbc.connect(**conndict)

result = conn.cursor().execute(get_query(False)).fetchall()
pattern = r'\s?[a-zA-Z0-9\._-]+@[a-zA-Z0-9]+\.[a-zA-Z]+\.?[a-zA-Z]*\s?'
date_pattern = r'\d\d[./]{1}\d\d[./]\d{2,4}'
df_dict = {}

for _ in result:
    new_str = re.sub(date_pattern, "", str(_)).replace(r'\r', ' ').lower()
    match = list({x.strip() for x in re.findall(pattern, new_str) if 'rowcliffe.co.uk' not in x})
    df_dict[_[0]] = match

df = pd.DataFrame(dict([(key, pd.Series(value)) for key, value in df_dict.items()])).transpose()
df.replace(np.nan, "", inplace=True)
df.to_excel('Result.xlsx')
conn.close()
