def pasarchivo(datb, tablas):
    """copy to csv files tables from query results"""
    import sqlite3
    import csv
    import os.path
    from pathlib import Path
    import pandas as pd
    from datetime import date
    dat_dir = Path("C:/XML")
    db_path1 = dat_dir / datb
    # base_dir = os.path.dirname(os.path.abspath(__file__))
    # db_path = os.path.join(base_dir, datb)
    conn = sqlite3.connect(db_path1)
    c = conn.cursor()
    today = date.today()
    tablist = []
    with open(tablas, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for line in csv_reader:
            tablist.append(line['tabla'])
    xls_file = "Param" + today.strftime("%y%m%d") + ".xlsx"
    xls_path = dat_dir / xls_file
    csv_path = dat_dir / "csv"
    writer = pd.ExcelWriter(xls_path, engine='xlsxwriter')
    for line in tablist:
        try:
            df = pd.read_sql_query("select * from " + line + ";", conn)  # "SELECT rowid, * FROM "+line
            if len(df) > 1000000:
                print('save to csv')
                df.to_csv('c:/xml/' + line + today.strftime("%y%m%d") + '.csv.gz', compression='gzip')
            else:
                csv_loc = line + today.strftime("%y%m%d") + '.csv'
                df.to_csv(csv_path / csv_loc)
            #     df.to_excel(writer, sheet_name=line)
#            c.execute("SELECT rowid, * FROM "+line)    # table from list to be retrieved
#            print(line)
#            columns = [column[0] for column in c.description]  # header
#            results = []                                        #
#            for row in c.fetchall():
#                 results.append(dict(zip(columns, row)))         # table info sent to results
#             with open(line+".csv", "w", newline='') as new_file:    # open file to save sqlite table
#                 fieldnames = columns        # header
#                 writer = csv.DictWriter(new_file, fieldnames=fieldnames)
#                 writer.writeheader()
#                 for line in results:
#                     writer.writerow(line)
        except sqlite3.Error as error:              # sqlite error handling
            print('SQLite error: %s' % (' '.join(error.args)))
    writer.save()
    c.close()
    conn.close()


pasarchivo("20200522_sqlite.db", "tablasSQL.csv")