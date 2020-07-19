def pasarchivo(datb, tablas):
    """copy to csv files tables from query results"""
    import sqlite3
    import csv
    import os.path
    from datetime import date
    from pathlib import Path
    dat_dir = Path("C:/XML")
    db_path1 = dat_dir / datb
    conn = sqlite3.connect(db_path1)
    c = conn.cursor()
    today = date.today()
    # base_dir = os.path.dirname(os.path.abspath(__file__))
    # db_path = os.path.join(base_dir, datb)
    conn = sqlite3.connect(db_path1)
    c = conn.cursor()
    tablist = []
    with open(tablas, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for line in csv_reader:
            tablist.append(line['tabla'])
    for line in tablist:
        try:
            c.execute("SELECT rowid, * FROM "+line)    # table from list to be retrieved
            print(line)
            columns = [column[0] for column in c.description]  # header
            results = []                                        #
            for row in c.fetchall():
                results.append(dict(zip(columns, row)))         # table info sent to results
            with open(line+".csv", "w", newline='') as new_file:    # open file to save sqlite table
                fieldnames = columns        # header
                writer = csv.DictWriter(new_file, fieldnames=fieldnames)
                writer.writeheader()
                for line in results:
                    writer.writerow(line)
        except sqlite3.Error as error:              # sqlite error handling
            print('SQLite error: %s' % (' '.join(error.args)))
    conn.close()


pasarchivo("20200522_sqlite.db", "tablasSQL.csv")