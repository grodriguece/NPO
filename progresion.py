import sqlite3
from tkinter import *
from tkinter import ttk
root = Tk()
root.title('NorOcc Table - Audit Process')
root.iconbitmap('IT.ico')
root.geometry("800x400+350+200")        # WxH+Right+Down


def pasarchivo(ruta, datb, tablas, tipo):
    """copy to csv files tables from query results"""
    import sqlite3
    import pandas as pd
    import timeit
    from pyexcelerate import Workbook
    from pathlib import Path
    from datetime import date
    dat_dir = Path(ruta)
    db_path1 = dat_dir / datb
    start_time = timeit.default_timer()
    conn = sqlite3.connect(db_path1)  # database connection
    c = conn.cursor()
    today = date.today()
    df1 = pd.read_csv(tablas)
    xls_file = "Param" + today.strftime("%y%m%d") + ".xlsx"
    xls_path = dat_dir / xls_file  # xls file path-name
    csv_path = dat_dir / "csv"  # csv path to store big data
    wb = Workbook()  # excelerator file init
    i = 0
    for index, row in df1.iterrows():  # panda row iteration tablas file by tipo column
        line = row[tipo]
        if not pd.isna(row[tipo]):  # nan null values validation
            try:
                df = pd.read_sql_query("select * from " + line + ";", conn)  # pandas dataframe from sqlite
                if len(df) > 1000000:  # excel not supported
                    csv_loc = line + today.strftime("%y%m%d") + '.csv.gz'  # compressed csv file name
                    print('Table {} saved in {}'.format(line, csv_loc))
                    df.to_csv(csv_path / csv_loc, compression='gzip')  # pandas dataframe saved to csv
                else:
                    data = [df.columns.tolist()] + df.values.tolist()
                    data = [[index] + row for index, row in zip(df.index, data)]
                    wb.new_sheet(line, data=data)
                    print('Table {} stored in xlsx sheet'.format(line))
                    i += 1
            except sqlite3.Error as error:  # sqlite error handling
                print('SQLite error: %s' % (' '.join(error.args)))
    end_time = timeit.default_timer()
    delta = round(end_time - start_time, 2)
    print("Data proc took " + str(delta) + " secs")
    deltas = 0
    if i == 0:
        print('No tables to excel')
    else:
        print("Saving tables in {} workbook".format(xls_path))
        start_time = timeit.default_timer()
        wb.save(xls_path)
        end_time = timeit.default_timer()
        deltas = round(end_time - start_time, 2)
        print("xlsx save took " + str(deltas) + " secs")
    print("Total time " + str(delta + deltas) + " secs")
    c.close()
    conn.close()


def tables():
    pasarchivo("C:/XML/SQL/missing", "20200522_sqlite.db", "tablasSQL.csv", "tabla")


def audit():
    pasarchivo("C:/XML/SQL/missing", "20200522_sqlite.db", "tablasSQL.csv", "Audit")


def missing():
    import sqlite3
    from ADJS_Crea import ADJSCrea
    from ADJS_Crea import ADJSDep
    import os.path
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(BASE_DIR, "20200522_sqlite.db")
    conn = sqlite3.connect(db_path)

    def insert_crea(crea):
        with conn:
            c.execute(
                "INSERT INTO ADJS_Add VALUES (:rnc_id, :mcc, :mnc, :wcel_ids, :wcels, :celldnt, :wcelt, :rthop, :nrthop, :hshop, :hsrthop)",
                {'rnc_id': crea.sourcerncid, 'mcc': crea.mcc, 'mnc': crea.mnc,
                 'wcel_ids': crea.sourceci, 'wcels': crea.sourcename,
                 'celldnt': crea.targetcelldn, 'wcelt': crea.targetname,
                 'rthop': crea.rthop, 'nrthop': crea.nrthop, 'hshop': crea.hshop, 'hsrthop': crea.hsrthop}
            )

    def insert_depu(depu):
        with conn:
            c.execute("INSERT INTO ADJS_Dep VALUES (:wcels, :wcelt, :rnc_id, :wbts_id, :wcel_id, :adjs_id)",
                      {'wcels': depu.sourcename, 'wcelt': depu.targetname, 'rnc_id': depu.sourcerncid,
                       'wbts_id': depu.wbtsids,
                       'wcel_id': depu.sourceci, 'adjs_id': depu.adjsid}
                      )

    # def get_miss(missc):
    #     with conn:
    #         c.execute("SELECT rowid, * FROM MISS3 WHERE (WCELS = (:wcels))",
    #                   {'wcels': missc})
    #         return c.fetchall()

    def get_046y(exist):
        with conn:
            c.execute("SELECT rowid, * FROM S046_DistTY WHERE (WCELS = (:wcels))",
                      {'wcels': exist})
            return c.fetchall()

    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS ADJS_Add")
    c.execute("""CREATE TABLE ADJS_Add (
                    rnc_id integer, mcc integer,
                    ncc integer, wcel_ids integer,
                    wcels text, celldnt text,
                    wcelt text, rthop integer,
                    nrthop integer, hshop integer,
                    hsrthop integer
                    )""")
    c.execute("DROP TABLE IF EXISTS ADJS_Dep")
    c.execute("""CREATE TABLE ADJS_Dep (
                    wcels text, wcelt text, 
                    rnc_id integer,
                    wbts_id integer,
                    wcel_id integer,
                    adjs_id integer
                    )""")
    c.execute("SELECT rowid, * FROM MISS3")
    cellsm = c.fetchall()
    i = 0                               # miss row counter initialize
    n = len(cellsm)                        # misscell amnt
    while i < n:                        # while <> EOlist
        my_progress['value'] = round(i/n*100) # prog bar increase a cording to i steps in loop
        print(my_progress['value'])
        label1.config(text=my_progress['value'])
        root.update_idletasks()
        k = cellsm[i][27]  # missed qty
        if cellsm[i][14] == 0:  # when adjs list is empty
            o = 0  # add control up to 20
            while o < k and o < 20:  # rows to add
                crea = ADJSCrea(cellsm[i][5], cellsm[i][6], cellsm[i][3], cellsm[i][7], cellsm[i][4])
                insert_crea(crea)  # raw inset into ADJS_Add db with ADJSCrea class
                i += 1  # general miss row id
                o += 1  # control for set of miss cells added
            while (cellsm[i][3] == cellsm[i - 1][3]) and i < n:
                i += 1  # increase row miss pointer until next  diff cell
        else:
            en = 1  # break control for att comparison
            deps = 0  # max adjmiss addd allowed control
            # msrc = cellsm[i][3]  # wcel src
            j = 0  # SY row counter
            celldep = get_046y(cellsm[i][3])  # SY cell list belonging to mcel
            if not celldep:  # miss cell was not in SY but is in ADJs, add up to ten
                if (cellsm[i][27] + cellsm[i][14]) < 30:
                    o = 0  # add control up to 10
                    while o < k and o < 10:  # rows to add < miss qty < 10
                        crea = ADJSCrea(cellsm[i][5], cellsm[i][6], cellsm[i][3], cellsm[i][7], cellsm[i][4])
                        insert_crea(crea)  # raw inset into ADJS_Add db with ADJSCrea class
                        i += 1  # general miss row id
                        o += 1  # control for set of miss cells added
                    while (cellsm[i][3] == cellsm[i - 1][3]) and i < n:
                        i += 1  # increase row miss pointer until next  diff cell
                else:
                    i += 1
            else:
                #    print('It is None')
                # except NameError:
                #    print("This variable is not defined")
                m = len(celldep)  # SYcell amnt
                # sycel = celldep[j][3]
                if (k + m) < 30:
                    o = 0
                    while o < k:  # rows to add
                        crea = ADJSCrea(cellsm[i][5], cellsm[i][6], cellsm[i][3], cellsm[i][7], cellsm[i][4])
                        insert_crea(crea)  # raw inset into ADJS_Add db with ADJSCrea class
                        i += 1  # general miss row id
                        o += 1  # control for set of miss cells added
                else:
                    while (cellsm[i][3] == celldep[j][3]) and en == 1 and i < n and j < m:
                        if (cellsm[i][17] > celldep[j][16]) and deps < 11:
                            crea = ADJSCrea(cellsm[i][5], cellsm[i][6], cellsm[i][3], cellsm[i][7], cellsm[i][4])
                            insert_crea(crea)  # raw insert into ADJS_Add db with ADJSCrea class
                            depu = ADJSDep(celldep[j][5], celldep[j][7], celldep[j][3], celldep[j][9], celldep[j][4],
                                           celldep[j][6], celldep[j][8])
                            insert_depu(depu)  # raw insert into ADJS_Dep db with ADJSDep class
                            i += 1  # next miss
                            j += 1  # next SY
                            deps += 1  # miss add qty control
                        else:
                            en = 0  # break condition
                            i += 1  # next miss cell
                    if i == 0 and i < n:
                        # increase row miss pointer until next diff cell for first misscell if cond is not passed
                        while cellsm[i][3] == celldep[j][3]:
                            i += 1
                    else:
                        # increase row miss pointer until next  diff cell
                        while (cellsm[i][3] == cellsm[i - 1][3]) and i < n:
                            i += 1
    conn.commit()
    conn.close()


def undefined():
    import sqlite3
    from ADCE_Crea import ADCECrea
    from ADCE_Crea import ADCEDep
    import os.path
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(BASE_DIR, "20200522_sqlite.db")
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS ADCE_Add")
    c.execute("""CREATE TABLE ADCE_Add (
                BTSS text, BSCidS int, BCFidS int, BTSidS int,
                BTST text, BSCidT int, BCFidT int, BTSidT int,
                LACT int, cellIdT int, hoMarginLev int, hoMarginQual int,
                pbgt int, umbrella int
                )""")
    c.execute("DROP TABLE IF EXISTS ADCE_Dep")
    c.execute("""CREATE TABLE ADCE_Dep (
                BSCidS int, BCFidS int, BTSidS int, 
                LACT int, cellIdT int
                )""")

    def insert_crea(crea):
        with conn:
            c.execute("""INSERT INTO ADCE_Add VALUES (:BTSS, :BSCidS, :BCFidS,
            :BTSidS, :BTST, :BSCidT, :BCFidT, :BTSidT, :LACT, :cellIdT,
            :hoMarginLev, :hoMarginQual, :pbgt, :umbrella)""",
                      {'BTSS': crea.BTSS, 'BSCidS': crea.BSCidS, 'BCFidS': crea.BCFidS,
                       'BTSidS': crea.BTSidS, 'BTST': crea.BTST,
                       'BSCidT': crea.BSCidT, 'BCFidT': crea.BCFidT,
                       'BTSidT': crea.BTSidT, 'LACT': crea.LACT, 'cellIdT': crea.cellIdT,
                       'hoMarginLev': crea.hoMarginLev, 'hoMarginQual': crea.hoMarginQual,
                       'pbgt': crea.pbgt, 'umbrella': crea.umbrella})

    def insert_dead(dead):
        with conn:
            c.execute("INSERT INTO ADCE_Dep VALUES (:BSCidS, :BCFidS, :BTSidS, :LACT, :cellIdT)",
                      {'BSCidS': dead.BSCidS, 'BCFidS': dead.BCFidS, 'BTSidS': dead.BTSidS,
                       'LACT': dead.LACT, 'cellIdT': dead.cellIdT})

    # def get_miss(missc):
    #     with conn:
    #         c.execute("SELECT rowid, * FROM UND_DistF2 WHERE (BTSS = (:btss))",
    #                   {'btss': missc})
    #         return c.fetchall()

    def get_015(exist):
        with conn:
            c.execute("SELECT rowid, * FROM RSBSS015_3 WHERE (BTSname = (:btss))",
                      {'btss': exist})
            return c.fetchall()

    c.execute("SELECT rowid, * FROM UND_DistF2")  # full undefined list
    cellsm = c.fetchall()
    # cellsm = get_miss(miss1)            # undefined cell list just for one bts
    i = 0  # miss row counter initialize
    n = len(cellsm)  # misscell amnt
    while i < n:  # while <> EOlist
        my_progress['value'] = round(i / n * 100)  # prog bar increase a cording to i steps in loop
        print(my_progress['value'])
        label1.config(text=my_progress['value'])
        root.update_idletasks()
        k = cellsm[i][36]  # missed qty for actual bts
        if cellsm[i][5] == 0:  # when adce list is empty
            o = 0  # add control up to 20
            while o < k and o < 20:  # rows to add
                crea = ADCECrea(cellsm[i][2], cellsm[i][13], cellsm[i][14], cellsm[i][15],
                                cellsm[i][3], cellsm[i][19], cellsm[i][20], cellsm[i][21],
                                cellsm[i][23], cellsm[i][22], cellsm[i][37], cellsm[i][38])
                insert_crea(crea)  # raw inset into ADCE_Add db with ADCECrea class
                i += 1  # general miss row id
                o += 1  # control for set of miss cells added
                while (cellsm[i][2] == cellsm[i - 1][2]) and i < n:
                    i += 1  # increase row miss pointer until next  diff bts
                band = 0
                while i < n and band == 0:
                    if cellsm[i][2] != cellsm[i - 1][2]:
                        band = 1
                    else:
                        i += 1  # increase row miss pointer until next  diff bts
        else:               # when adce list is not empty
            en = 1  # break control for att comparison
            deps = 0  # max adjmiss addd allowed control
            # msrc = cellsm[i][2]  # wcel src
            j = 0  # SY row counter
            celldep = get_015(cellsm[i][2])  # SY cell list belonging to mcel
            if not celldep:  # miss cell was not in SY but is in ADJs, add up to ten and less than adce + o <29
                o = 0  # add control up to 10
                if (cellsm[i][5] + o) < 29:  # done until adce + added = 29
                    # if (cellsm[i][27] + cellsm[i][14]) < 29:
                    # o = 0  # add control up to 10
                    while o < k and o < 10:  # rows to add < miss qty < 10
                        crea = ADCECrea(cellsm[i][2], cellsm[i][13], cellsm[i][14], cellsm[i][15],
                                        cellsm[i][3], cellsm[i][19], cellsm[i][20], cellsm[i][21],
                                        cellsm[i][23], cellsm[i][22], cellsm[i][37], cellsm[i][38])
                        insert_crea(crea)  # raw inset into ADJS_Add db with ADJSCrea class
                        i += 1  # general miss row id
                        o += 1  # control for set of miss cells added
                    band = 0
                    while i < n and band == 0:
                        if cellsm[i][2] != cellsm[i - 1][2]:
                            band = 1
                        else:
                            i += 1  # increase row miss pointer until next  diff bts
                else:
                    i += 1  # list is full and bts is incremented to compare with next one
                    band = 0
                    while i < n and band == 0:
                        if cellsm[i][2] != cellsm[i - 1][2]:
                            band = 1
                        else:
                            i += 1  # increase row miss pointer until next  diff bts
            else:
                m = len(celldep)  # SYcell amnt
                # sycel = celldep[j][4]
                if (k + cellsm[i][5]) < 29:  # amount undefined + adce number < 29
                    o = 0
                    while o < k:  # rows to add
                        crea = ADCECrea(cellsm[i][2], cellsm[i][13], cellsm[i][14], cellsm[i][15],
                                        cellsm[i][3], cellsm[i][19], cellsm[i][20], cellsm[i][21],
                                        cellsm[i][23], cellsm[i][22], cellsm[i][37], cellsm[i][38])
                        insert_crea(crea)  # raw inset into ADJS_Add db with ADJSCrea class
                        i += 1  # general miss row id
                        o += 1  # control for set of miss cells added
                else:
                    band = 0
                    while en == 1 and i < n and j < m and band == 0:
                        if cellsm[i][2] != celldep[j][4]:      # cell und diff cell to dep, out
                            band = 1
                        else:
                            if (cellsm[i][29] > celldep[j][30]) and deps < 11:  # up to 10 add - dep
                                crea = ADCECrea(cellsm[i][2], cellsm[i][13], cellsm[i][14], cellsm[i][15],
                                                cellsm[i][3], cellsm[i][19], cellsm[i][20], cellsm[i][21],
                                                cellsm[i][23], cellsm[i][22], cellsm[i][37], cellsm[i][38])
                                insert_crea(crea)  # raw insert into ADJS_Add db with ADJSCrea class
                                dead = ADCEDep(celldep[j][6], celldep[j][7], celldep[j][8], celldep[j][22],
                                               celldep[j][21])
                                insert_dead(dead)  # raw insert into ADJS_Dep db with ADCEDep class
                                i += 1  # next miss
                                j += 1  # next SY
                                deps += 1  # miss add qty control
                            else:
                                en = 0  # break condition
                                i += 1  # next miss cell
                    if i == 0 and i < n:
                        # increase row miss pointer until next diff cell for first miss cell if cond is not passed
                        band = 0
                        while i < n and band == 0:
                            if cellsm[i][2] != cellsm[i - 1][2]:
                                band = 1
                            else:
                                i += 1  # increase row miss pointer until next  diff bts
                    else:
                        # increase row miss pointer until next  diff cell
                        band = 0
                        while i < n and band == 0:
                            if cellsm[i][2] != cellsm[i - 1][2]:  # avoid out of file pointer
                                band = 1
                            else:
                                i += 1
    conn.commit()
    conn.close()


my_progress = ttk.Progressbar(root, orient=HORIZONTAL, length=300, mode='determinate')
my_progress.pack(pady=20)

label1 = Label(root, text="")
label1.place(x=20, y=20)
label1.pack(pady=20)

button_1 = Button(root, text="Missing", command=missing)
button_1.pack(pady=20)

button_2 = Button(root, text="Undefined", command=undefined)
button_2.pack(pady=20)

button_3 = Button(root, text="Config Tables", command=tables)
button_3.pack(pady=20)

button_4 = Button(root, text="Audit Tables", command=audit)
button_4.pack(pady=20)

button_q = Button(root, text="Quit", command=root.destroy)
button_q.pack(pady=20)

root.mainloop()
