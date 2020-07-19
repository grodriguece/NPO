import sqlite3
from tkinter import *
from tkinter import ttk
from ADJS_Crea import ADJSCrea
from ADJS_Crea import ADJSDep
import os.path
# import pyprind
# import sys
# import time
root = Tk()
root.title('Missing_Audit')
root.iconbitmap('IT.ico')
root.geometry("800x400")



def tables():
    pass


def audit():
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

    def get_miss(missc):
        with conn:
            c.execute("SELECT rowid, * FROM MISS3 WHERE (WCELS = (:wcels))",
                      {'wcels': missc})
            return c.fetchall()

    def get_046y(exist):
        with conn:
            c.execute("SELECT rowid, * FROM S046_DistTY WHERE (WCELS = (:wcels))",
                      {'wcels': exist})
            return c.fetchall()

    c = conn.cursor()
    c.execute ("DROP TABLE IF EXISTS ADJS_Add")
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
    # cellsm = get_miss(miss1)            #miss cells list
    i = 0                               # miss row counter initialize
    # lp = 0                               # increase bar counter
    n = len(cellsm)                         # misscell amnt
    while i < n:                        # while <> EOlist
        my_progress['value'] = i/n*100 # prog bar increase a cording to i steps in loop
        print(my_progress['value'])
        label1.config(text=my_progress['value'])
        root.update_idletasks()
        # lp = i                           # i value at loop start
        k = cellsm[i][27]                 # missed qty
        if cellsm[i][14] == 0:        # when adjs list is empty
            o = 0                    # add control up to 20
            while o < k and o < 20:              # rows to add
                crea = ADJSCrea(cellsm[i][5], cellsm[i][6], cellsm[i][3], cellsm[i][7], cellsm[i][4])
                insert_crea(crea)         # raw inset into ADJS_Add db with ADJSCrea class
                i += 1                    # general miss row id
                o += 1                    # control for set of miss cells added
            while (cellsm[i][3] == cellsm[i - 1][3]) and i < n:
                i += 1                      # increase row miss pointer until next  diff cell
        else:
            en = 1                         # break control for att comparison
            deps = 0                        # max adjmiss addd allowed control
            msrc = cellsm[i][3]             # wcel src
            j = 0                           # SY row counter
            celldep = get_046y(cellsm[i][3])     # SY cell list belonging to mcel
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
                m = len(celldep)                   # SYcell amnt
                sycel = celldep[j][3]
                if (k + m) < 30:
                    o = 0
                    while o < k:              # rows to add
                        crea = ADJSCrea(cellsm[i][5], cellsm[i][6], cellsm[i][3], cellsm[i][7], cellsm[i][4])
                        insert_crea(crea)         # raw inset into ADJS_Add db with ADJSCrea class
                        i += 1                    # general miss row id
                        o += 1                    # control for set of miss cells added
                else:
                    while (cellsm[i][3] == celldep[j][3]) and en == 1 and i < n and j < m:
                        if (cellsm[i][17] > celldep[j][16]) and deps < 11:
                            crea = ADJSCrea(cellsm[i][5], cellsm[i][6], cellsm[i][3], cellsm[i][7], cellsm[i][4])
                            insert_crea(crea)         # raw insert into ADJS_Add db with ADJSCrea class
                            depu = ADJSDep(celldep[j][5],celldep[j][7],celldep[j][3],celldep[j][9],celldep[j][4],celldep[j][6],celldep[j][8])
                            insert_depu(depu)        # raw insert into ADJS_Dep db with ADJSDep class
                            i += 1                    # next miss
                            j += 1                    # next SY
                            deps += 1                 # miss add qty control
                        else:
                            en = 0                    # break condition
                            i += 1                    # next miss cell
                    if i == 0 and i < n:
                        # increase row miss pointer until next diff cell for first misscell if cond is not passed
                        while cellsm[i][3] == celldep[j][3]:
                            i += 1
                    else:
                        # increase row miss pointer until next  diff cell
                        while (cellsm[i][3] == cellsm[i - 1][3]) and  i < n:
                            i += 1
    conn.commit()
    conn.close()


my_progress = ttk.Progressbar(root, orient=HORIZONTAL, length=300, mode='determinate')
my_progress.pack(pady=20)

label1 = Label(root, text="")
label1.pack(pady=20)

button_1 = Button(root, text="Audit", command=audit)
button_1.pack(pady=20)

button_2 = Button(root, text="Tablas", command=tables)
button_2.pack(pady=20)

root.mainloop()
