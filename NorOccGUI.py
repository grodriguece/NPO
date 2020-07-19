from pasarchivo import *
from tkinter import *
from tkinter import ttk
from PIL import ImageTk,Image
from tkinter import messagebox


def tables():
    pasarchivo("C:/SQLite", "20200522_sqlite.db", "tablasSQL.csv", "tabla")
    my_progress['value'] = 100  # prog bar increase a cording to i steps in loop
    proglabel2.config(text=my_progress['value'])
    response = messagebox.showinfo("Tablas", "Process Finished")
    proglabel3 = Label(root, text=response)
    # proglabel3 = Label(root, text="")
    # proglabel3.grid(row=3, column=1, pady=10)
    my_progress['value'] = 0  # prog bar increase a cording to i steps in loop
    proglabel2.config(text="   ")
    # root.update_idletasks()


def audit():
    pasarchivo("C:/SQLite", "20200522_sqlite.db", "tablasSQL.csv", "Audit")
    my_progress['value'] = 100  # prog bar increase a cording to i steps in loop
    proglabel2.config(text=my_progress['value'])
    response = messagebox.showinfo("Audit", "Process Finished")
    proglabel3 = Label(root, text=response)
    # proglabel3 = Label(root, text="")
    # proglabel3.grid(row=3, column=1, pady=10)
    my_progress['value'] = 0  # prog bar increase a cording to i steps in loop
    proglabel2.config(text="   ")

def undefined():
    import sqlite3
    from ADCE_Crea import ADCECrea
    from ADCE_Crea import ADCEDep
    import os.path
    global proglabel2
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
    proglabel2.config(text="")
    i = 0  # miss row counter initialize
    n = len(cellsm)  # misscell amnt
    while i < n:  # while <> EOlist
        my_progress['value'] = round(i / n * 100)  # prog bar increase a cording to i steps in loop
        # print(my_progress['value'])
        proglabel2.config(text=my_progress['value'])
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
    my_progress['value'] = 100  # prog bar increase a cording to i steps in loop
    proglabel2.config(text=my_progress['value'])
    response = messagebox.showinfo("Undefined", "Process Finished")
    proglabel3 = Label(root, text=response)
    # proglabel3 = Label(root, text="")
    # proglabel3.grid(row=3, column=1, pady=10)
    my_progress['value'] = 0  # prog bar increase a cording to i steps in loop
    proglabel2.config(text="   ")
    root.update_idletasks()

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
        # print(my_progress['value'])
        proglabel2.config(text=my_progress['value'])
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
    my_progress['value'] = 100  # prog bar increase a cording to i steps in loop
    proglabel2.config(text=my_progress['value'])
    response = messagebox.showinfo("Missing", "Process Finished")
    proglabel3 = Label(root, text=response)
    # proglabel3 = Label(root, text="")
    # proglabel3.grid(row=3, column=1, pady=10)
    my_progress['value'] = 0  # prog bar increase a cording to i steps in loop
    proglabel2.config(text="   ")
    root.update_idletasks()

root = Tk()
root.title('NorOcc Table - Audit Process')
root.iconbitmap('IT.ico')
root.geometry("400x200+350+200")        # WxH+Right+Down
my_progress = ttk.Progressbar(root, orient=HORIZONTAL, length=300, mode='determinate')
# my_progress.pack(pady=20)
my_progress.grid(row=0, column=0, columnspan=2,pady=10, padx=10, ipadx=10)
# label1 = Label(root, text="")
# label1.place(x=20, y=20)
# label1.pack(pady=20)
# proglabel = Label(root, text="Progress")
# proglabel.grid(row=1, column=0, pady=10)
proglabel2 = Label(root, text="")
proglabel2.grid(row=0, column=2, pady=10)
# Create Tables Button
tables_btn = Button(root, text="Tables", command=tables)
tables_btn.grid(row=1, column=0, columnspan=1, pady=10, padx=10, ipadx=39)
# Create Audit Button
audit_btn = Button(root, text="Reuse Audits", command=audit)
audit_btn.grid(row=1, column=1, columnspan=1, pady=10, padx=10, ipadx=23)
#Create A Delete Button
miss_btn = Button(root, text="Missing UMTS", command=missing)
miss_btn.grid(row=2, column=0, columnspan=1, pady=10, padx=10, ipadx=18)
#Create A Delete Button
undef_btn = Button(root, text="Undefined GSM", command=undefined)
undef_btn.grid(row=2, column=1, columnspan=1, pady=10, padx=10, ipadx=17)
# Create an Exit Button
q_btn = Button(root, text="Exit", command=root.destroy)
q_btn.grid(row=3, column=0, columnspan=1, pady=10, padx=10, ipadx=45)
root.mainloop()
