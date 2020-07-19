import sqlite3
import csv
from ADCE_Crea import ADCECrea
from ADCE_Crea import ADCEDep
import os.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(BASE_DIR, "20200522_sqlite.db")
conn = sqlite3.connect(db_path)
c = conn.cursor()
c.execute ("DROP TABLE IF EXISTS ADCE_Add")
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


def get_miss(missc):
    with conn:
        c.execute("SELECT rowid, * FROM UND_DistF2 WHERE (BTSS = (:btss))",
                  {'btss': missc})
        return c.fetchall()


def get_015(exist):
    with conn:
        c.execute("SELECT rowid, * FROM RSBSS015_3 WHERE (BTSname = (:btss))",
                  {'btss': exist})
        return c.fetchall()


c.execute("SELECT rowid, * FROM UND_DistF2")    # full undefined list
cellsm = c.fetchall()
# cellsm = get_miss(miss1)            # undefined cell list just for one bts
i = 0                               # miss row counter initialize
n = len(cellsm)                         # misscell amnt
while i < n:                        # while <> EOlist
    k = cellsm[i][36]                 # missed qty for actual bts
    if cellsm[i][5] == 0:        # when adce list is empty
        o = 0                    # add control up to 20
        while o < k and o < 20:              # rows to add
            crea = ADCECrea(cellsm[i][2], cellsm[i][13], cellsm[i][14], cellsm[i][15],
                            cellsm[i][3], cellsm[i][19], cellsm[i][20], cellsm[i][21],
                            cellsm[i][23], cellsm[i][22], cellsm[i][37], cellsm[i][38])
            insert_crea(crea)         # raw inset into ADCE_Add db with ADCECrea class
            i += 1                    # general miss row id
            o += 1                    # control for set of miss cells added
            band = 0
            while i < n and band == 0:
                if cellsm[i][2] != cellsm[i - 1][2]:
                    band = 1
                else:
                    i += 1                  # increase row miss pointer until next  diff bts
    else:
        en = 1                         # break control for att comparison
        deps = 0                        # max adjmiss addd allowed control
        msrc = cellsm[i][2]             # wcel src
        j = 0                           # SY row counter
        celldep = get_015(cellsm[i][2])     # SY cell list belonging to mcel
        if not celldep:  # miss cell was not in SY but is in ADJs, add up to ten and less than adce + o <29
            o = 0       # add control up to 10
            if (cellsm[i][5] + o) < 29:     # done until adce + added = 29
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
                i += 1          # list is full and bts is incremented to compare with next one
                band = 0
                while i < n and band == 0:
                    if cellsm[i][2] != cellsm[i - 1][2]:
                        band = 1
                    else:
                        i += 1  # increase row miss pointer until next  diff bts
        else:
            m = len(celldep)                   # SYcell amnt
            sycel = celldep[j][4]
            if (k + cellsm[i][5]) < 29:  # amount undefined + adce number < 29
                o = 0
                while o < k:              # rows to add
                    crea = ADCECrea(cellsm[i][2], cellsm[i][13], cellsm[i][14], cellsm[i][15],
                            cellsm[i][3], cellsm[i][19], cellsm[i][20], cellsm[i][21],
                            cellsm[i][23], cellsm[i][22], cellsm[i][37], cellsm[i][38])
                    insert_crea(crea)         # raw inset into ADJS_Add db with ADJSCrea class
                    i += 1                    # general miss row id
                    o += 1                    # control for set of miss cells added
            else:
                band = 0
                while en == 1 and i < n and j < m and band == 0:
                    if cellsm[i][2] != celldep[j][4]:
                        band = 1
                    else:
                        if (cellsm[i][29] > celldep[j][30]) and deps < 11:       # up to 10 add - dep
                            crea = ADCECrea(cellsm[i][2], cellsm[i][13], cellsm[i][14], cellsm[i][15],
                                cellsm[i][3], cellsm[i][19], cellsm[i][20], cellsm[i][21],
                                cellsm[i][23], cellsm[i][22], cellsm[i][37], cellsm[i][38])
                            insert_crea(crea)         # raw insert into ADJS_Add db with ADJSCrea class
                            dead = ADCEDep(celldep[j][6],celldep[j][7],celldep[j][8],celldep[j][22],celldep[j][21])
                            insert_dead(dead)        # raw insert into ADJS_Dep db with ADCEDep class
                            i += 1                    # next miss
                            j += 1                    # next SY
                            deps += 1                 # miss add qty control
                        else:
                            en = 0                    # break condition
                            i += 1                    # next miss cell
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
                        if cellsm[i][2] != cellsm[i - 1][2]:        # avoid out of file pointer
                            band = 1
                        else:
                            i += 1
c.execute("SELECT rowid, * FROM ADCE_Add")    # full undefined list
cellsm = c.fetchall()
with open("ADCE_Add.csv", "w") as new_file:
    fieldnames = ['BTSS', 'BSCidS', 'BCFidS', 'BTSidS', 'BTST',
                  'BSCidT', 'BCFidT', 'BTSidT', 'LACT', 'cellIdT',
                  'hoMarginLev', 'hoMarginQual', 'pbgt', 'umbrella']
    csv_writer = csv.DictWriter(new_file, fieldnames=fieldnames)
    csv_writer.writeheader()
    for line in cellsm:
        csv_writer.writerow(line)
# csvWriter = csv.writer(open("ADCE_Add.csv", "w"))
# csvWriter.writerows(cellsm)
print ("test2")
conn.commit()
conn.close()
