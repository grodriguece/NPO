import os
from pathlib import Path
import pandas as pd
import sqlite3
from pyexcelerate import Workbook
from datetime import date


def sqlcopybl(db1, db2):
    conn = sqlite3.connect(db1)  # connection to src db
    c = conn.cursor()
    try:
        c.execute("ATTACH DATABASE '" + str(db2) + "' AS db_2")  # add conn to tgt db
        c.execute("DROP TABLE IF EXISTS db_2.baseline")
        c.execute("DROP TABLE IF EXISTS db_2.Baseline_UMTS")
        c.execute("DROP TABLE IF EXISTS db_2.Baseline_LTE")
        c.execute("DROP TABLE IF EXISTS db_2.Baseline_GSM")
        c.execute("DROP TABLE IF EXISTS db_2.Baseline_700FU")
        c.execute("DROP TABLE IF EXISTS db_2.Baseline_LTemp")
        c.execute("DROP TABLE IF EXISTS db_2.Baseline_LTempF")
        c.execute("CREATE TABLE db_2.baseline AS SELECT * FROM baseline")
        c.execute("CREATE TABLE db_2.Baseline_UMTS AS SELECT * FROM Baseline_UMTS")
        c.execute("CREATE TABLE db_2.Baseline_LTE AS SELECT * FROM Baseline_LTE")
        c.execute("CREATE TABLE db_2.Baseline_GSM AS SELECT * FROM Baseline_GSM")
        c.execute("CREATE TABLE db_2.Baseline_700FU AS SELECT * FROM Baseline_700FU")
        c.execute("CREATE TABLE db_2.Baseline_LTemp AS SELECT * FROM Baseline_LTemp")
        c.execute("CREATE TABLE db_2.Baseline_LTempF AS SELECT * FROM Baseline_LTempF")
    except sqlite3.Error as error:  # sqlite error handling
        print('SQLite error: %s' % (' '.join(error.args)))
    c.close()
    conn.close()
    return


def sqlcsvimport(c, tipo, tec):  # cursor from tgt db
    c.execute("DROP TABLE IF EXISTS " + tipo)
    try:  # import baseline csv to sqlite by 10000 rows batch
        xlspath = Path('C:/xml/baseline/')  # baseline csv directory
        base = pd.read_csv(xlspath / Path('bl' + tec + '.csv'), encoding='latin-1')
        base.to_sql(tipo, conn, if_exists='append', index=False, chunksize=10000)
    except sqlite3.Error as error:  # sqlite error handling
        print('SQLite error: %s' % (' '.join(error.args)))
    return


def sqlcsvimpNAct(c, loc, tipo):  # cursor from tgt db
    c.execute("DROP TABLE IF EXISTS " + tipo)
    try:  # import report LTE031 csv to sqlite by 10000 rows batch
        xlspath = Path('C:/xml/baseline/' + loc + '/')  # 031 csv directory
        for baself in xlspath.glob('*.csv'):  # file iteration inside directory
            tempo = pd.read_csv(baself, sep=';', chunksize=50000)  # csv read in 50K rows blocks
            for chunk in tempo:
                chunk.to_sql(tipo, conn, if_exists='append', index=False, chunksize=10000)
    except sqlite3.Error as error:  # sqlite error handling
        print('SQLite error: %s' % (' '.join(error.args)))
    return


def sqltabexport(conn1, tabs1, filenam, datab):
    today = date.today()
    xls_file = filenam + '_' + today.strftime("%y%m%d") + ".xlsx"
    xls_path = datab.parent / 'xlsx' / xls_file  # xls file path-name
    wb = Workbook()
    for i in tabs1:
        try:
            df = pd.read_sql_query("select * from " + i + ";", conn1)  # pandas dataframe from sqlite
            data = [df.columns.tolist()] + df.values.tolist()  # dataframe to list to pyexcelerate save
            wb.new_sheet(i, data=data)
        except sqlite3.Error as error:  # sqlite error handling
            print('SQLite error: %s' % (' '.join(error.args)))
    wb.save(xls_path)
    return


def amleprsqlconcat(tabs1, conn2):
    dfconcat = pd.DataFrame()
    for i in tabs1:
        try:
            df = pd.read_sql_query("select * from " + i + ";", conn2)  # pandas dataframe from sqlite
            dfconcat = dfconcat.append(df, ignore_index=True)
        except sqlite3.Error as error:  # sqlite error handling
            print('SQLite error: %s' % (' '.join(error.args)))
    data = [dfconcat.columns.tolist()] + dfconcat.values.tolist()  # dataframe to list to pyexcelerate save
    return data


def concat(cspath, tec):
    xlspath = cspath / tec  # tec baselines from RF page directory
    conca = pd.DataFrame()
    for baself in xlspath.glob('*.xls'):  # file iteration inside directory
        tempo = pd.read_html(str(baself))
        conca = conca.append(tempo)
        try:
            os.remove(baself)  # remove src xls file
        except:
            print("Error while deleting file : ", baself)
    conca.to_csv(xlspath.parent / Path('bl' + tec + '.csv'))
    return


dateini = '0109'
dbsrc = Path('C:/sqlite/2021' + dateini + '_sqlite.db')
datesq = '0110'
dbtgt = Path('C:/sqlite/2021' + datesq + '_sqlite.db')
#
#
# stage1
#
# conn = sqlite3.connect(dbtgt)
# cur = conn.cursor()
# csvpath = Path('C:/xml/baseline/')  # tec baselines from RF page directory
# for baself in csvpath.glob('*.csv'):  # file iteration inside directory
#     try:
#         os.remove(baself)  # remove old csv files
#     except:
#         print("Error while deleting file : ", baself)
# concat(csvpath, 'UMTS')
# concat(csvpath, 'LTE')
# concat(csvpath, 'Sitios')
# concat(csvpath, 'GSM')
# sqlcsvimport(cur, 'baseline', 'Sitios')
# sqlcsvimport(cur, 'Baseline_LTE', 'LTE')
# sqlcsvimport(cur, 'Baseline_UMTS', 'UMTS')
# sqlcsvimport(cur, 'Baseline_GSM', 'GSM')
# cur.close()
# conn.close()
#
#
# stage2
#
# conn = sqlite3.connect(dbtgt)
# cur = conn.cursor()
# sqlcsvimpNAct(cur, '031', 'RSLTE031')
# cur.close()
# conn.close()
# sqlcopybl(dbsrc, dbtgt)
#
#
# stage3
#
# proc = [8]
proc = [1, 2, 3, 4]
cont = sqlite3.connect(dbtgt)  # database connection for all iterations
cur = cont.cursor()
for iter1 in proc:
    if iter1 == 1:
        tabs = ['LNREL_DISC_700', 'LNREL_PART_NOCOLOC', 'LNREL_PART_NOCOSCTR', 'LNREL_PART_NOCOSITE',
                'LNREL_PART_UNDFND', 'LNMME_Miss', 'PCI_DistF1', 'RSI_DistF1', 'LTE_Param', 'WCEL_PARAM1',
                'BTS_PARAM']
        filen = 'Mob_Audit'
        sqltabexport(cont, tabs, filen, dbtgt)
    elif iter1 == 2:
        tabs = ['T031_PAR_LNRELS']
        filen = '031_LNREL'
        sqltabexport(cont, tabs, filen, dbtgt)
    elif iter1 == 3:
        tabs = ['IRFIM_Miss', 'AMLEPR_MISS', 'LNREL_COS_MISS', 'ADJL_DISC', 'ADJL_AUD9560', 'ADJL_AUD9560G',
                'ADJL_AUD626', 'ADJL_AUD626G', 'ADJL_AUD651', 'ADJL_AUD651G', 'ADJL_AUD3075', 'ADJL_AUD3075G',
                'ADJL_AUD3225', 'ADJL_AUD3225G']
        filen = 'IRFIM_ADJL_Missing'
        sqltabexport(cont, tabs, filen, dbtgt)
    elif iter1 == 4:
        filet = 'LTE2051_1841_Disc'
        today = date.today()
        xls_file = filet + '_' + today.strftime("%y%m%d") + ".xlsx"
        xls_path = dbtgt.parent / 'xlsx' /xls_file  # xls file path-name
        wb = Workbook()
        tabs = ['IRFIM_626AUD', 'IRFIM_651AUD', 'IRFIM_9560AUD', 'IRFIM32253075AUD', 'IRFIM30753225AUD',
                'IRFIM_3075AUD', 'IRFIM_3225AUD']
        filen = 'IRFIM_DISC'
        data1 = amleprsqlconcat(tabs, cont)
        wb.new_sheet(filen, data=data1)
        tabs = ['AMLEPR_3075_3225', 'AMLEPR_3075_651', 'AMLEPR_3075_626', 'AMLEPR_3075_9560', 'AMLEPR_3225_3075',
                'AMLEPR_3225_651', 'AMLEPR_3225_626', 'AMLEPR_3225_9560', 'AMLEPR_651_3075', 'AMLEPR_651_3225',
                'AMLEPR_651_626', 'AMLEPR_651_9560', 'AMLEPR_626_3075', 'AMLEPR_626_3225', ' AMLEPR_626_651',
                'AMLEPR_626_9560', 'AMLEPR_9560_3075', 'AMLEPR_9560_3225', 'AMLEPR_9560_651', 'AMLEPR_9560_626']
        filen = 'AMLEPR_DISC'
        data1 = amleprsqlconcat(tabs, cont)
        wb.new_sheet(filen, data=data1)
        tabs = ['LNCEL_IDCONGEN_15_20', 'LNCEL_IDCONGEN_10', 'LNCEL_IDCONGEN_5']  # next audit
        filen = 'LNCEL_IDCONGEN'
        data1 = amleprsqlconcat(tabs, cont)
        wb.new_sheet(filen, data=data1)
        tabs = ['LNCEL_AUD1841_15_20', 'LNCEL_AUD1841_10', 'LNCEL_AUD1841_5']  # next audit
        filen = 'LNCEL_2051_1841'
        data1 = amleprsqlconcat(tabs, cont)
        wb.new_sheet(filen, data=data1)
        tabs = ['LNBTS_AUD2051']
        filen = 'WBTS_DISC'
        data1 = amleprsqlconcat(tabs, cont)
        wb.new_sheet(filen, data=data1)
        wb.save(xls_path)
    elif iter1 == 5:
        tabs = ['T031_PAR_LNRELT']
        filen = '031_LNREL'
        sqltabexport(cont, tabs, filen, dbtgt)
    elif iter1 == 6:
        tabs = ['LNCEL_Full', 'IRFIM_ref']
        filen = 'IRFIM'
        sqltabexport(cont, tabs, filen, dbtgt)
    elif iter1 == 7:
        tabs = ['MISS3', 'ADJS_Add', 'ADJS_Dep']
        filen = 'ADJS_OPT'
        sqltabexport(cont, tabs, filen, dbtgt)
    elif iter1 == 8:
        tabs = ['T031_LNREL_ATL', 'T031_LNREL_BOL', 'T031_LNREL_MGC', 'T031_LNREL_SC', 'T031_LNREL_OTHER']
        filen = 'T031_LNREL_RC10'
        sqltabexport(cont, tabs, filen, dbtgt)
cur.close()
cont.close()
#
#
# stage4 Tables for ADJS Missing
#
# cont = sqlite3.connect(dbtgt)  # database connection
# cur = cont.cursor()
# sqlcsvimpNAct(cur, 'DET', 'DET_SET')
# sqlcsvimpNAct(cur, 'DRAFMS', 'DRP_AFT_MISSING')
# sqlcsvimpNAct(cur, '046', 'RSRAN046')
# cur.close()
# cont.close()
#
#
print('ok')




