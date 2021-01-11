from pathlib import Path
import pandas as pd
import sqlite3
from pyexcelerate import Workbook
from datetime import date


def sqlcopybl(dbs, dbt):
    db1 = Path('C:/sqlite/2020' + str(dbs) + '_sqlite.db')
    db2 = Path('C:/sqlite/2020' + str(dbt) + '_sqlite.db')
    conn = sqlite3.connect(db1)
    c = conn.cursor()
    try:
        c.execute("ATTACH DATABASE '" + str(db2) + "' AS db_2")
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


def sqlcsvimport(datsq, tipo, tec):
    datab = Path('C:/sqlite/2020' + str(datsq) + '_sqlite.db')
    conn = sqlite3.connect(datab)
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS " + tipo)
    try:  # import baseline csv to sqlite by 10000 rows batch
        xlspath = Path('C:/xml/baseline/')  # baseline csv directory
        base = pd.read_csv(xlspath / Path('bl' + tec + '.csv'), encoding='latin-1')
        base.to_sql(tipo, conn, if_exists='append', index=False, chunksize=10000)
    except sqlite3.Error as error:  # sqlite error handling
        print('SQLite error: %s' % (' '.join(error.args)))
    c.close()
    conn.close()
    return


def sqlcsvimp031(datsq, tipo):
    datab = Path('C:/sqlite/2020' + str(datsq) + '_sqlite.db')
    conn = sqlite3.connect(datab)
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS " + tipo)
    try:  # import report LTE031 csv to sqlite by 10000 rows batch
        xlspath = Path('C:/xml/baseline/031/')  # 031 csv directory
        for baself in xlspath.glob('*.csv'):  # file iteration inside directory
            tempo = pd.read_csv(baself, sep=';', chunksize=50000)  # csv read in 50K rows blocks
            for chunk in tempo:
                chunk.to_sql(tipo, conn, if_exists='append', index=False, chunksize=10000)
    except sqlite3.Error as error:  # sqlite error handling
        print('SQLite error: %s' % (' '.join(error.args)))
    c.close()
    conn.close()
    return


def sqltabexport(datsq, tabs1, filenam):
    datab = Path('C:/sqlite/2020' + str(datsq) + '_sqlite.db')
    today = date.today()
    xls_file = filenam + '_' + today.strftime("%y%m%d") + ".xlsx"
    xls_path = datab.parent / xls_file  # xls file path-name
    conn = sqlite3.connect(datab)  # database connection
    c = conn.cursor()
    wb = Workbook()
    for i in tabs1:
        try:
            df = pd.read_sql_query("select * from " + i + ";", conn)  # pandas dataframe from sqlite
            data = [df.columns.tolist()] + df.values.tolist()  # dataframe to list to pyexcelerate save
            wb.new_sheet(i, data=data)
        except sqlite3.Error as error:  # sqlite error handling
            print('SQLite error: %s' % (' '.join(error.args)))
    c.close()
    conn.close()
    wb.save(xls_path)
    return


def amleprsqlconcat(tabs1, conn):
    # datab = Path('C:/sqlite/2020' + str(datsq) + '_sqlite.db')
    # today = date.today()
    # xls_file = filenam + '_' + today.strftime("%y%m%d") + ".xlsx"
    # xls_path = datab.parent / xls_file  # xls file path-name
    # conn = sqlite3.connect(datab)  # database connection
    # c = conn.cursor()
    # wb = Workbook()
    dfconcat = pd.DataFrame()
    for i in tabs1:
        try:
            df = pd.read_sql_query("select * from " + i + ";", conn)  # pandas dataframe from sqlite
            dfconcat = dfconcat.append(df, ignore_index=True)
        except sqlite3.Error as error:  # sqlite error handling
            print('SQLite error: %s' % (' '.join(error.args)))
    data = [dfconcat.columns.tolist()] + dfconcat.values.tolist()  # dataframe to list to pyexcelerate save
    # wb.new_sheet(filenam, data=data)
    # c.close()
    # conn.close()
    # # wb.save(xls_path)
    return data


def concat(tec):
    xlspath = Path('C:/xml/baseline/' + tec)  # tec baselines from RF page directory
    conca = pd.DataFrame()
    for baself in xlspath.glob('*.xls'):  # file iteration inside directory
        tempo = pd.read_html(str(baself))
        conca = conca.append(tempo)
    conca.to_csv(xlspath.parent / Path('bl' + tec + '.csv'))
    return


dateini = 1217
datesq = 1218

# stage1
concat('UMTS')
concat('LTE')
concat('Sitios')
concat('GSM')
sqlcsvimport(datesq, 'baseline', 'Sitios')
sqlcsvimport(datesq, 'Baseline_LTE', 'LTE')
sqlcsvimport(datesq, 'Baseline_UMTS', 'UMTS')
sqlcsvimport(datesq, 'Baseline_GSM', 'GSM')
#
# stage2
# sqlcsvimp031(datesq, 'RSLTE031')
# sqlcopybl(dateini, datesq)
# stage3
# proc = [3]
# proc = [1, 2, 3, 4]
# for iter1 in proc:
#     if iter1 == 1:
#         tabs = ['LNREL_DISC_700', 'LNREL_PART_NOCOLOC', 'LNREL_PART_NOCOSCTR', 'LNREL_PART_NOCOSITE',
#                 'LNREL_PART_UNDFND', 'LNMME_Miss', 'PCI_DistF1', 'RSI_DistF1', 'LTE_Param', 'WCEL_PARAM1',
#                 'BTS_PARAM']
#         filen = 'Mob_Audit'
#         sqltabexport(datesq, tabs, filen)
#     elif iter1 == 2:
#         tabs = ['T031_PAR_LNRELS']
#         filen = '031_LNREL'
#         sqltabexport(datesq, tabs, filen)
#     elif iter1 == 3:
#         tabs = ['IRFIM_Miss', 'AMLEPR_MISS', 'LNREL_COS_MISS', 'ADJL_AUD9560', 'ADJL_AUD9560G', 'ADJL_AUD626', 'ADJL_AUD626G',
#                 'ADJL_AUD651', 'ADJL_AUD651G', 'ADJL_AUD3075', 'ADJL_AUD3075G', 'ADJL_AUD3225', 'ADJL_AUD3225G']
#         filen = 'IRFIM_ADJL_Missing'
#         sqltabexport(datesq, tabs, filen)
#     elif iter1 == 4:
#         filet = 'LTE2051_1841_Disc'
#         datab = Path('C:/sqlite/2020' + str(datesq) + '_sqlite.db')
#         today = date.today()
#         xls_file = filet + '_' + today.strftime("%y%m%d") + ".xlsx"
#         xls_path = datab.parent / xls_file  # xls file path-name
#         conn = sqlite3.connect(datab)  # database connection
#         c = conn.cursor()
#         wb = Workbook()
#         tabs = ['IRFIM_626AUD', 'IRFIM_651AUD', 'IRFIM_9560AUD', 'IRFIM32253075AUD', 'IRFIM30753225AUD',
#                 'IRFIM_3075AUD', 'IRFIM_3225AUD']
#         filen = 'IRFIM_DISC'
#         data1 = amleprsqlconcat(tabs, conn)
#         wb.new_sheet(filen, data=data1)
#         tabs = ['AMLEPR_3075_3225', 'AMLEPR_3075_651', 'AMLEPR_3075_626', 'AMLEPR_3075_9560', 'AMLEPR_3225_3075',
#                 'AMLEPR_3225_651', 'AMLEPR_3225_626', 'AMLEPR_3225_9560', 'AMLEPR_651_3075', 'AMLEPR_651_3225',
#                 'AMLEPR_651_626', 'AMLEPR_651_9560', 'AMLEPR_626_3075', 'AMLEPR_626_3225', ' AMLEPR_626_651',
#                 'AMLEPR_626_9560', 'AMLEPR_9560_3075', 'AMLEPR_9560_3225', 'AMLEPR_9560_651', 'AMLEPR_9560_626']
#         filen = 'AMLEPR_DISC'
#         data1 = amleprsqlconcat(tabs, conn)
#         wb.new_sheet(filen, data=data1)
#         tabs = ['LNCEL_IDCONGEN_15_20', 'LNCEL_IDCONGEN_10', 'LNCEL_IDCONGEN_5']  # next audit
#         filen = 'LNCEL_IDCONGEN'
#         data1 = amleprsqlconcat(tabs, conn)
#         wb.new_sheet(filen, data=data1)
#         tabs = ['LNCEL_AUD1841_15_20', 'LNCEL_AUD1841_10', 'LNCEL_AUD1841_5']  # next audit
#         filen = 'LNCEL_2051_1841'
#         data1 = amleprsqlconcat(tabs, conn)
#         wb.new_sheet(filen, data=data1)
#         tabs = ['LNBTS_AUD2051']
#         filen = 'WBTS_DISC'
#         data1 = amleprsqlconcat(tabs, conn)
#         wb.new_sheet(filen, data=data1)
#         c.close()
#         conn.close()
#         wb.save(xls_path)
#     elif iter1 == 5:
#         tabs = ['T031_PAR_LNRELT']
#         filen = '031_LNREL'
#         sqltabexport(datesq, tabs, filen)
#     elif iter1 == 6:
#         tabs = ['LNCEL_Full', 'IRFIM_ref']
#         filen = 'IRFIM'
#         sqltabexport(datesq, tabs, filen)
print('ok')



