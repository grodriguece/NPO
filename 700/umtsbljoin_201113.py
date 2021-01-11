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


def concat(tec):
    xlspath = Path('C:/xml/baseline/' + tec)  # call logs directory
    conca = pd.DataFrame()
    for baself in xlspath.glob('*.csv'):  # file iteration inside directory
        tempo = pd.read_csv(baself, encoding='latin-1')
        conca = conca.append(tempo)
    conca.to_csv(xlspath.parent / Path('bl' + tec + '.csv'))
    return


# stage1
# concat('UMTS')
# concat('LTE')
# concat('Sitios')
# concat('GSM')
# sqlcsvimport(datesq, 'baseline', 'Sitios')
# sqlcsvimport(datesq, 'Baseline_LTE', 'LTE')
# sqlcsvimport(datesq, 'Baseline_UMTS', 'UMTS')
# sqlcsvimport(datesq, 'Baseline_GSM', 'GSM')
#
dateini = 1112
datesq = 1113
# stage2
# sqlcsvimp031(datesq, 'RSLTE031')
# sqlcopybl(dateini, datesq)
# stage3
proc = [1, 2, 3]
for iter1 in proc:
    if iter1 == 1:
        tabs = ['LNREL_PART_NOCOLOC', 'LNREL_PART_NOCOSCTR', 'LNREL_PART_NOCOSITE', 'LNREL_PART_UNDFND',
                'LNMME_Miss', 'PCI_DistF1', 'RSI_DistF1', 'LTE_Param', 'WCEL_PARAM1']
        filen = 'Mob_Audit'
        sqltabexport(datesq, tabs, filen)
    elif iter1 == 2:
        tabs = ['T031_PAR_LNRELS']
        filen = '031_LNREL'
        sqltabexport(datesq, tabs, filen)
    elif iter1 == 3:
        tabs = ['ADJL_AUD9560', 'ADJL_AUD626', 'ADJL_AUD651', 'ADJL_AUD3075', 'ADJL_AUD3225']
        filen = 'ADJL_Missing'
        sqltabexport(datesq, tabs, filen)
    elif iter1 == 4:
        tabs = ['T031_PAR_LNRELT']
        filen = '031_LNREL'
        sqltabexport(datesq, tabs, filen)
print('ok')
