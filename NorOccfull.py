from pathlib import Path
from rfpack.pasarchivoc import pasarchivo
from tkinter import *
from tkinter import ttk
from PIL import ImageTk, Image
from tkinter import messagebox
import numpy as np
import pandas as pd
import sqlite3
from rfpack.switcherc import *
from rfpack.statzonc import statzon
from rfpack.par_auditc import par_audit
from rfpack.cleaniparmc import cleaniparm, cleaniparm2
from plotnine import *
from mizani.transforms import trans


def tabconv(tabsent):
    switcher = {
        'LNCEL': 'LNCEL_Full',
        'LNHOIF': 'LNHOIF_ref',
        'LNBTS': 'LNBTS_Full',
        'LNHOW': 'LNHOW_ref',
        'WCEL': 'WCEL_FULL1',
        'RNFC': 'RNFC_ref',
        'AMLEPR': 'AMLEPR_ref',
        'ANRPRL': 'ANRPRL_ref',
        'LNREL': 'LNREL_NO',
        'IRFIM': 'IRFIM_ref',
        'UFFIM_UTRFDDCARFRQL': 'UFFIM_UTRFDDCARFRQL_ref',
        'IAFIM_INTRFRNCLIST': 'IAFIM_INTRFRNCLIST_ref',
        'UFFIM': 'UFFIM_ref',
    }
    return switcher.get(tabsent, 'nothing')  # 'nothing' if not found


class asinh_trans(trans):
    """
        asinh Transformation
        """

    @staticmethod
    def transform(y):
        y = np.asarray(y)
        return np.arcsinh(y)

    @staticmethod
    def inverse(y):
        y = np.asarray(y)
        return np.sinh(y)


def customparam(datb, tab_par, iterini, root1, my_progress1, proglabel21):
    dat_dir = datb.parent
    (dat_dir / 'csv').mkdir(parents=True, exist_ok=True)  # create csv folder to save temp files
    lst = []  # list with xlsx sheet order
    dict1 = {'id': str(1), 'name': 'Total'}
    lst.append(dict1.copy())
    shorder = 2  # xlsx sheet order
    pnglist = []
    consfull = []
    conspref = []
    conn = sqlite3.connect(datb)  # database connection
    c = conn.cursor()
    ftab1 = tab_par + '.csv'  # tables and parameters to audit
    df3 = pd.read_csv(dat_dir / ftab1)
    df4 = df3.groupby('table_name')['parameter'].apply(list).reset_index(name='parlist')
    tabqty = len(df4)
    for index, row in df4.iterrows(): # table row iteration
        my_progress1['value'] = iterini + round(index / tabqty * 53)  # prog bar up to iterini +53
        proglabel21.config(text=my_progress1['value'])  # prog bar updt
        root1.update_idletasks()
        line = row['table_name']
        namtoinx = 'LNCELname'    # default values for lncel related tables
        carrfilt = 'earfcnDL'
        if line == 'RNFC' or line == 'LNBTS':  # carrier count - amount of graphs 1 for BTS
            n = 1    # 2 individual tables
        else:
            n = 5   # 11 tables with carries to graph
        paramst1 = row['parlist']  # parameter list
        if line == 'WCEL': # id parameter definition
            paramsext = ('Prefijo', 'WBTS_id', 'UARFCN', 'WCELName', 'Banda', 'Encargado')
            namtoinx = 'WCELName'
            carrfilt = 'UARFCN'
        elif line == 'ANRPRL':
            paramsext = ('Prefijo', 'LNBTS_id', 'LNBTSname', 'Banda', 'Encargado')
            namtoinx = 'LNBTSname'
            carrfilt = 'targetCarrierFreq'
        elif line == 'RNFC':
            paramsext = ('Prefijo', 'RNC_id', 'RNCName', 'Encargado')
            namtoinx = 'RNCName'
            carr = 'RNC'
        elif line == 'LNBTS':
            paramsext = ('Prefijo', 'LNBTSname', 'Cluster', 'Region', 'Depto', 'Mun', 'PLMN_id',
                         'MRBTS_id', 'LNBTS_id')
            namtoinx = 'LNBTSname'
            carr = 'LNBTS'
        elif line == 'LNCEL' or line == 'AMLEPR':
            paramsext = ('Prefijo', 'LNCELname', 'Cluster', 'Region', 'Depto', 'Mun', 'PLMN_id',
                         'Banda', 'MRBTS_id', 'LNBTS_id', 'LNCEL_id', 'LNBTSname')
        # elif line == 'AMLEPR':
        #     paramsext = ('LNCELname', 'Cluster', 'Region', 'Depto', 'Mun', 'PLMN_id', 'Prefijo',
        #                  'Banda', 'MRBTS_id', 'LNBTS_id', 'LNCEL_id', 'LNBTSname')
        else:  # add columns to include in table query
            paramsext = ('Prefijo', 'LNBTS_id', 'earfcnDL', 'LNCELname', 'Banda', 'Encargado')
        paramst1.extend(paramsext)
        parstring = ','.join(paramst1)
        tabsq = tabconv(line)  # select reference table to get info
        try:  # include queries for all and carrier, pending
            datsrc = pd.read_sql_query("select " + parstring + " from " + tabsq + ";", conn,
                                       index_col=[namtoinx, 'Prefijo'])
            datsrc.to_csv(dat_dir / 'csv' / Path(line + '_data.csv'))
            dict1 = {'id': str(shorder), 'name': line + '_data'}
            lst.append(dict1.copy())  # saves raw info
            shorder += 1
            if not (line == 'LNBTS' or line == 'RNFC' or line == 'WBTS'):
                datsrc = datsrc.dropna(subset=['Banda'])  # cleans NaN band registers
            for i in range(0, n):  # loop for each carrier. once for no carrier tables
                if line == 'WCEL': # unique UMTS table
                    carr = carriers(i)
                    cart = carrtext(i)
                else:   # add columns to include in table query
                    carr = carrierl(i)  # carrier number
                    cart = carrtexl(i)  # carrier name
                if line == 'LNBTS' or line == 'RNFC' or line == 'WBTS' or carr == 'all':
                    df2 = datsrc
                else:
                    df2 = datsrc[:][datsrc[carrfilt] == carr]
                if len(df2) > 0:  # control for empty df
                    stpref = statzon(df2)  # stats per parameter and prefijo
                    st = par_audit(df2)  # stats per parameter full set
                    df2, st = cleaniparm(dat_dir, "ExParam.csv", "expfeat1", df2, st)  # info parameter removal
                    if line == 'LNBTS' or line == 'RNFC' or line == 'WBTS' or carr == 'all':
                        sttemp = st.copy(deep=True)
                        sttemp.insert(0, 'table', line)
                        sttemp1 = stpref.copy(deep=True)
                        sttemp1.insert(0, 'table', line)
                        if len(consfull) == 0:  # empty stzf control
                            consfull = sttemp   # review info to png
                        else:
                            consfull = consfull.append(sttemp)
                        if len(conspref) == 0:  # empty stzf control
                            conspref = sttemp1  # pref review info to png
                        else:
                            conspref = conspref.append(sttemp1)
                    else:
                        st.to_csv(dat_dir / 'csv' / Path(line + str(carr) + '.csv'))
                        dict1 = {'id': str(shorder), 'name': line + str(carr)}
                        lst.append(dict1.copy())
                        shorder += 1
                        stpref.to_csv(dat_dir / 'csv' / Path(line + str(carr) + 'pref' + '.csv'))
                        dict1 = {'id': str(shorder), 'name': line + str(carr) + 'pref'}
                        lst.append(dict1.copy())
                        shorder += 1
                    df2, st = cleaniparm2(df2, st)  # standardized params and NaN>0.15*n removal
                    parqty = len(st)   # parameter amount
                    if parqty > 0: # only for parameters with discrepancies
                        st['topdisc'] = range(parqty)  # top disc counter by IQR-CV
                        st['topdisc'] = st['topdisc'].floordiv(10)  # split disc in groups by 10
                        st.sort_values(by=['Median'], inplace=True, ascending=[False])  # for better visualization
                        st['counter'] = range(parqty)  # counter controls number of boxplots
                        st['counter'] = st['counter'].floordiv(10)  # split parameters in groups by 10
                        cols = ['StdDev', 'mean', 'Median', 'upper', 'lower', 'CV']
                        st[cols] = st[cols].round(1)  # scales colums with 1 decimal digit
                        stpref[cols] = stpref[cols].round(1)  # Prefijo info
                        # concat info to put text in boxplots
                        st['concat'] = st['StdDev'].astype(str) + ', ' + st['NoModeQty'].astype(str)
                        stpref['concat'] = stpref['StdDev'].astype(str) + ', ' + stpref['NoModeQty'].astype(str)
                        ldcol = list(st.index)  # parameters to include in melt command
                        # Structuring df2 according to ‘tidy data‘ standard
                        df2 = df2.reset_index()  # to use indexes in melt operation
                        df1 = df2.melt(id_vars=['Prefijo'], value_vars=ldcol,  # WCELName is not used
                                       var_name='parameter', value_name='value')
                        df1 = df1.dropna(subset=['value'])  # drop rows with value NaN
                        st.reset_index(inplace=True)  # parameter from index to col
                        stpref.reset_index(inplace=True)  # parameter from index to col
                        temp = st[['parameter', 'topdisc']]  # topdisc to be included in stpref
                        stpref = pd.merge(stpref, temp, on='parameter')
                        result = pd.merge(df1, st, on='parameter')  # merge by columns not by index
                        resultzon = pd.merge(df1, stpref, on=['parameter', 'Prefijo'])  # merge by columns not by index
                        # graph code
                        custom_axis = theme(axis_text_x=element_text(color="grey", size=6, angle=90, hjust=.3),
                                            axis_text_y=element_text(color="grey", size=6),
                                            plot_title=element_text(size=25, face="bold"),
                                            axis_title=element_text(size=10),
                                            panel_spacing_x=1.6, panel_spacing_y=.45,
                                            # 2nd value number of rows and colunms
                                            figure_size=(5 * 4, 3.5 * 4)
                                            )
                        # ggplot code:value 'concat' is placed in coordinate (parameter, stddev)
                        my_plot = (ggplot(data=result, mapping=aes(x='parameter', y='value')) + geom_boxplot() +
                                   geom_text(data=st, mapping=aes(x='parameter', y='StdDev', label='concat'),
                                             color='red', va='top', ha='left', size=7, nudge_x=.6, nudge_y=-1.5) +
                                   facet_wrap('counter', scales='free') + custom_axis + scale_y_continuous(
                                    trans=asinh_trans) + ylab("Values") + xlab("Parameters") +
                                   labs(title=line + " Parameter Audit " + cart) + coord_flip())
                        pngname = str(line) + str(carr) + ".png"  # saveplot
                        pngfile = dat_dir / pngname
                        my_plot.save(pngfile, width=20, height=10, dpi=300)
                        pnglist.append(pngfile)  # plots to be printed in pdf
                        if parqty < 11:
                            n = 1  # only 1 plot
                        else:
                            n = 2  # top 2 plots
                        for j in range(0, n):
                            toplot = resultzon.loc[resultzon['topdisc'] == j]  # filter info for param set to be printed
                            toplot1 = stpref.loc[stpref['topdisc'] == j]
                            custom_axis = theme(axis_text_x=element_text(color="grey", size=7, angle=90, hjust=.3),
                                                axis_text_y=element_text(color="grey", size=7),
                                                plot_title=element_text(size=25, face="bold"),
                                                axis_title=element_text(size=10),
                                                panel_spacing_x=0.6, panel_spacing_y=.45,
                                                # 2nd value number of rows and colunms
                                                figure_size=(5 * 4, 3.5 * 4)
                                                )
                            top_plot = (ggplot(data=toplot, mapping=aes(x='parameter', y='value')) + geom_boxplot()
                                        + geom_text(data=toplot1,
                                                    mapping=aes(x='parameter', y='StdDev', label='concat'),
                                                    color='red', va='top', ha='left', size=7, nudge_x=.6, nudge_y=-1.5)
                                        + facet_wrap('Prefijo') + custom_axis + scale_y_continuous(trans=asinh_trans)
                                        + ylab("Values") + xlab("Parameters")
                                        + labs(title="Top " + str(j + 1) + " Disc Parameter per Zone. " + cart)
                                        + coord_flip())
                            pngname = str(line) + str(carr) + str(j + 1) + ".png"
                            pngfile = dat_dir / pngname
                            top_plot.save(pngfile, width=20, height=10, dpi=300)
                            pnglist.append(pngfile)
        except sqlite3.Error as error:  # sqlite error handling.
            print('SQLite error: %s' % (' '.join(error.args)))
    filterpar = list(df3.parameter)
    consfull = consfull.reset_index().rename(columns={'index': 'parameter'})
    consfull['Prefijo'] = 'full'
    conspref = conspref.reset_index().rename(columns={'index': 'parameter'})
    consfull = consfull.append(conspref, ignore_index = True)  # consfull info to show CV and nomodeper
    consfull = consfull[consfull['parameter'].isin(filterpar)]  # includes only input parameters
    consfull.dropna(subset=['CV'], inplace=True)
    for index, row in consfull.iterrows():  # table row iteration by Prefijo column type
        if row['Prefijo'] in znfrmt(0):
            consfull.loc[index, 'prorder'] = 0  # update column with print order id
        elif row['Prefijo'] in znfrmt(1):
            consfull.loc[index, 'prorder'] = 1
        elif row['Prefijo'] in znfrmt(2):
            consfull.loc[index, 'prorder'] = 2
        else:
            consfull.loc[index, 'prorder'] = 3
    my_progress1['value'] = 60  # prog bar 60
    proglabel21.config(text=my_progress1['value'])  # prog bar updt
    root1.update_idletasks()
    consfull.to_csv(dat_dir / 'csv' / Path('Total.csv'), index=False)
    c.close()
    conn.close()
    return pnglist, lst


def validatab(datb, pfind, tabexc):  # findtable.csv tabcustom.csv
    import pandas as pd
    from pathlib import Path
    import sqlite3
    dat_dir = datb.parent
    output = 'tab_par.csv'  # file for tab_param output
    tex = pd.read_csv(tabexc)  # tables to exclude
    tlstex = list(tex.table_name)  # exclusion tables to a list to compare with query result
    conn = sqlite3.connect(datb)  # database connection
    c = conn.cursor()
    df1 = pd.read_csv(pfind)  # parameters to get tables
    parafind = df1.parameter.tolist() # para to find to a list for query
    try:
        # insert ? times according to parameter amount. Query only for parameters required
        quer = "select * from Fulltabcol WHERE parameter in ({})"
        df = pd.read_sql_query(quer.format(','.join(list('?' * len(parafind)))), conn, params=parafind)
        common = df[~df.table_name.isin(tlstex)]  # common only with default tables
        common.to_csv(dat_dir / output, index=False)
    except sqlite3.Error as error:  # sqlite error handling.
        print('SQLite error: %s' % (' '.join(error.args)))
        feedbk = tk.Label(top, text='SQLite error: %s' % (' '.join(error.args)))
        feedbk.pack()
    c.close()
    conn.close()




def specaud():
    from datetime import date
    from pathlib import Path
    from pyexcelerate import Workbook
    # from rfpack.validatabc import validatab
    # from rfpack.customparamc import customparam
    from rfpack.pntopdc import pntopd
    from rfpack.graffullc import graffull
    from rfpack.csvfrmxlsxc import xlsxfmcsv
    proglabel2.config(text="")  # label init
    datab = Path('C:/SQLite/20200522_sqlite.db')
    pdf_file = date.today().strftime("%y%m%d") + '_Feat1ParAudit.pdf'
    pdf_path = datab.parent / pdf_file
    xls_file = Path(pdf_path.with_suffix('.xlsx'))
    wb = Workbook()  # pyexcelerate Workbook
    fndtbl = datab.parent / Path('findtable.csv')
    tbcstm = datab.parent / Path('tabcustom.csv')
    validatab(datab, fndtbl, tbcstm)  # locate input tab/parameters in dbabase
    pnglist, sheetsdic = customparam(datab, 'tab_par', 5, root, my_progress, proglabel2) # generates png files
    # print Total info in 4 pages, 3 regions per page, bar starts at 60%
    pnglist1 = graffull(xls_file, 'Total', 4, 60, root, my_progress, proglabel2)
    pnglist1.extend(pnglist)  # review png at the beginning
    pntopd(pdf_path, pnglist1, 50, 550, 500, 500)  # png to pdf
    xlsxfmcsv(xls_file, sheetsdic, 75, root, my_progress, proglabel2 )
    my_progress['value'] = 100  # prog bar increase a cording to i steps in loop
    proglabel2.config(text=my_progress['value'])
    response = messagebox.showinfo("Specific Audit", "Process Finished")
    proglabel3 = Label(root, text=response)
    my_progress['value'] = 0  # prog bar increase according to i steps in loop
    proglabel2.config(text="   ")
    root.update_idletasks()
    # try to put progress inside routine and avoid to exit from app







class asinh_trans(trans):
    """
        asinh Transformation
        """

    @staticmethod
    def transform(y):
        y = np.asarray(y)
        return np.arcsinh(y)

    @staticmethod
    def inverse(y):
        y = np.asarray(y)
        return np.sinh(y)


def par_audit(df):
    import functools
    import pandas as pd
    from rfpack.iqrcalcc import iqrcalc

    df = df.copy(deep=True)  # Modifications to the data of the copy wont be reflected in the orig object
    # n = len(df.index)  # row count
    # mode stored in columns
    modes = df.mode(dropna=False)
    # dummy rows delete
    modes = modes.dropna(subset=['PLMN_id'])
    # dictionaries. data (count values diff from mode in modes) data1 (count of values = mode in modes)
    data = {col: (~df[col].isin(modes[col])).sum() for col in df.iloc[:, 0:].columns}
    data1 = {col: df[col].isin(modes[col]).sum() for col in df.iloc[:, 0:].columns}
    # st3 mode info
    st3 = pd.DataFrame.from_dict(data, orient='index', columns=['NoModeQty'])
    st3['ModeQty'] = pd.DataFrame.from_dict(data1, orient='index')
    st3['NoModePer'] = 100 * (st3['NoModeQty'] / (st3['ModeQty'] + st3['NoModeQty']))
    # st3.index.name = 'parameter'
    st2 = modes.T
    st2.columns = ['Mode']
    # st2.index.name = 'parameter'
    st2 = st2.merge(st3, how='left', left_index=True, right_index=True)
    # st1 = pd.DataFrame({'Vmin': df.min(), 'StdDev': df.std(), 'NaNQty': df.isnull().sum(axis=0), 'Mean': df.mean(),
    #                     'Q1': df.quantile(.25), 'Q3': df.quantile(.75), 'Median': df.quantile(.5)})
    # st1[['Max', 'Min', 'IQR', 'CV']] = st1.apply(lambda row: iqrcalc(row['Q1'], row['Q3'], n, row['StdDev'],
    #                                                                  row['Mean']), axis=1, result_type='expand')
    df2 = df.describe().T
    df2.rename(columns={'25%': 'Q1', '50%': 'Median', '75%': 'Q3', 'std': 'StdDev',
                        'count': 'n', 'min': 'Vmin'}, inplace=True)
    st1 = pd.DataFrame({'NaNQty': df.isnull().sum(axis=0)})
    st1 = df2.merge(st1, how='left', left_index=True, right_index=True)
    st1[['upper', 'lower', 'IQR', 'CV']] = st1.apply(lambda row: iqrcalc(row['Q1'], row['Q3'], row['n'], row['StdDev'],
                                                                         row['mean']), axis=1, result_type='expand')
    st4 = st1.merge(st2, how='left', left_index=True, right_index=True)
    st4.index.name = 'parameter'
    # st1.index.name = 'parameter'
    # df merge
    # dfs = [st1, st2, st3]
    # st4 = functools.reduce(lambda left, right: pd.merge(left, right, on='parameter'), dfs)
    st4.sort_values(by=['IQR', 'CV'], inplace=True, ascending=[False, False])
    return st4



def statzon(df):
    from rfpack.zonec import zone
    # from rfpack.par_auditc import par_audit  # traido

    df = df.copy(deep=True)  # Modifications to the data of the copy wont be reflected in the orig object
    dftemp = df.reset_index(level=(0, 1))
    stzf = []
    n = 10  # zone number
    for i in range(0, n):  # loop for each zone
        area = zone(i)
        dfzi = dftemp[:][dftemp.Prefijo == area]  # data per zone
        if len(dfzi) > 0:  # control for empty df
            if i == 0:
                stzf = par_audit(dfzi)
                stzf['Prefijo'] = area
            else:
                stz = par_audit(dfzi)
                stz['Prefijo'] = area
                if len(stzf) == 0:  # empty stzf control
                    stzf = stz
                else:
                    stzf = stzf.append(stz)
    return stzf



def par_aud(datab, tablas, tipo, iterini, root1, my_progress1, proglabel21):

    import pandas as pd
    from pyexcelerate import Workbook
    import pyexcelerate_to_excel
    from datetime import date
    import sqlite3
    from rfpack.carriersc import carriers
    from rfpack.carrierlc import carrierl
    from rfpack.carrtextc import carrtext
    from rfpack.carrtexlc import carrtexl
    # from rfpack.statzonc import statzon
    # from rfpack.par_auditc import par_audit
    from rfpack.cleaniparmc import cleaniparm
    from rfpack.cleaniparm2c import cleaniparm2
    from rfpack.pntopdc import pntopd

    dat_dir = datab.parent
    conn = sqlite3.connect(datab)  # database connection
    c = conn.cursor()
    df1 = pd.read_csv(tablas)
    today = date.today()
    # xls_file = tipo + today.strftime("%y%m%d") + ".xlsx"
    # xls_path = dat_dir / xls_file  # xls file path-name
    wb = Workbook()  # pyexcelerate Workbook
    pnglist = []
    tit = today.strftime("%y%m%d") + '_ParameterAudit'
    xls_file = tit + ".xlsx"
    xls_path = dat_dir / xls_file
    pdf_file = tit + ".pdf"
    pdf_path = dat_dir / pdf_file
    for index, row in df1.iterrows():  # table row iteration by audit2 column type
        my_progress1['value'] = iterini + round(index / len(df1 * 75))        # prog bar up to iterini + 75
        proglabel21.config(text=my_progress1['value'])  # prog bar updt
        root1.update_idletasks()
        line = row[tipo]
        if not pd.isna(row[tipo]):  # nan null values validation
            if line == 'LNCEL' or line == 'WCEL' : # carrier count - amount of graphs 1 for BTS
                n = 5
            elif line == 'LNBTS' or line == 'WBTS' :
                n = 1
            for i in range(0, n):  # loop for each carrier
                if line == 'LNBTS' or line == 'WBTS':
                    cart = ''
                    if line == 'LNBTS':
                        carr = 'LNBTS'
                    else:
                        carr = 'WBTS'
                elif line == 'LNCEL':
                    carr = carrierl(i) # carrier number
                    cart = carrtexl(i)
                elif line == 'WCEL':
                    carr = carriers(i)
                    cart = carrtext(i)
                try:
                    if line == 'LNBTS':
                        df = pd.read_sql_query("select * from LNBTS_Full;", conn, index_col=['LNBTSname', 'Prefijo'])
                    elif line == 'WBTS':
                        df = pd.read_sql_query("select * from WBTS_Full1;", conn, index_col=['WBTSName', 'Prefijo'])
                    elif line == 'LNCEL':
                        if carr == 'all':
                            df = pd.read_sql_query("select * from LNCEL_Full;", conn, index_col=['LNCELname', 'Prefijo'])
                        else:
                            df = pd.read_sql_query("select * from LNCEL_Full where (earfcnDL = " + str(carr) + ");",
                                                   conn, index_col=['LNCELname', 'Prefijo'])
                        df = df.dropna(subset=['Banda'])   # drop rows with band nan
                    elif line == 'WCEL':
                        if carr == 'all':
                            df = pd.read_sql_query("select * from WCEL_FULL1;", conn, index_col=['WCELName', 'Prefijo'])
                        else:
                            df = pd.read_sql_query("select * from WCEL_FULL1 where (UARFCN = " + str(carr) + ");",
                                                   conn, index_col=['WCELName', 'Prefijo'])
                    stpref = statzon(df)  # stats per parameter and prefijo
                    st = par_audit(df)  # stats per parameter full set
                    output = 'parametros.csv'
                    st.to_csv(dat_dir / output)
                    if line == 'LNBTS':
                        df, st = cleaniparm(dat_dir, "ExParam.csv", "explwbt", df, st)  # info parameter removal
                    elif line == 'WBTS':
                        df, st = cleaniparm(dat_dir, "ExParam.csv", "expwbts", df, st)  # info parameter removal
                    elif line == 'LNCEL':
                        df, st = cleaniparm(dat_dir, "ExParam.csv", "explcel", df, st)  # info parameter removal
                    elif line == 'WCEL':
                        df, st = cleaniparm(dat_dir, "ExParam.csv", "expar", df, st)  # info parameter removal
                    pyexcelerate_to_excel(wb, st, sheet_name= str(carr), index=True)
                    df, st = cleaniparm2(df, st)  # standardized params and NaN>0.15*n removal
                    st['topdisc'] = range(len(st))  # top disc counter by IQR-CV
                    st['topdisc'] = st['topdisc'].floordiv(10)  # split disc in groups by 10
                    st.sort_values(by=['Median'], inplace=True, ascending=[False])  # for better visualization
                    st['counter'] = range(len(st))  # counter controls number of boxplots
                    st['counter'] = st['counter'].floordiv(10)  # split parameters in groups by 10
                    cols = ['StdDev', 'Mean', 'Median', 'Max', 'Min', 'CV']
                    st[cols] = st[cols].round(1)  # scales colums with 1 decimal digit
                    stpref[cols] = stpref[cols].round(1) # Prefijo info
                    # concat info to put text in boxplots
                    st['concat'] = st['StdDev'].astype(str) + ', ' + st['NoModeQty'].astype(str)
                    stpref['concat'] = stpref['StdDev'].astype(str) + ', ' + stpref['NoModeQty'].astype(str)
                    ldcol = list(st.index)  # parameters to include in melt command
                    # Structuring df1 according to ‘tidy data‘ standard
                    df.reset_index(level=(0, 1), inplace=True)  # to use indexes in melt operation
                    df1 = df.melt(id_vars=['Prefijo'], value_vars=ldcol,  # WCELName is not used
                                  var_name='parameter', value_name='value')
                    df1 = df1.dropna(subset=['value'])  # drop rows with value NaN
                    st.reset_index(inplace=True)  # parameter from index to col
                    stpref.reset_index(inplace=True)  # parameter from index to col
                    temp = st[['parameter', 'topdisc']] # topdisc to be included in stpref
                    stpref = pd.merge(stpref, temp, on='parameter')
                    result = pd.merge(df1, st, on='parameter')  # merge by columns not by index
                    resultzon = pd.merge(df1, stpref, on=['parameter', 'Prefijo'])  # merge by columns not by index
                    # graph code
                    custom_axis = theme(axis_text_x=element_text(color="grey", size=6, angle=90, hjust=.3),
                                        axis_text_y=element_text(color="grey", size=6),
                                        plot_title=element_text(size=25, face="bold"),
                                        axis_title=element_text(size=10),
                                        panel_spacing_x=1.6, panel_spacing_y=.45,
                                        # 2nd value number of rows and colunms
                                        figure_size=(5 * 4, 3.5 * 4)
                                        )
                    # ggplot code:value 'concat' is placed in coordinate (parameter, stddev)
                    my_plot = (ggplot(data=result, mapping=aes(x='parameter', y='value')) + geom_boxplot() +
                               geom_text(data=st, mapping=aes(x='parameter', y='StdDev', label='concat'),
                                         color='red', va='top', ha='left', size=7, nudge_x=.6, nudge_y=-1.5) +
                               facet_wrap('counter', scales='free') + custom_axis + scale_y_continuous(
                                trans=asinh_trans) + ylab("Values") + xlab("Parameters") +
                               labs(title=line + " Parameter Audit " + cart) + coord_flip())
                    pngname = str(carr) + ".png" # saveplot
                    pngfile = dat_dir / pngname
                    my_plot.save(pngfile, width=20, height=10, dpi=300)
                    pnglist.append(pngfile) # plots to be printed in pdf
                    n = 2 # top 2 plots
                    for j in range(0, n):
                        toplot = resultzon.loc[resultzon['topdisc'] == j] # filter info for parameter set to be printed
                        toplot1 = stpref.loc[stpref['topdisc'] == j]
                        custom_axis = theme(axis_text_x=element_text(color="grey", size=7, angle=90, hjust=.3),
                                            axis_text_y=element_text(color="grey", size=7),
                                            plot_title=element_text(size=25, face="bold"),
                                            axis_title=element_text(size=10),
                                            panel_spacing_x=0.6, panel_spacing_y=.45,
                                            # 2nd value number of rows and colunms
                                            figure_size=(5 * 4, 3.5 * 4)
                                            )
                        top_plot = (ggplot(data=toplot, mapping=aes(x='parameter', y='value')) + geom_boxplot() +
                                    geom_text(data=toplot1, mapping=aes(x='parameter', y='StdDev', label='concat'),
                                              color='red', va='top', ha='left', size=7, nudge_x=.6, nudge_y=-1.5) +
                                    facet_wrap('Prefijo') + custom_axis + scale_y_continuous(
                                    trans=asinh_trans) + ylab("Values") + xlab("Parameters") +
                                    labs(title="Top " + str(j+1) + " Disc Parameter per Zone. " + cart) + coord_flip())
                        pngname = str(carr) + str(j+1) + ".png"
                        pngfile = dat_dir / pngname
                        top_plot.save(pngfile, width=20, height=10, dpi=300)
                        pnglist.append(pngfile)
                except sqlite3.Error as error:  # sqlite error handling.
                    print('SQLite error: %s' % (' '.join(error.args)))
                    feedbk = tk.Label(top, text='SQLite error: %s' % (' '.join(error.args)))
                    feedbk.pack()
    wb.save(xls_path)
    90
    pntopd(pdf_path, pnglist, 50, 550, 500, 500)
    100
    c.close()
    conn.close()

def gralaud():
    # from rfpack.par_audc import par_aud  # traido
    datab = Path('C:/sqlite/20201016_sqlite.db')
    tabfile = datab.parent / Path('tablasSQL.csv')
    tabfileop = "audit2"
    par_aud(datab, tabfile, tabfileop, 1, root, my_progress, proglabel2)  # audit2 column from csv table file


def tables():
    datab = Path('C:/sqlite/20200522_sqlite.db')
    tabfile = datab.parent / Path('tablasSQL.csv')
    tabfileop = "tabla"
    pasarchivo(datab, tabfile, tabfileop, 1, root, my_progress, proglabel2)


def audit():
    datab = Path('C:/sqlite/20200522_sqlite.db')
    tabfile = datab.parent / Path('tablasSQL.csv')
    tabfileop = "Audit"
    pasarchivo(datab, tabfile, tabfileop, 1, root, my_progress, proglabel2)


def undefined():
    import sqlite3
    from rfpack.adjcreac import ADCEDep, ADCECrea
    global proglabel2

    datab = Path('C:/sqlite/20200522_sqlite.db')
    conn = sqlite3.connect(datab)
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


def missing(datedb):
    import sqlite3
    from rfpack.adjcreac import ADJSDep, ADJSCrea
    import os.path

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

    datab = Path('C:/sqlite/2020' + str(datedb) + '_sqlite.db')
    # BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    # db_path = os.path.join(BASE_DIR, "20200522_sqlite.db")
    conn = sqlite3.connect(datab)
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
    c.execute("SELECT rowid, * FROM MISS3 ORDER BY MISS3.WCELS")
    cellsm = c.fetchall()
    i = 0                               # miss row counter initialize
    n = len(cellsm)                        # misscell amnt
    while i < n:                        # while <> EOlist
        # my_progress['value'] = round(i/n*100) # prog bar increase a cording to i steps in loop
        # print(my_progress['value'])
        # proglabel2.config(text=my_progress['value'])
        # root.update_idletasks()
        k = cellsm[i][27]  # missed qty MISS_QTY
        if cellsm[i][14] is None:  # for empty ADJS list
            adjsq = 0  # adjs qty
        else:
            adjsq = cellsm[i][14]  # ADJS qty for missed src
        if adjsq == 0:  # when adjs list is empty adjQTY
            o = 0  # add control up to k
            while o < k:  # rows to add
                crea = ADJSCrea(cellsm[i][5], cellsm[i][6], cellsm[i][3], cellsm[i][7], cellsm[i][4])
                insert_crea(crea)  # raw inset into ADJS_Add db with ADJSCrea class
                i += 1  # general miss row id
                if i == n:
                    break
                o += 1  # control for set of miss cells added
            while (cellsm[i][3] == cellsm[i - 1][3]) and i < n:
                i += 1  # increase row miss pointer until next  diff cell
                if i == n:
                    break
        else:
            en = 1  # break control for att comparison
            deps = 0  # max adjmiss addd allowed control
            # msrc = cellsm[i][3]  # wcel src WCELS
            j = 0  # SY row counter
            celldep = get_046y(cellsm[i][3])  # 046 Yes cell list belonging to missed cel
            if not celldep:  # missed not in 046Yes, add up to k. No info for depuration
                if (k + adjsq) < 30: # missed+ adjsqty<30
                    o = 0  # add control up to k
                    while o < k:  # rows to add < miss qty
                        crea = ADJSCrea(cellsm[i][5], cellsm[i][6], cellsm[i][3], cellsm[i][7], cellsm[i][4])
                        insert_crea(crea)  # raw inset into ADJS_Add db with ADJSCrea class
                        i += 1  # general miss row id
                        if i == n:
                            break
                        o += 1  # control for set of miss cells added
                    while (cellsm[i][3] == cellsm[i - 1][3]) and i < n:
                        i += 1  # increase row miss pointer until next  diff cell
                        if i == n:
                            break
                else:
                    i += 1 # missed+046yesqty>=30 next missed
                    if i == n:
                        break
            else:  # miss cell is in 046 Yes and in missed list
                #    print('It is None')
                # except NameError:
                #    print("This variable is not defined")
                m = len(celldep)  # 046 Yes cell amnt
                # sycel = celldep[j][3]
                o = 0  # miss add counter
                if (k + adjsq) < 30: # missed qty MISS_QTY+ adjs qty
                    while o < k:  # rows to add
                        crea = ADJSCrea(cellsm[i][5], cellsm[i][6], cellsm[i][3], cellsm[i][7], cellsm[i][4])
                        insert_crea(crea)  # raw inset into ADJS_Add db with ADJSCrea class
                        i += 1  # general miss row id
                        if i == n:
                            break
                        o += 1  # control for set of miss cells added
                else:  # missed qty MISS_QTY+ adjs qty >=30
                    while (o + adjsq) < 30:  # fill list without depuration
                        crea = ADJSCrea(cellsm[i][5], cellsm[i][6], cellsm[i][3], cellsm[i][7], cellsm[i][4])
                        insert_crea(crea)  # raw inset into ADJS_Add db with ADJSCrea class
                        i += 1  # general miss row id
                        if i == n:
                            break
                        o += 1  # control for set of miss cells added
                    # list full depuration start
                    while (cellsm[i][3] == celldep[0][3]) and (m - deps) > 3 and en == 1 and i < n and j < m:
                        # cellname is the same in celldep list. while 046y - deps >3 to avoid depurate all cells
                        print(i, n)
                        if cellsm[i][17] > celldep[j][16]:
                            crea = ADJSCrea(cellsm[i][5], cellsm[i][6], cellsm[i][3], cellsm[i][7], cellsm[i][4])
                            insert_crea(crea)  # raw insert into ADJS_Add db with ADJSCrea class
                            depu = ADJSDep(celldep[j][5], celldep[j][7], celldep[j][3], celldep[j][9], celldep[j][4],
                                           celldep[j][6], celldep[j][8])
                            insert_depu(depu)  # raw insert into ADJS_Dep db with ADJSDep class
                            i += 1  # next miss, watchout when i=n
                            j += 1  # next SY
                            deps += 1  # miss add qty control
                        else:
                            en = 0  # break condition
                            i += 1  # next miss cell
                        if i == n:
                            break
                    if i == 0 and i < n:
                        # increase row miss pointer until next diff cell for first misscell if cond is not passed
                        while cellsm[i][3] == celldep[0][3]:
                            i += 1
                            if i == n:
                                break
                    else:
                        if i == n:
                            break
                        # increase row miss pointer until next  diff cell
                        while (cellsm[i][3] == cellsm[i - 1][3]) and i < n:
                            i += 1
                            if i == n:
                                break
    conn.commit()
    conn.close()
    # my_progress['value'] = 100  # prog bar increase a cording to i steps in loop
    # proglabel2.config(text=my_progress['value'])
    # response = messagebox.showinfo("Missing", "Process Finished")
    # proglabel3 = Label(root, text=response)
    # proglabel3 = Label(root, text="")
    # proglabel3.grid(row=3, column=1, pady=10)
    # my_progress['value'] = 0  # prog bar increase a cording to i steps in loop
    # proglabel2.config(text="   ")
    # root.update_idletasks()


# root = Tk()
# root.title('NorOcc Table - Audit Process')
# root.iconbitmap('IT.ico')
# root.geometry("400x400+350+200")        # WxH+Right+Down
# my_progress = ttk.Progressbar(root, orient=HORIZONTAL, length=300, mode='determinate')
# # my_progress.pack(pady=20)
# my_progress.grid(row=0, column=0, columnspan=2,pady=10, padx=10, ipadx=10)
# # label1 = Label(root, text="")
# # label1.place(x=20, y=20)
# # label1.pack(pady=20)
# # proglabel = Label(root, text="Progress")
# # proglabel.grid(row=1, column=0, pady=10)
# proglabel2 = Label(root, text="")
# proglabel2.grid(row=0, column=2, pady=10)
# # Create Tables Button
# tables_btn = Button(root, text="Tables", command=tables)
# tables_btn.grid(row=1, column=0, columnspan=1, pady=10, padx=10, ipadx=39)
# # Create Audit Button
# audit_btn = Button(root, text="Reuse Audits", command=audit)
# audit_btn.grid(row=1, column=1, columnspan=1, pady=10, padx=10, ipadx=23)
# # Create A Missing Button
# miss_btn = Button(root, text="Missing UMTS", command=missing)
# miss_btn.grid(row=2, column=0, columnspan=1, pady=10, padx=10, ipadx=18)
# # Create An Undefined Button
# undef_btn = Button(root, text="Undefined GSM", command=undefined)
# undef_btn.grid(row=2, column=1, columnspan=1, pady=10, padx=10, ipadx=17)
# # Create A general audit Button
# gralaud_btn = Button(root, text="General Audit", command=gralaud)
# gralaud_btn.grid(row=3, column=0, columnspan=1, pady=10, padx=10, ipadx=18)
# # Create A specific audit Button
# specaud_btn = Button(root, text="Specific Audit", command=specaud)
# specaud_btn.grid(row=3, column=1, columnspan=1, pady=10, padx=10, ipadx=17)
# # Create an Exit Button
# q_btn = Button(root, text="Exit", command=root.destroy)
# q_btn.grid(row=4, column=0, columnspan=1, pady=10, padx=10, ipadx=45)
# root.mainloop()


print('stert')
missing(1222)
print('ok')