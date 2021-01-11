from pathlib import Path
import pandas as pd
import sqlite3
from datetime import date
import matplotlib.pyplot as plt
from pathlib import Path
import pandas as pd
import xlsxwriter
import io


def labelbar(axr):
    for patch in axr.patches:
        # xy coords of the lower left corner of the rectangle
        bl = patch.get_xy()
        x = 0.2 * patch.get_width() + bl[0]
        # change 0.2 to move the text up and down
        y = 0.2 * patch.get_height() + bl[1]
        axr.text(x, y, "%d" % (patch.get_height()),
                 ha='center', rotation='vertical', fontsize=11, weight='bold', color='black')
    return


Dict = {0: 'Centro',
        1: 'Costa',
        2: 'NorOccidente',
        3: 'Oriente',
        4: 'SurOccidente',
        5: 'NotInBL',
        }

#
# paramst1 = ["RegionS", "DeptoS", "Zona", "Prefijo", "LNBTSname", "LNBTSnameT", "LNCELname", "LNCELnameT",
#             "LNCELnameTB", "PLMN_id", "MRBTS_id", "LNBTS_id", "LNCEL_id", "LNREL_id", "amleAllowed",
#             "handoverAllowed", "removeAllowed", "cellIndOffNeigh", "cellIndOffNeighDelta", "SameSite",
#             "SameSector", "BandaS", "BandaT", "Distance", "bearing", "bearback", "ThetaAng", "CoefCoOrient",
#             "AzS ","AzT", "ClusterS", "MunS", "ClusterT", "MunT", "lcrId","lcrIdT","PLMN_idT", "MRBTS_idT",
#             "LNBTS_idT", "LNCEL_idT", "eutraCelId", "PowerBoost", "PCI", "RSI", "Estado", "EstadoT"]
# parstring = ','.join(paramst1)
# tabsq = "LNREL_700"
# datsrcf = pd.read_sql_query("select " + parstring + " from " + tabsq + ";", cont)
# tabsq = "LNREL_DISC_700"
# datsrca = pd.read_sql_query("select " + parstring + " from " + tabsq + ";", cont)


# SELECT
# L.RegionS
# COUNT(DISTINCT *)
# FROM LNREL_PAR AS L
# WHERE L.earfcnDLS = 9560;

tab_par = "LNREL_700_Follow"
datesq = '0110'
dbtgt = Path('C:/sqlite/2021' + datesq + '_sqlite.db')
today = date.today()
dat_dir = dbtgt.parent
(dat_dir / 'csv').mkdir(parents=True, exist_ok=True)  # create csv folder to save temp files
ftab1 = tab_par + '.csv'  # tables and parameters to audit
ftab2 = tab_par + '.xlsx'
dfini = pd.read_csv(dat_dir / 'csv' / ftab1)
# cont = sqlite3.connect(dbtgt)  # database connection for all iterations
# cur = cont.cursor()
# sql1 = "select L.RegionS, count(*) as 'Cantidad' from LNREL_PAR AS L where "
# sql2 = " group by L.RegionS;"
# cond1 = "L.earfcnDLS = 9560"
# cond2 = "(L.earfcnDLS = 9560 AND L.amleAllowed = 0 AND L.handoverAllowed = 0)"
# datsrcf = pd.read_sql_query(sql1 + cond1 + sql2, con=cont, index_col="RegionS")
# datsrcf1 = pd.read_sql_query(sql1 + cond2 + sql2, con=cont, index_col="RegionS")
# datsrcf = datsrcf.merge(datsrcf1, how='left', left_index=True, right_index=True)
# datsrcf['Perc'] = datsrcf['Cantidad_y'] / datsrcf['Cantidad_x']
# # datsrcf['Date'] = today.strftime("%y%m%d")
# datsrcf.rename(columns={"Cantidad_y": "LNREL700_AMLE_Disc", "Cantidad_x": "LNREL700_Tot"}, inplace=True)
# datsrcf = datsrcf.reset_index()
# datsrcf.insert(0, 'Date', '21' + datesq)
# datsrcf['RegionS'].fillna('NotInBL', inplace=True)
# dfini = dfini.append(datsrcf, ignore_index=True)
# dfini.to_csv(dat_dir / 'csv' / ftab1, index=False)
# # df1 = dfini.melt(id_vars=['RegionS'], value_vars=['LNREL700_AMLE_Allowed', 'LNREL700_Tot'],  # WCELName is not used
# #                                        var_name='Amount', value_name='value')
# print(datsrcf)
# cur.close()
# cont.close()
# plot generation
dfini.set_index("Date", inplace=True)
# create all axes we need
frames = {i: dat for i, dat in dfini.groupby('RegionS')}  # dataframe dict per region
fig = plt.figure()
#
for i in Dict:
    rslt_df = frames[Dict[i]]
    axtemp = plt.subplot(2, 3, i + 1)  # 2 rows, 3 columns, plot position
    rslt_df[['LNREL700_Tot', 'LNREL700_AMLE_Disc']].plot(kind='bar', ax=axtemp, figsize=(20, 10), use_index=True,
                                                         legend=False)
    labelbar(axtemp)
    axtempi = axtemp.twinx()  # secondary axis graph
    axtempi.plot(axtemp.get_xticks(), rslt_df[['Perc']].values, linestyle='-', marker='o', linewidth=2.0,
                 label='Percentage', color="r")
    axtemp.set_title(Dict[i])
    locals()['axtemp{0}'.format(i)] = axtempi  # for secondary axis sharing
    if i == 0:  # set legend in first plot
        axtempi.legend(loc=0)
        axtempi.legend(bbox_to_anchor=(0.05, 1.02, 1., .102), loc='lower left',
                       ncol=1, mode="expand", borderaxespad=0, frameon=False)
        axtemp.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc='lower left',
                      ncol=2, mode="expand", borderaxespad=1.5, frameon=False)
    # share the secondary axes
axtemp0.get_shared_y_axes().join(axtemp0, axtemp1, axtemp2, axtemp3, axtemp4, axtemp5)
# Place a legend above this subplot, expanding itself to fully use the given bounding box.
fig.suptitle('LNREL700 amleAllowed = 0, handoverAllowed = 0')
fig.tight_layout(pad=3.0)  # row separation
# plt.show()
writer = pd.ExcelWriter(str(dat_dir / 'csv' / ftab2), engine='xlsxwriter')  # Create Pandas Excel writer. XlsxWriter as the engine.
dfini.to_excel(writer, sheet_name='Data') # Convert the dataframe to an XlsxWriter Excel object.
workbook = writer.book  # Get the xlsxwriter objects from the dataframe writer object.
# workbook = xlsxwriter.Workbook('LNREL700.xlsx')
# worksheet = writer.sheets['Data']
wks1 = workbook.add_worksheet('Chart')
# wks1.write(0,0,'test')
imgdata = io.BytesIO()
fig.savefig(imgdata, format='png')
wks1.insert_image(1, 1, '', {'image_data': imgdata})
workbook.close()
# Close the Pandas Excel writer and output the Excel file.
# writer.save()



