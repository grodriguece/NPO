from pathlib import Path
from datetime import datetime, date
import operator
from pyexcelerate import Workbook
from rfpack.pyexcelerate_toexcel import pyexecelerate_to_excel
from rfpack.dt_xtract import readdrp, xtrctlnd, xtrctlns
from rfpack.dt_filter import cellmeasx, fillinf, scaninfx
from rfpack.dt_format import scanpd, celmpd, drclpd


calpath = Path('C:/SQLite/logs/nemo/call')  # call logs directory
scapath = calpath.parent / 'scan'
today = date.today()
xls_file = 'dtdrop_' + today.strftime("%y%m%d") + ".xlsx"
outfile = calpath.parent / xls_file
dcall = 1  # counter for dcalls in list
cellmetot = []  # consolidation lists
scainftot = []  # scan consolidation lists
dcaltot = []  # drp call consolidation lists
drptimes = []  # drop hour to search scan info
scatimes = []  # scan files seconds from file name
scanfile = []  # files to iterate to get 5 sec info
diter = 0  # drop call iteration for scanfile build
for callf in calpath.glob('*.nmf'):  # file iteration inside directory
    timed = readdrp(callf)  # list with drops time
    for timei in timed:  # process for each drop
        datainf, sib3info, lastcmeas = xtrctlnd(callf, timei[1], dcall)  # 5 sec window msg info for each drop time
        cmeas, sib3, callde = cellmeasx(datainf, dcall, sib3info, lastcmeas)  # lists construction
        coninfo = fillinf(cmeas, sib3)  # cell info and cellmeas concatenation
        dcall += 1  # next drop analysis
        for reg in coninfo:
            cellmetot.append(reg)  # inf consolidation
        for reg in callde:
            reg.append(timei[8])  # put date at the end
            dcaltot.append(reg)
for times in dcaltot:
    if times[1] == 'CAD':  # drop hours info
        time_string = times[8] + times[2]
        date_time = datetime.strptime(time_string, '%y%b%d%H:%M:%S.%f')
        a_timedelta = date_time - datetime(date_time.year, 1, 1)  # delta from year start
        seconds = a_timedelta.total_seconds()  # seconds from year start
        drptimes.append([times[0], date_time, seconds, times[2], times[3], times[4], times[5], times[6], times[7]])
drptimes = sorted(drptimes, key=operator.itemgetter(1))  # drops ordered by time
dcaext = []
for reg in cellmetot:
    for reg1 in drptimes:
        if reg[5] == reg1[8]:
            dcatmp = [reg1[0], reg1[3], reg1[4], reg1[5], reg1[6], reg1[7], reg1[8]]
            for iter in reg:
                dcatmp.append(iter)
            dcaext.append(dcatmp)  # cellmeas info before drop call
for scanf in scapath.glob('*.nmf'):  # file iteration inside directory
    scan1 = scanf.stem  # scan file name
    time_string = scan1[0:14]
    date_time = datetime.strptime(time_string, '%y%b%d %H%M%S')  # datetime object from name
    a_timedelta = date_time - datetime(date_time.year, 1, 1)  # delta from year start
    seconds = a_timedelta.total_seconds()  # seconds from year start
    scatimes.append([scanf.name, seconds])
scatimes = sorted(scatimes, key=operator.itemgetter(1))  # scan files ordered by time
for scan in scatimes:
    if scan[1] < drptimes[diter][2]:
        scanf = scan[0]
        if len(drptimes) - len(scanfile) == 1:  # last drop in last scan file
            scanfile.append([drptimes[diter][0], scanf])
    else:
        scanfile.append([drptimes[diter][0], scanf])
        diter += 1
dcall = 0  # iterator for drop call time
for scanit in scanfile:
    scanf = scapath / scanit[1]
    scanin = xtrctlns(scanf, drptimes[dcall][3])
    scanmsgf = scaninfx(scanin, scanit[0])  # generates df with pscrs measurements for the period selected
    dcall += 1
    for reg in scanmsgf:
        scainftot.append(reg)  # scan inf consolidation for all drops
scproc = scanpd(scainftot)
cmproc = celmpd(cellmetot)
dcproc = drclpd(dcaext)
toxls = (['scproc', 'cmproc', 'dcproc'])
wb = Workbook()  # pyexcelerate Workbook
pyexecelerate_to_excel(wb, dcproc, sheet_name='drpdetail', index=False)
pyexecelerate_to_excel(wb, cmproc, sheet_name='cellmeasinfo', index=False)
pyexecelerate_to_excel(wb, scproc, sheet_name='scaninfo', index=False)
wb.save(outfile)
