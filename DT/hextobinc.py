import math
import numpy as np
from pathlib import Path
from datetime import datetime, date
import operator
from pyexcelerate import Workbook
from rfpack.pyexcelerate_toexcel import pyexecelerate_to_excel


def timeconv(string):  # not used code improved
    import datetime
    timed = [int(e) if e.isdecimal() else (float(e)) for e in string.split(":")]  # hour to list [hr, m, s]
    timed.append(int(1000000 * (timed[2] - math.floor(timed[2])) + 1))  # micro secs
    timed[2] = math.floor(timed[2])  # secs int
    timefor = datetime.time(timei[0], timei[1], timei[2], timei[3])  # drop time in time format
    return timefor


def fillinforig(cellmeas, sibinf):  # not used code improved
    cellm1 = list(cellmeas)
    sib31 = list(sibinf)
    listot = []
    sibtime = []  # sib times to compare with drp times
    i = 1  # check start in second element
    k = 0  # cellsm counter
    o = 0  # control to iterate cellm1
    jump = 0  # control when to advance in cellm1 up to k
    for sb in sib31:
        sibtime.append(sb[1])
    for sb in sib31:
        for cms in cellm1:
            if o < k and jump == 1:
                o += 1
            elif comptime(cms[1], sib31[i][1], 0) == 1:
                # cms[1] < sib31[i][1]:
                jump = 0
                sbt = list(sb)
                for j in cms:
                    sbt.append(j)
                listot.append(sbt)
                k += 1
            else:
                jump = 1
                i += 1
                break
        o = 0
    return listot


def sib3cellinf(msgstr):  # in dt_xtract file
    # 4 bit for each hex char 04b means 0-padded 4chars long binary string):
    res2 = "".join([f'{int(c, 16):04b}' for c in msgstr])  # 16 4 bit groups
    rnc = int(res2[2:14], base=2)
    cellid = int(res2[14:30], base=2)
    return rnc, cellid


def decsecs(timestr, nsec):  # ret string with nsec decremented. in dt_timec file
    timelist = []
    timelst = [int(e) if e.isdecimal() else (float(e)) for e in timestr.split(":")]  # hour to list [hr, m, s]
    timelst.append(int(1000000 * (timelst[2] - math.floor(timelst[2])) + 1))  # micro secs
    timelst[2] = math.floor(timelst[2])  # secs int
    timetst = list(timelst[: 3])  # avoid change in timelst, microsecs discarded
    new_data = ['{:02d}'.format(x) for x in timetst]
    timestr = ':'.join(map(str, new_data))
    timelist.append(timestr)  # appends current time
    for sec in range(nsec-1):
        if timetst[2] - 1 < 0:
            timetst[2] = 59  # avoid negative values
            if timetst[1] == 0:
                timetst[1] = 59
                if timetst[0] == 0:
                    timetst[0] = 23
                else:
                    timetst[0] = timetst[0] -1
            else:
                timetst[1] = timetst[1] -1
        else:
            timetst[2] = timetst[2] - 1
        new_data = ['{:02d}'.format(x) for x in timetst]
        timestr = ':'.join(map(str, new_data))
        timelist.append(timestr)
    return timelist


def comptime(ref, tgt, sbef):  # compares 2 times with sbef shift. in dt_timec file
    cond = 1
    refl = [int(e) if e.isdecimal() else (float(e)) for e in ref.split(":")]  # hour to list [hr, m, s]
    tgtl = [int(e) if e.isdecimal() else (float(e)) for e in tgt.split(":")]
    if (3600 * refl[0] + 60 * refl[1] + refl[2]) >= (3600 * tgtl[0] + 60 * tgtl[1] + tgtl[2] - sbef):
        cond = 0  # returns 1 if ref < tgt - sbet
    return cond


def xtrctlns(filex, timetst):  # in dt_xtract file
    scanlog = []
    timestr = decsecs(timetst, 5)  # string with times to extract 5 secs
    with open(filex, 'rt') as myfile:  # Open scanf for reading text
        for line in myfile.readlines():
            for tim in timestr:  # check str list in line
                if tim in line:  # time window msg
                    scanlog.append(line)  # filter for event time (5 secs)
    return scanlog


# def xtrctlnd(filex, timetst, drcall):
#     datalog = []
#     sib3inf = []
#     timestr = decsecs(timetst, 5)  # string with times to extract
#     with open(filex, 'rt') as myfile:  # open scanf for reading text
#         for line in myfile.readlines():
#             if "SYSTEM_INFORMATION_BLOCK_TYPE_3" in line:
#                 listemp = line.split(",")
#                 if comptime(listemp[1], timetst, 5) == 1:  # stores last sib3 info before window
#                     sibfst = listemp[1]
#                     listemp[9] = listemp[9].replace('"', "")
#                     rncid, cellid = sib3cellinf(listemp[9][0: 8])
#             for tim in timestr:  # check str list in line
#                 if tim in line:  # time window msg
#                     if (line.startswith("CARE") or line.startswith("CAD") or
#                             line.startswith("CELLMEAS") or "SYSTEM_INFORMATION_BLOCK_TYPE_3" in line):
#                         datalog.append(line)  # filter for event time (5 secs)
#     sib3inf.append([drcall, sibfst, rncid, cellid])  # sib3 initial msg
#     return datalog, sib3inf


def xtrctlnd(filex, timetst, drcall):  # in dt_xtract file
    datalog = []
    sib3inf = []
    timestr = decsecs(timetst, 5)  # string with times to extract
    with open(filex, 'rt') as myfile:  # open scanf for reading text
        for line in myfile.readlines():
            listemp = line.split(",")
            if listemp[0] == 'RRCSM':
                listemp[5] = listemp[5].replace('"', "")
                if listemp[5] == 'SYSTEM_INFORMATION_BLOCK_TYPE_3':
                    if comptime(listemp[1], timetst, 5) == 1:  # stores last sib3 info before window
                        sibfst = listemp[1]
                        listemp[9] = listemp[9].replace('"', "")
                        rncid, cellid = sib3cellinf(listemp[9][0: 8])
            for tim in timestr:  # check str list in line
                if tim in listemp[1]:  # time window msg
                    if listemp[0] == 'CARE' or listemp[0] == 'CAD' or listemp[0] == 'CELLMEAS':
                        datalog.append(line)  # filter for event time (5 secs)
                        if listemp[0] == 'CELLMEAS':
                            if comptime(listemp[1], timetst, 0) == 1:  # cmeas time < drp time
                                lastmeas = listemp[1]  # last meas time bef drop
                    if listemp[0] == 'RRCSM':
                        if listemp[5] == 'SYSTEM_INFORMATION_BLOCK_TYPE_3':
                            datalog.append(line)
    sib3inf.append([drcall, sibfst, rncid, cellid])  # sib3 initial msg
    return datalog, sib3inf, lastmeas


def readdrp(callog):  # get drop calls time in times list. in dt_xtract file
    cllfl = callog.stem[0:7]  # date from file name 7 chars
    with open(callog, "r") as fi:
        idlst = []  # init drop msg list
        for ln in fi:
            if ln.startswith("CAD"):  # Call disconnect msg
                listemp = ln.split(",")  # string to list
                if int(listemp[6]) == 2:  # CS disc. status Dropped call
                    listemp.append(cllfl)
                    idlst.append(listemp)  # get entire msg
    return idlst


def scaninfx(scninf, dcall):  #in dt_filter file
    pilotslt = []
    for lin in scninf:
        line = lin.split(",")  # string to list
        if line[0] == 'PILOTSCAN':
            if line[9] == '':  # cells calculation based on msg size
                line[9] = math.floor(int((len(line)-11)/6))  # 11 independent params, 6 common params per cell
            for cl in range(int(line[9])):  # cellmeas list build
                pilotslt.append([dcall, line[8], line[5], line[7], line[11 + 6 * cl], line[12 + 6 * cl],
                                 line[13 + 6 * cl]])
    return pilotslt  # just UMTS info from scan file


# def cellmeasx(datainf1, dcall, sib3inf, lstcmeas):
#     cellmlt = []
#     calldet = []
#     for lin in datainf1:
#         line = lin.split(",")  # string to list *********
#         # timef1 = timeconv(line[1])
#         # timef = timef1.strftime("%H:%M:%S.%f")
#         if line[0] == 'CELLMEAS':
#             # cellml = []
#             # timed = [int(e) if e.isdecimal() else (float(e)) for e in line[1].split(":")]  # hour to list [hr, m, s]
#             # timed.append(int(1000000 * (timed[2] - math.floor(timed[2])) + 1))  # micro secs
#             # timed[2] = math.floor(timed[2])  # secs int
#             # timef = datetime.time(timei[0], timei[1], timei[2], timei[3])  # drop time in time format
#             disp1 = 3 * (int(line[5]) - 1)  # adjust for multiple channels in scan msg(cells without uarfcn decoded)
#             if line[10 + disp1] == '':  # cells calculation based on msg size
#                 line[10 + disp1] = math.floor(int((len(line) - (12 + disp1)/17)))
#             for cl in range(int(line[10 + disp1])):
#                 # cellmeas list build
#                 cellmlt.append([dcall, line[1], line[7], line[8], line[12 + disp1 + 17 * cl],
#                                 line[14 + disp1 + 17 * cl], line[15 + disp1 + 17 * cl],
#                                 line[16 + disp1 + 17 * cl], line[18 + disp1 + 17 * cl]])
#         elif line[0] == 'RRCSM':  # sib 3 list build
#             line[9] = line[9].replace('"', "")  # remove unwanted characters
#             rncid, cellid = sib3cellinf(line[9][0: 8])  # get cell info with sib 3 first 8 bytes msg
#             sib3inf.append([dcall, line[1], rncid, cellid])
#         elif line[0] == 'CAD':  # call disconnect list build include last cellmeas serving info
#             calldet.append([dcall, line[0], line[1], line[3], line[5], line[6], line[7], lstcmeas])
#         else:  # Call Restablishment case
#             calldet.append([dcall, line[0], line[1], line[3], None, line[5], line[6], lstcmeas])
#     return cellmlt, sib3inf, calldet


def cellmeasx(datainf1, dcall, sib3inf, lstcmeas):  #in dt_filter file
    cellmlt = []
    calldet = []
    for lin in datainf1:
        line = lin.split(",")  # string to list *********
        # timef1 = timeconv(line[1])
        # timef = timef1.strftime("%H:%M:%S.%f")
        if line[0] == 'CELLMEAS':
            # cellml = []
            # timed = [int(e) if e.isdecimal() else (float(e)) for e in line[1].split(":")]  # hour to list [hr, m, s]
            # timed.append(int(1000000 * (timed[2] - math.floor(timed[2])) + 1))  # micro secs
            # timed[2] = math.floor(timed[2])  # secs int
            # timef = datetime.time(timei[0], timei[1], timei[2], timei[3])  # drop time in time format
            disp1 = 3 * (int(line[5]) - 1)  # adjust for multiple channels in scan msg(cells without uarfcn decoded)
            if line[10 + disp1] == '':  # cells calculation based on msg size
                line[10 + disp1] = math.floor(int((len(line) - (12 + disp1)/17)))
            for cl in range(int(line[10 + disp1])):
                # cellmeas list build
                cellmlt.append([dcall, line[1], line[7], line[8], line[12 + disp1 + 17 * cl],
                                line[14 + disp1 + 17 * cl], line[15 + disp1 + 17 * cl],
                                line[16 + disp1 + 17 * cl], line[18 + disp1 + 17 * cl]])
        elif line[0] == 'RRCSM':  # sib 3 list build
            line[9] = line[9].replace('"', "")  # remove unwanted characters
            rncid, cellid = sib3cellinf(line[9][0: 8])  # get cell info with sib 3 first 8 bytes msg
            sib3inf.append([dcall, line[1], rncid, cellid])
        elif line[0] == 'CAD':  # call disconnect list build include last cellmeas serving info
            calldet.append([dcall, line[0], line[1], line[3], line[5], line[6], line[7], lstcmeas])
        else:  # Call Restablishment case
            calldet.append([dcall, line[0], line[1], line[3], None, line[5], line[6], lstcmeas])
    return cellmlt, sib3inf, calldet


def fillinf(cellmeas, sibinf):  #in dt_filter file
    cellm1 = list(cellmeas)
    sib31 = list(sibinf)
    listot = []
    sibtime = []  # sib times to compare with drp times
    i = 0  # control to iterate sib31
    for sb in sib31:
        sibtime.append(sb[1])  # create list with sib times to compare
    for cms in cellm1:
        if len(sib31) > 1:
            if comptime(cms[1], sib31[i+1][1], 0) == 0:
                sbt = list(sib31[i+1])  # next sib element to use in append
                if (i+2) < len(sib31):  # end limit validation
                    i += 1  # next sib31 item
            else:
                sbt = list(sib31[i])  # concatenate with current sib value
        else:
            sbt = list(sib31[i])  # 1 element sib list just concatenate cellmeas info
        for j in cms:
            sbt.append(j)  # concatenate cellm1 info to sib31 info
        listot.append(sbt)
    return listot


def drclpd(drpclst):  # in dt_format file
    import pandas as pd
    cddf = pd.DataFrame(drpclst)
    cddf.columns = ['callnbrd', 'timedrp', 'callid', 'typed', 'status', 'cause', 'timesref',
                    'callnbrs', 'timesib', 'rncid', 'cellid', 'callnbr', 'timeas', 'carrier',
                    'carr_rssi', 'type', 'uarfcn', 'pscr', 'ec_no', 'rscp']
    cddf = cddf[['callnbrd', 'timedrp', 'callid', 'typed', 'status', 'cause', 'timesib', 'rncid',
                 'cellid', 'timeas', 'carrier', 'carr_rssi', 'type', 'uarfcn', 'pscr', 'ec_no', 'rscp']]
    cddf['ec_no'].replace('', np.nan, inplace=True)  # convert empty strings as nan
    cddf['uarfcn'].replace('', np.nan, inplace=True)  # convert empty strings as nan
    cddf.dropna(subset=['pscr', 'uarfcn', 'ec_no'], inplace=True)  # remove invalid info
    convert_dict = {'callnbrd': int,
                    'callid': int,
                    'typed': int,
                    'status': int,
                    'rncid': int,
                    'cellid': int,
                    'carrier': int,
                    'carr_rssi': float,
                    'type': int,
                    'uarfcn': int,
                    'pscr': int,
                    'ec_no': float,
                    'rscp': float
                    }
    cddf = cddf.astype(convert_dict)  # converts to num format for grouping
    return cddf


def scanpd(scanlst):  # group scan info after consolidation for all drops. in dt_format file
    import pandas as pd
    umdf = pd.DataFrame(scanlst)  # text to columns
    umdf.columns = ['drpcall', 'band', 'uarfcn', 'carr_rssi', 'pscr', 'ec_no', 'rscp']
    umdf['ec_no'].replace('', np.nan, inplace=True)  # convert empty strings as nan
    umdf.dropna(subset=['pscr', 'ec_no'], inplace=True)  # remove invalid info
    convert_dict = {'drpcall': int,
                    'band': int,
                    'uarfcn': int,
                    'carr_rssi': float,
                    'pscr': int,
                    'ec_no': float,
                    'rscp': float
                    }
    umdf = umdf.astype(convert_dict)  # converts to num format for grouping
    umgroup = umdf.groupby(['drpcall', 'band', 'uarfcn',
                            'pscr']).agg({'carr_rssi': ['mean'], 'ec_no': ['mean'], 'rscp': ['mean', 'count']})
    umgroup = umgroup.reset_index()
    umgroup.columns = ['drpcall', 'band', 'uarfcn', 'pscr', 'carr_rssi', 'ec_no', 'rscp', 'count']
    umgroup = umgroup.sort_values(["drpcall", "count", 'ec_no'], ascending=(True, False, False))
    return umgroup


def celmpd(droplst):  # group scan info after consolidation for all drops. in dt_format file
    import pandas as pd
    cmdf = pd.DataFrame(droplst)  # text to columns
    cmdf.columns = ['callnbrs', 'timesib', 'rncid', 'cellid', 'drpcall', 'timeas',
                    'carrier', 'carr_rssi', 'type', 'uarfcn', 'pscr', 'ec_no', 'rscp']
    cmdf['ec_no'].replace('', np.nan, inplace=True)  # convert empty strings as nan
    cmdf['uarfcn'].replace('', np.nan, inplace=True)  # convert empty strings as nan
    cmdf.dropna(subset=['pscr', 'uarfcn', 'ec_no'], inplace=True)  # remove invalid info
    convert_dict = {'rncid': int,
                    'cellid': int,
                    'drpcall': int,
                    'carrier': int,
                    'carr_rssi': float,
                    'type': int,
                    'uarfcn': int,
                    'pscr': int,
                    'ec_no': float,
                    'rscp': float
                    }
    cmdf = cmdf.astype(convert_dict)  # converts to num format for grouping
    ucgroup = cmdf.groupby(['rncid', 'cellid', 'drpcall', 'carrier', 'type', 'uarfcn',
                            'pscr']).agg({'carr_rssi': ['mean'], 'ec_no': ['max'], 'rscp': ['max', 'count']})
    ucgroup = ucgroup.reset_index()
    ucgroup.columns = ['rncid', 'cellid', 'drpcall', 'carrier', 'type', 'uarfcn',
                       'pscr', 'carr_rssi', 'ec_no', 'rscp', 'count']
    ucgroup = ucgroup.sort_values(['drpcall', 'type', 'count', 'ec_no'], ascending=(True, True, False, False))
    return ucgroup


calpath = Path('C:/SQLite/logs/nemo/call')  # call logs directory
# list(calpath.glob('*.nmf'))
scapath = calpath.parent / 'scan'

today = date.today()
xls_file = 'dtdrop_' + today.strftime("%y%m%d") + ".xlsx"
outfile = calpath.parent / xls_file
dcall = 1  # counter for dcalls in list
cellmetot = []  # consolidation lists
scainftot = []  # scan consolidation lists
dcaltot = []
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



print(scproc)

        # time operations
        #     from datetime import datetime, timedelta
        #     time1 = datetime.strptime(timelst, '%H:%M:%S.%f').time()
        #     time2 = time1.replace(microsecond=0)
        #     timelist.append(time2.strftime("%H:%M:%S"))

