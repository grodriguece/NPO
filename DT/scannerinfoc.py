import pandas as pd
from datetime import date
from pathlib import Path
import numpy as np
from rfpack.switcherc import tecnol


def scaninf(datalog, techid):
    import pandas as pd
    if techn == 1:  # umts info
        # columnam = ['type', 'time', 'space', 'meas_system', 'headparms', 'uarfcn',
        #             'chtype', 'carr_rssi', 'band', 'meas', 'parpercell']
        dumts = []
        types = 'PILOTSCAN'
        for srow in datalog:
            if types in str(srow):
                dumts.append(srow)  # creates new list with umts scanner info
        umdf = pd.DataFrame([sub.split(",") for sub in dumts])  # text to columns
        umdf[9] = umdf[9].astype(int)
        maxcel = umdf[9].max()  # max cells number in msg
        # for i in range(maxcel):  # put columns names
        #     columnam.extend(['pscr' + str(i), 'ec_no' + str(i), 'rscp' + str(i),
        #                      'sir' + str(i), 'delay' + str(i), 'delay_spread' + str(i)])
        # umdf.columns = columnam
        umdft = []  # df to put cell alike info in the same columns
        for i in range(maxcel):
            umdf1 = umdf.iloc[:, [8, 5, 7, 11 + i * 6, 12 + i * 6, 13 + i * 6]]
            umdf1.columns = ['band', 'uarfcn', 'carr_rssi', 'pscr', 'ec_no', 'rscp']
            if len(umdft) == 0:
                umdft = umdf1
            else:
                umdft = pd.concat([umdft, umdf1], ignore_index=True)
        umdft['ec_no'].replace('', np.nan, inplace=True)  # convert empty strings as nan
        umdft.dropna(subset=['pscr', 'ec_no'], inplace=True)  # remove invalid info
        convert_dict = {'band': int,
                        'uarfcn': int,
                        'carr_rssi': float,
                        'pscr': int,
                        'ec_no': float,
                        'rscp' : float
                        }
        umdft = umdft.astype(convert_dict)  # converts to num format for grouping
        umgroup = umdft.groupby(['band', 'uarfcn', 'pscr']).agg({'carr_rssi': ['mean'],
                                                                 'ec_no': ['mean'], 'rscp': ['mean', 'count']})
        umgroup = umgroup.reset_index()
    return umgroup


def fopinf (file):
    with open(file, 'rt') as myfile:  # Open scanf for reading text
        for line in myfile.readlines():
            if '#START' in line:  # time and date startlog
                startt = line
                break
    linelist = [int(e) if e.isdigit() else e for e in startt.split(',')]  # line to list
    hour = linelist[1]
    hourlst = [int(e) if e.isdecimal() else float(e) for e in hour.split(':')]  # hour to list [hr, m, s]
    dat = linelist[3]
    dat = dat.translate(str.maketrans({'\n': '', '"': ''}))  # remove unwanted chars
    datelst = [int(e) if e.isdigit() else e for e in dat.split('.')]  # date to list [d, m, y]
    return datelst, hourlst


def decsecs(timelst, nsec):  # ret string with nsec decremented
    timetst = list(timelst)
    if timetst[2] - nsec < 0:
        timetst[2] = 60 - timetst[2] + nsec  # avoid negative values
        if timetst[1] == 0:
            timetst[1] = 59
            if timetst[0] == 0:
                timetst[0] = 23
            else:
                timetst[0] = timetst[0] -1
        else:
            timetst[1] = timetst[1] -1
    else:
        timetst[2] = timetst[2] - nsec
    new_data = ['{:02d}'.format(x) for x in timetst]
    timestr = ':'.join(map(str, new_data))
    return timestr


def xtrctlns(filex, timetst):
    datalog = []
    timest1 = decsecs(timetst, 2)
    timest2 = decsecs(timetst, 1)  # 3 sec window
    timest3 = decsecs(timetst, 0)
    # timestr = ':'.join(map(str, timetest))
    # timestr1 = ':'.join(map(str, timetest1))
    with open(filex, 'rt') as myfile:  # Open scanf for reading text
        for line in myfile.readlines():
            if timest1 in line or timest2 in line or timest3 in line:
                datalog.append(line)  # filter for event time (2 secs)
    return datalog


scanf = Path('H:/CELCITE/Tools/SQLite/logs/19Mar12 130948.1.nmf')
workdir = scanf.parent
techn = 1
timetest = [13, 12, 9]  # time to search in scan file
dates, times = fopinf(scanf)  # date and hour of file start
datainf = xtrctlns(scanf, timetest)
scaninf(datainf, techn)  # generates df with pscrs measuremets for the period selected

# with open(scanf, 'rt') as myfile:  # Open scanf for reading text
#     for line in myfile.readlines():
#         if '#START' in line:  # time and date startlog
#             startt = line
#             break
# mList = [int(e) if e.isdigit() else e for e in startt.split(',')]
# hora = mList[1]
# mlist2 = [int(e) if e.isdecimal() else float(e) for e in hora.split(':')]
# fecha = mList[3]
# fecha = fecha.translate(str.maketrans({'\n': '', '"': ''}))  # remove unwanted chars
# mlist3 = [int(e) if e.isdigit() else e for e in fecha.split('.')]
# mList2 with time [hr, m, s] mList3 wit date [d, m, y]

# dataLog = []
# with open(scanf, 'rt') as myfile:  # Open lorem.txt for reading text
#     for line in myfile.readlines():
#         if 'SCAN' in line or 'GPS' in line or 'START' in line:
#             # contents = myfile.read()              # Read the entire file to a string
#             dataLog.append(line)
# print(dataLog)  # Print the string

# timew =
# tabfile = datab.parent / Path('tablasSQL.csv')
# tabfileop = "tabla"
# scanfinf(scanf, time, tabfileop, 1, root, my_progress, proglabel2)

# myfile = open(scanf, "rt") # open lorem.txt for reading text
# contents = myfile.read()         # read the entire file to string
# myfile.close()                   # close the file
# print(contents)

