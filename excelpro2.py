#!/usr/bin/env python

import os
import glob
import csv
import xlwt
from pathlib import Path

dat_dir = Path("C:/XML/csv")
floc = dat_dir / "*.csv"
wb = xlwt.Workbook()
# for csvfile in Path.glob(floc):
for csvfile in dat_dir.glob('*.csv'):
    # for csvfile in glob.glob(os.path.join('.', '*.csv')):
    print("starting new workbook")
    print(csvfile.name)
    fname = csvfile.name.rsplit('.', 1)[0]
    print(csvfile.name.rsplit('.', 1)[0])
    # fpath = csvfile.split("/", 1)
    # fname = fpath[1].split(".", 1)  # fname[0] should be our worksheet name
    print("adding sheet " + fname)
    ws = wb.add_sheet(fname)
    with open(csvfile.name) as f:
        reader = csv.reader(f)
        for r, row in enumerate(reader):
            for c, col in enumerate(row):
                ws.write(r, c, col)
print("saving workbook")
wb.save('output.xls')