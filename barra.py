#!/bin/python3
# geektechstuff
# progress bar
# modules to import
from tkinter import *
# ttk makes the window look like running Operating System’s theme
from tkinter import ttk
# create root tkinter window to hold progress bar
root = Tk()
# create progress bar
progress = ttk.Progressbar(root, orient = HORIZONTAL, length = 120)
# pack progress bar into root
progress.pack()
# to step progress bar up
progress.config(mode='determinate', maximum=100, value=5)
progress.step(5)
# # to have an moving bar that doesn’t indicate how long it will take
# progress.config(mode=’indeterminate’)
# # to start indeterminate bar
# progress.start()
# # to stop indeterminate bar
# progress.stop()
# ——-