# from tkinter import *
# # from PIL import ImageTk,Image
# from tkinter import messagebox
#
# root = Tk()
# root.title('Learn To Code at Codemy.com')
# root.iconbitmap('IT.ico')
#
# # showinfo, showwarning, showerror, askquestion, askokcancel, askyesno
#
#
# def popup():
#     response = messagebox.showinfo("This is my Popup!", "Hello World!")
#     Label(root, text=response).pack()
# # if response == "yes":
# # Label(root, text="You Clicked Yes!").pack()
# # else:
# # Label(root, text="You Clicked No!!").pack()
#
#
# Button(root, text="Popup", command=popup).pack()
#
# mainloop()
#
# import tkinter as tk
# from tkinter import messagebox
#
# root = tk.Tk()
# messagebox.showinfo("info", "this information goes beyond the width of the messagebox")
# root.mainloop()
#
#
# from tkinter import *
import tkinter as tk
root = tk.Tk()

e = tk.Entry(root, width=50, font=('Helvetica', 24))
# e = Entry(root, width=50, fg="#FF0000", bg="blue", borderwidth=5, font=('Helvetica', 24))
e.pack()
e.insert(0, "Enter Your Name: ")


def myClick():
    hello = "Hello " + e.get()
    myLabel = tk.Label(top, text=hello)
    e.delete(0, 'end')
    myLabel.pack()


top = tk.Toplevel()
top.title("Process Progress")
top.geometry("300x300+120+120")
myButton = tk.Button(root, text="Enter Name", command=myClick)
myButton.pack()
root.mainloop()