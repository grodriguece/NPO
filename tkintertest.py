# cd c:/pycharm/npo
# python tkintertest.py
#
#
# test1
#
#
# from tkinter import *
#
# root = Tk()
# # creating a label widget
# mylabel = Label(root, text="Hello World!")
# # showing it onto screen
# mylabel.pack()
#
#
# root.mainloop()
#
#
# test2
#
#
# from tkinter import *
#
# root = Tk()
#
# # Creating a Label Widget
# myLabel1 = Label(root, text="Hello World!").grid(row=0, column=0)
# myLabel2 = Label(root, text="My Name Is German").grid(row=1, column=5)
# myLabel3 = Label(root, text="                     ").grid(row=1, column=1)
# # Shoving it onto the screen
# # myLabel1.grid(row=0, column=0)
# # myLabel2.grid(row=1, column=5)
# root.mainloop()
#
#
# test3
#
#
# from tkinter import *
#
# root = Tk()
#
# def myClick():
# 	myLabel = Label(root, text="Button")
# 	myLabel.pack()
#
# # myButton = Button(root, text="Try Me!", padx=50, pady=50, state=DISABLED)
# myButton = Button(root, text="Try Me!", command=myClick, fg="#FF0000", bg="blue")
# myButton.pack()
#
#
#
# root.mainloop()
#
#
# test4
#
#
# from tkinter import *
#
# root = Tk()
#
# e = Entry(root, width=50, font=('Helvetica', 24))
# e = Entry(root, width=50, fg="#FF0000", bg="blue", borderwidth=5, font=('Helvetica', 24))
# e.pack()
# e.insert(0, "Enter Your Name: ")
#
# def myClick():
# 	hello = "Hello " + e.get()
# 	myLabel = Label(root, text=hello)
# 	e.delete(0, 'end')
# 	myLabel.pack()
#
# myButton = Button(root, text="Enter Name", command=myClick)
# myButton.pack()
#
#
# test5
#
#
# from tkinter import *
#
# root = Tk()
# root.title("Simple Calculator")
#
# e = Entry(root, width=35, borderwidth=5)
# e.grid(row=0, column=0, columnspan=3, padx=10, pady=10)
#
# #e.insert(0, "")
#
# def button_click(number):
# 	#e.delete(0, END)
# 	current = e.get()
# 	e.delete(0, END)                       # clean field
# 	e.insert(0, str(current) + str(number)) # avoid adding numbers, just concatenate
#
# def button_clear():
# 	e.delete(0, END)
#
# def button_add():
# 	first_number = e.get()
# 	global f_num                   # to use in the entire program
# 	global math
# 	math = "addition"
# 	f_num = int(first_number)
# 	e.delete(0, END)
#
# def button_equal():
# 	second_number = e.get()
# 	e.delete(0, END)
#
# 	if math == "addition":
# 		e.insert(0, f_num + int(second_number))
#
# 	if math == "subtraction":
# 		e.insert(0, f_num - int(second_number))
#
# 	if math == "multiplication":
# 		e.insert(0, f_num * int(second_number))
#
# 	if math == "division":
# 		e.insert(0, f_num / int(second_number))
#
#
#
# def button_subtract():
# 	first_number = e.get()
# 	global f_num
# 	global math
# 	math = "subtraction"
# 	f_num = int(first_number)
# 	e.delete(0, END)
#
# def button_multiply():
# 	first_number = e.get()
# 	global f_num
# 	global math
# 	math = "multiplication"
# 	f_num = int(first_number)
# 	e.delete(0, END)
#
# def button_divide():
# 	first_number = e.get()
# 	global f_num
# 	global math
# 	math = "division"
# 	f_num = int(first_number)
# 	e.delete(0, END)
#
#
# # Define Buttons
#
# button_1 = Button(root, text="1", padx=40, pady=20, command=lambda: button_click(1))
# button_2 = Button(root, text="2", padx=40, pady=20, command=lambda: button_click(2))
# button_3 = Button(root, text="3", padx=40, pady=20, command=lambda: button_click(3))
# button_4 = Button(root, text="4", padx=40, pady=20, command=lambda: button_click(4))
# button_5 = Button(root, text="5", padx=40, pady=20, command=lambda: button_click(5))
# button_6 = Button(root, text="6", padx=40, pady=20, command=lambda: button_click(6))
# button_7 = Button(root, text="7", padx=40, pady=20, command=lambda: button_click(7))
# button_8 = Button(root, text="8", padx=40, pady=20, command=lambda: button_click(8))
# button_9 = Button(root, text="9", padx=40, pady=20, command=lambda: button_click(9))
# button_0 = Button(root, text="0", padx=40, pady=20, command=lambda: button_click(0))
# button_add = Button(root, text="+", padx=39, pady=20, command=button_add)
# button_equal = Button(root, text="=", padx=91, pady=20, command=button_equal)
# button_clear = Button(root, text="Clear", padx=79, pady=20, command=button_clear)
#
# button_subtract = Button(root, text="-", padx=41, pady=20, command=button_subtract)
# button_multiply = Button(root, text="*", padx=40, pady=20, command=button_multiply)
# button_divide = Button(root, text="/", padx=41, pady=20, command=button_divide)
#
# # Put the buttons on the screen
#
# button_1.grid(row=3, column=0)
# button_2.grid(row=3, column=1)
# button_3.grid(row=3, column=2)
#
# button_4.grid(row=2, column=0)
# button_5.grid(row=2, column=1)
# button_6.grid(row=2, column=2)
#
# button_7.grid(row=1, column=0)
# button_8.grid(row=1, column=1)
# button_9.grid(row=1, column=2)
#
# button_0.grid(row=4, column=0)
# button_clear.grid(row=4, column=1, columnspan=2)
# button_add.grid(row=5, column=0)
# button_equal.grid(row=5, column=1, columnspan=2)
#
# button_subtract.grid(row=6, column=0)
# button_multiply.grid(row=6, column=1)
# button_divide.grid(row=6, column=2)
#
# root.mainloop()
#
#
# test6
#
#
# from PIL import Image
# filename = r'C:\\Users\\German\\Pictures\\IT.png'
# img = Image.open(filename)
# img.save('IT.ico',format = 'ICO', sizes=[(32,32)])

# from tkinter import *
# from PIL import ImageTk,Image
#
# root = Tk()
# root.title('NPO NorOcc Info - Audit Process')
# root.iconbitmap('c:/pycharm/npo/IT.ico')
#
# my_img = ImageTk.PhotoImage(Image.open("../daniel/DSCF2027.jpg"))
# my_label = Label(image=my_img)
# my_label.pack()
#
#
# button_quit = Button(root, text="Exit", command=root.quit)
# button_quit.pack()
#
# root.mainloop()
#
#
# test7
#
#
# from tkinter import *
# from PIL import ImageTk,Image
#
# root = Tk()
# root.title('Maraton Medellin 2018')
# root.iconbitmap('IT.ico')
#
#
# my_img1 = ImageTk.PhotoImage(Image.open("C:/Users/Dell/Pictures/maraton/DSCF2027r2.jpg"))
# my_img2 = ImageTk.PhotoImage(Image.open("C:/Users/Dell/Pictures/maraton/DSCF2028r.jpg"))
# my_img3 = ImageTk.PhotoImage(Image.open("C:/Users/Dell/Pictures/maraton/DSCF2029.jpg"))
# my_img4 = ImageTk.PhotoImage(Image.open("C:/Users/Dell/Pictures/maraton/DSCF2091.jpg"))
# my_img5 = ImageTk.PhotoImage(Image.open("C:/Users/Dell/Pictures/maraton/S0042078.jpg"))
#
# image_list = [my_img1, my_img2, my_img3, my_img4, my_img5]
#
#
#
# my_label = Label(image=my_img1)
# my_label.grid(row=0, column=0, columnspan=3)
#
# def forward(image_number):
# 	global my_label                    #works out of the function
# 	global button_forward
# 	global button_back
#
# 	my_label.grid_forget()             #clean screen
# 	my_label = Label(image=image_list[image_number-1]) # each time fwd is pressed next image is shown
# 	button_forward = Button(root, text=">>", command=lambda: forward(image_number+1))
# 	button_back = Button(root, text="<<", command=lambda: back(image_number-1))
# 	if image_number == 5:          # end of pics
# 		button_forward = Button(root, text=">>", state=DISABLED)
#
# 	my_label.grid(row=0, column=0, columnspan=3)
# 	button_back.grid(row=1, column=0)
# 	button_forward.grid(row=1, column=2)
#
# def back(image_number):
# 	global my_label
# 	global button_forward
# 	global button_back
#
# 	my_label.grid_forget()
# 	my_label = Label(image=image_list[image_number-1])
# 	button_forward = Button(root, text=">>", command=lambda: forward(image_number+1))
# 	button_back = Button(root, text="<<", command=lambda: back(image_number-1))
#
# 	if image_number == 1:
# 		button_back = Button(root, text="<<", state=DISABLED)
#
# 	my_label.grid(row=0, column=0, columnspan=3)
# 	button_back.grid(row=1, column=0)
# 	button_forward.grid(row=1, column=2)
#
#
#
# button_back = Button(root, text="<<", command=back, state=DISABLED)
# button_exit = Button(root, text="Exit Program", command=root.quit)
# button_forward = Button(root, text=">>", command=lambda: forward(2))
#
#
# button_back.grid(row=1, column=0)
# button_exit.grid(row=1, column=1)
# button_forward.grid(row=1, column=2)
#
# root.mainloop()
#
# test8
#
# from tkinter import *
# from PIL import ImageTk,Image
#
# root = Tk()
# root.title('Maraton Medellin 2018')
# root.iconbitmap('IT.ico')
#
#
#
# my_img1 = ImageTk.PhotoImage(Image.open("C:/Users/Dell/Pictures/maraton/DSCF2027r2.jpg"))
# my_img2 = ImageTk.PhotoImage(Image.open("C:/Users/Dell/Pictures/maraton/DSCF2028r.jpg"))
# my_img3 = ImageTk.PhotoImage(Image.open("C:/Users/Dell/Pictures/maraton/DSCF2029.jpg"))
# my_img4 = ImageTk.PhotoImage(Image.open("C:/Users/Dell/Pictures/maraton/DSCF2091.jpg"))
# my_img5 = ImageTk.PhotoImage(Image.open("C:/Users/Dell/Pictures/maraton/S0042078.jpg"))
#
# image_list = [my_img1, my_img2, my_img3, my_img4, my_img5]
#
# status = Label(root, text="Image 1 of " + str(len(image_list)), bd=1, relief=SUNKEN, anchor=E)
#
# my_label = Label(image=my_img1)
# my_label.grid(row=0, column=0, columnspan=3)
#
# def forward(image_number):
# 	global my_label
# 	global button_forward
# 	global button_back
#
# 	my_label.grid_forget()
# 	my_label = Label(image=image_list[image_number-1])
# 	button_forward = Button(root, text=">>", command=lambda: forward(image_number+1))
# 	button_back = Button(root, text="<<", command=lambda: back(image_number-1))
#
# 	if image_number == 5:
# 		button_forward = Button(root, text=">>", state=DISABLED)
#
# 	my_label.grid(row=0, column=0, columnspan=3)
# 	button_back.grid(row=1, column=0)
# 	button_forward.grid(row=1, column=2)
#
# 	status = Label(root, text="Image " + str(image_number) + " of " + str(len(image_list)), bd=1, relief=SUNKEN, anchor=E)
# 	status.grid(row=2, column=0, columnspan=3, sticky=W+E)
#
#
#
# def back(image_number):
# 	global my_label
# 	global button_forward
# 	global button_back
#
# 	my_label.grid_forget()
# 	my_label = Label(image=image_list[image_number-1])
# 	button_forward = Button(root, text=">>", command=lambda: forward(image_number+1))
# 	button_back = Button(root, text="<<", command=lambda: back(image_number-1))
#
# 	if image_number == 1:
# 		button_back = Button(root, text="<<", state=DISABLED)
#
# 	my_label.grid(row=0, column=0, columnspan=3)
# 	button_back.grid(row=1, column=0)
# 	button_forward.grid(row=1, column=2)
#
# 	# Update Status Bar
# 	status = Label(root, text="Image " + str(image_number) + " of " + str(len(image_list)), bd=1, relief=SUNKEN, anchor=E)
# 	status.grid(row=2, column=0, columnspan=3, sticky=W+E)
#
# button_back = Button(root, text="<<", command=back, state=DISABLED)
# button_exit = Button(root, text="Exit Program", command=root.quit)
# button_forward = Button(root, text=">>", command=lambda: forward(2))
#
#
# button_back.grid(row=1, column=0)
# button_exit.grid(row=1, column=1)
# button_forward.grid(row=1, column=2, pady=10)
# status.grid(row=2, column=0, columnspan=3, sticky=W+E)
#
#
# root.mainloop()
#
#test9
#
#
# from tkinter import *
# from PIL import ImageTk,Image
#
# root = Tk()
# root.title('Maraton Medellin 2018')
# root.iconbitmap('IT.ico')
# frame = LabelFrame(root, text="Tables Section", padx=50, pady=50)
# frame.pack(padx=10, pady=10)
#
# b = Button(frame, text="Don't Click Here!")
# b2 = Button(frame, text="...or here!")
# b.grid(row=0, column=0)
# b2.grid(row=1, column=1)
#
#
#
#
# root.mainloop()
#
#
#test10
#
#
# from tkinter import *
# from PIL import ImageTk,Image
#
# root = Tk()
# root.title('Learn To Code at Codemy.com')
# root.iconbitmap('IT.ico')
#
# #r = IntVar()
# #r.set("2")
#
# TOPPINGS = [
# 	("Pepperoni", "Pepperoni"),		#first text, second topping
# 	("Cheese", "Cheese"),
# 	("Mushroom", "Mushroom"),
# 	("Onion", "Onion"),
# ]
#
# pizza = StringVar()
# pizza.set("Pepperoni") # set the first one
#
# for text, topping in TOPPINGS:
# 	Radiobutton(root, text=text, variable=pizza, value=topping).pack(anchor=W)
#
#
# def clicked(value):
# 	myLabel = Label(root, text=value)
# 	myLabel.pack()
#
# #Radiobutton(root, text="Option 1", variable=r, value=1, command=lambda: clicked(r.get())).pack()  #value choosen sent to function clicked
# #Radiobutton(root, text="Option 2", variable=r, value=2, command=lambda: clicked(r.get())).pack()
#
# #myLabel = Label(root, text=pizza.get())
# #myLabel.pack()
#
# myButton = Button(root, text="Click Me!", command=lambda: clicked(pizza.get()))
# myButton.pack()
# mainloop()
#
#test11
#
#
# from tkinter import *
# from PIL import ImageTk,Image
# from tkinter import messagebox
#
# root = Tk()
# root.title('Learn To Code at Codemy.com')
# root.iconbitmap('IT.ico')
#
# # showinfo, showwarning, showerror, askquestion, askokcancel, askyesno
#
# def popup():
# 	response = messagebox.showinfo("This is my Popup!", "Hello World!")
# 	Label(root, text=response).pack()
# 	#if response == "yes":
# 	#	Label(root, text="You Clicked Yes!").pack()
# 	#else:
# 	#	Label(root, text="You Clicked No!!").pack()
#
# Button(root, text="Popup", command=popup).pack()
#
#
# mainloop()
#
#test12
#
# from tkinter import *
# from PIL import ImageTk,Image
#
#
# root = Tk()
# root.title('Images')
# root.iconbitmap('IT.ico')
#
# def open():
# 	global my_img
# 	top = Toplevel()
# 	top.title('Maraton Window')
# 	top.iconbitmap('IT.ico')
# 	my_img = ImageTk.PhotoImage(Image.open("C:/Users/Dell/Pictures/maraton/DSCF2027r2.jpg"))
# 	my_label = Label(top, image=my_img).pack()
# 	btn2 = Button(top, text="close window", command=top.destroy).pack()
#
# btn = Button(root, text="Open Second Windo", command=open).pack()
#
# mainloop()
#
#test13
#
#
# from tkinter import *
# from PIL import ImageTk,Image
# from tkinter import filedialog
#
# root = Tk()
# root.title('Images')
# root.iconbitmap('IT.ico')
#
#
#
# def open():
# 	global my_image
# 	root.filename = filedialog.askopenfilename(initialdir="C:/Users/Dell/Pictures/maraton", title="Select A File", filetypes=(("jpg files", "*.jpg"),("all files", "*.*")))
# 	my_label = Label(root, text=root.filename).pack()
# 	my_image = ImageTk.PhotoImage(Image.open(root.filename))
# 	my_image_label = Label(image=my_image).pack()
#
#
# my_btn = Button(root, text="Open File", command=open).pack()
#
#
# root.mainloop()
#
# test14
#
# from tkinter import *
# from PIL import ImageTk,Image
#
#
# root = Tk()
# root.title('Image')
# root.iconbitmap('IT.ico')
# root.geometry("400x400")
#
# vertical = Scale(root, from_=0, to=200)
# vertical.pack()
#
# def slide():
# 	my_label = Label(root, text=horizontal.get()).pack()
# 	root.geometry(str(horizontal.get()) + "x" + str(vertical.get()))
#
# horizontal = Scale(root, from_=0, to=400, orient=HORIZONTAL)
# horizontal.pack()
#
# my_label = Label(root, text=horizontal.get()).pack()
#
#
#
# my_btn = Button(root, text="Click Me!", command=slide).pack()
#
# root.mainloop()
#
#
# test15
#
# from tkinter import *
# from PIL import ImageTk,Image
#
#
# root = Tk()
# root.title('Checks')
# root.iconbitmap('IT.ico')
# root.geometry("400x400")
#
# def show():
# 	myLabel = Label(root, text=var.get()).pack()
#
#
# var = StringVar()
#
# c =Checkbutton(root, text="Would you like to SuperSize your order? Check Here!", variable=var, onvalue="SuperSize", offvalue="RegularSize")
# c.deselect()		# to fix bug
# c.pack()
#
#
#
# myButton = Button(root, text="Show Selection", command=show).pack()
#
#
# root.mainloop()
#
# test16
#
#
# from tkinter import *
# from PIL import ImageTk,Image
# from tkinter import ttk
#
#
# root = Tk()
# root.title('Dropdown')
# root.iconbitmap('IT.ico')
# root.geometry("400x400")
#
# # Drop Down Boxes
#
# def clicking(event):
# 	myLabel = Label(root, text=droped.get()).pack()
#
# def clicker(event):
# 	myLabel = Label(root, text=clicked.get()).pack()
#
# def show():
# 	myLabel = Label(root, text=clicked.get()).pack()
#
# options = [
# 	"Monday",
# 	"Tuesday",
# 	"Wednesday",
# 	"Thursday",
# 	"Friday",
# 	"Saturday"
# ]
#
# clicked = StringVar()
# clicked.set(options[0])
#
# drop = OptionMenu(root, clicked, *options, command=clicker)
#
# drop.pack()
#
# droped = ttk.Combobox(root, value=["Search by...", "Last Name", "Email Address", "Customer ID"])
# droped.current(0)
# droped.bind("<<ComboboxSelected>>", clicking)
# droped.pack()
#
#
#
# #myButton = Button(root, text="Show Selection", command=show).pack()
#
# root.mainloop()
#
# test17
#
from tkinter import *
from PIL import ImageTk,Image
import sqlite3


root = Tk()
root.title('SQLite manager')
root.iconbitmap('IT.ico')
root.geometry("400x600")

# Databases

# Create a database or connect to one
conn = sqlite3.connect('address_book.db')

# Create cursor
c = conn.cursor()

# Create table
'''
c.execute("""CREATE TABLE addresses (
		first_name text,
		last_name text,
		address text,
		city text,
		state text,
		zipcode integer
		)""")	# just execute at the beginning to create database
'''
# Create Update function to update a record
def update():
	# Create a database or connect to one
	conn = sqlite3.connect('address_book.db')
	# Create cursor
	c = conn.cursor()

	record_id = delete_box.get()

	c.execute("""UPDATE addresses SET
		first_name = :first,
		last_name = :last,
		address = :address,
		city = :city,
		state = :state,
		zipcode = :zipcode
		WHERE oid = :oid""",
		{
		'first': f_name_editor.get(),
		'last': l_name_editor.get(),
		'address': address_editor.get(),
		'city': city_editor.get(),
		'state': state_editor.get(),
		'zipcode': zipcode_editor.get(),
		'oid': record_id
		})


	#Commit Changes
	conn.commit()

	# Close Connection
	conn.close()

	editor.destroy()
	root.deiconify()

# Create Edit function to update a record
def edit():
	root.withdraw()
	global editor
	editor = Tk()
	editor.title('Update A Record')
	editor.iconbitmap('c:/gui/codemy.ico')
	editor.geometry("400x300")
	# Create a database or connect to one
	conn = sqlite3.connect('address_book.db')
	# Create cursor
	c = conn.cursor()

	record_id = delete_box.get()
	# Query the database
	c.execute("SELECT * FROM addresses WHERE oid = " + record_id)
	records = c.fetchall()

	#Create Global Variables for text box names
	global f_name_editor
	global l_name_editor
	global address_editor
	global city_editor
	global state_editor
	global zipcode_editor

	# Create Text Boxes
	f_name_editor = Entry(editor, width=30)
	f_name_editor.grid(row=0, column=1, padx=20, pady=(10, 0))
	l_name_editor = Entry(editor, width=30)
	l_name_editor.grid(row=1, column=1)
	address_editor = Entry(editor, width=30)
	address_editor.grid(row=2, column=1)
	city_editor = Entry(editor, width=30)
	city_editor.grid(row=3, column=1)
	state_editor = Entry(editor, width=30)
	state_editor.grid(row=4, column=1)
	zipcode_editor = Entry(editor, width=30)
	zipcode_editor.grid(row=5, column=1)

	# Create Text Box Labels
	f_name_label = Label(editor, text="First Name")
	f_name_label.grid(row=0, column=0, pady=(10, 0))
	l_name_label = Label(editor, text="Last Name")
	l_name_label.grid(row=1, column=0)
	address_label = Label(editor, text="Address")
	address_label.grid(row=2, column=0)
	city_label = Label(editor, text="City")
	city_label.grid(row=3, column=0)
	state_label = Label(editor, text="State")
	state_label.grid(row=4, column=0)
	zipcode_label = Label(editor, text="Zipcode")
	zipcode_label.grid(row=5, column=0)

	# Loop thru results
	for record in records:
		f_name_editor.insert(0, record[0])
		l_name_editor.insert(0, record[1])
		address_editor.insert(0, record[2])
		city_editor.insert(0, record[3])
		state_editor.insert(0, record[4])
		zipcode_editor.insert(0, record[5])


	# Create a Save Button To Save edited record
	edit_btn = Button(editor, text="Save Record", command=update)
	edit_btn.grid(row=6, column=0, columnspan=2, pady=10, padx=10, ipadx=145)




# Create Function to Delete A Record
def delete():
	# Create a database or connect to one
	conn = sqlite3.connect('address_book.db')
	# Create cursor
	c = conn.cursor()

	# Delete a record
	c.execute("DELETE from addresses WHERE oid = " + delete_box.get())

	delete_box.delete(0, END)

	#Commit Changes
	conn.commit()

	# Close Connection
	conn.close()



# Create Submit Function For database
def submit():
	# Create a database or connect to one
	conn = sqlite3.connect('address_book.db')
	# Create cursor
	c = conn.cursor()

	# Insert Into Table
	c.execute("INSERT INTO addresses VALUES (:f_name, :l_name, :address, :city, :state, :zipcode)",
			{
				'f_name': f_name.get(),
				'l_name': l_name.get(),
				'address': address.get(),
				'city': city.get(),
				'state': state.get(),
				'zipcode': zipcode.get()
			})


	#Commit Changes
	conn.commit()

	# Close Connection
	conn.close()

	# Clear The Text Boxes
	f_name.delete(0, END)
	l_name.delete(0, END)
	address.delete(0, END)
	city.delete(0, END)
	state.delete(0, END)
	zipcode.delete(0, END)

# Create Query Function
def query():
	# Create a database or connect to one
	conn = sqlite3.connect('address_book.db')
	# Create cursor
	c = conn.cursor()

	# Query the database
	c.execute("SELECT *, oid FROM addresses")
	records = c.fetchall()
	# print(records)

	# Loop Thru Results
	print_records = ''
	for record in records:
		print_records += str(record[0]) + " " + str(record[1]) + " " + "\t" +str(record[6]) + "\n"

	query_label = Label(root, text=print_records)
	query_label.grid(row=12, column=0, columnspan=2)

	#Commit Changes
	conn.commit()

	# Close Connection
	conn.close()


# Create Text Boxes
f_name = Entry(root, width=30)
f_name.grid(row=0, column=1, padx=20, pady=(10, 0))
l_name = Entry(root, width=30)
l_name.grid(row=1, column=1)
address = Entry(root, width=30)
address.grid(row=2, column=1)
city = Entry(root, width=30)
city.grid(row=3, column=1)
state = Entry(root, width=30)
state.grid(row=4, column=1)
zipcode = Entry(root, width=30)
zipcode.grid(row=5, column=1)
delete_box = Entry(root, width=30)
delete_box.grid(row=9, column=1, pady=5)


# Create Text Box Labels
f_name_label = Label(root, text="First Name")
f_name_label.grid(row=0, column=0, pady=(10, 0))
l_name_label = Label(root, text="Last Name")
l_name_label.grid(row=1, column=0)
address_label = Label(root, text="Address")
address_label.grid(row=2, column=0)
city_label = Label(root, text="City")
city_label.grid(row=3, column=0)
state_label = Label(root, text="State")
state_label.grid(row=4, column=0)
zipcode_label = Label(root, text="Zipcode")
zipcode_label.grid(row=5, column=0)
delete_box_label = Label(root, text="Select ID")
delete_box_label.grid(row=9, column=0, pady=5)

# Create Submit Button
submit_btn = Button(root, text="Add Record To Database", command=submit)
submit_btn.grid(row=6, column=0, columnspan=2, pady=10, padx=10, ipadx=100)

# Create a Query Button
query_btn = Button(root, text="Show Records", command=query)
query_btn.grid(row=7, column=0, columnspan=2, pady=10, padx=10, ipadx=137)

#Create A Delete Button
delete_btn = Button(root, text="Delete Record", command=delete)
delete_btn.grid(row=10, column=0, columnspan=2, pady=10, padx=10, ipadx=136)

# Create an Update Button
edit_btn = Button(root, text="Edit Record", command=edit)
edit_btn.grid(row=11, column=0, columnspan=2, pady=10, padx=10, ipadx=143)


#Commit Changes
conn.commit()

# Close Connection
conn.close()

root.mainloop()
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
