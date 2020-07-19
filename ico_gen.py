from PIL import Image
filename = r'C:\\Users\\German\\Pictures\\IT.png'
img = Image.open(filename)
img.save('IT.ico',format = 'ICO', sizes=[(32,32)])

