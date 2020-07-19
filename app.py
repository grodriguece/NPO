print('hello world')

#fp = open("C:\XML\SQL\RSLTE031_-_Neighbor_HO_analysisRC7.CSV", "w")
#fp.write(line.replace(';' , ','))
#RSLTE031_-_Neighbor_HO_analysisRC7.CSV

#input file
fin = open("C:\XML\SQL\RSLTE031_-_Neighbor_HO_analysisRC7.CSV", "rt")
#output file to write the result to
fout = open("C:\XML\SQL\RSLTE031_-_Neighbor_HO_analysisRC72.CSV", "wt")
#for each line in the input file
for line in fin:
	#read replace the string and write to output file
	fout.write(line.replace(';', ','))
#close input and output files
fin.close()
fout.close()