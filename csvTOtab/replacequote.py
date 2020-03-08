inputfile = "csvtotab" #put the "path/name" of the file you want to change
outputfile = "newcsvtotab" #put the "path/name" of the file you will generate

f = open(inputfile,"r")
text = ""
z = True
for line in f:
	for c in line:
		if c == '"':
			text = text + "'"
		elif c == "'":
			text = text + '"'
		else:
			text = text + c

z = open(outputfile,"w+")	

z.write(text)	
