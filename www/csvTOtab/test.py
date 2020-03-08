import re
from REGEXPRES import DDRE
txt =  "A+"
regexx= "^([0-2][0-9]|3[0-1])-(0[0-9]|1[0-2])-[0-9]{4}$"
def get_type():
	for type_, subtype, regex in DDRE: 
		x = re.match(regex, txt.upper())
		if x : 
			print(x.group(), ' ========> ', subtype)

		else: 
			print("XXXX NOT ", subtype)

get_type()

