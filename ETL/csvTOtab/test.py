import re
from REGEXsPRES import DDRE
txt =  "s-é"
regexx= "^[a-zA-Zàâçéèêëîïôûùüÿñæœ_+ -]+$"
def get_type():

	x = re.match(regexx, txt)
	if x : 
		print('yes')

	else: 
		print("XXXX NOT ")

get_type()

