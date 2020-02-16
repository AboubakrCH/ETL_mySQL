import re
txt =  "3FAHRENHEIT"
regex= '^[0-9][0-9]?°F|[0-9][0-9]?°?FAHRENHEIT|[0-9][0-9]?°?Fahrenheit$'
def get_type():
	x = re.match(regex, txt)
	if x : 
		print(' YES')
	else: 
		print("NOO")

get_type()


