
REGEXPRES=[
['NUMBER','INTEGER','^[0-9]+$'],
['NUMBER','FLOAT','^[0-9]+.[1-9][0-9]*$'],
['DATE','DATE','^([0-2][0-9]|3[0-1])-(0[0-9]|1[0-2])-[0-9]{4}$'],
['VARCHAR','ALPHABETHIQUE','^[a-zA-Z_+]+$'],
['VARCHAR','ALPHANUMRIQUE','^[a-zA-Z0-9_°,-_+èéà]+$'],
]

DDRE=[
['EMAIL','EMAIL',"^[A-Za-z][A-Za-z0-9]*@[a-zA-Z0-9]+.(FR|fr|COM|com|[a-zA-Z]+)$"],
['DATE','DATE_FRENCH','^([0-2][0-9]|3[0-1])(-|/)(0[0-9]|1[0-2])(-|/)[0-9]{4}$'],
['DATE','DATE_ENGLISH','^(0[0-9]|1[0-2])(-|/)([0-2][0-9]|3[0-1])(-|/)[0-9]{4}$'],
['CIVILITY','CIVILITY_FR','^(M.|MME|MLLE|MONSIEUR|MADAME|MADEMOISELLE)$'],
['CIVILITY','CIVILITY_EN','^(M.|MRS|MS|MISTER|MISS)$'],
['BLOODGROUP', 'BLOODGROUP', '^((A|B|AB|O){1}[+-]{1})$'],
]
