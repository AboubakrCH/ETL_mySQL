1. open mysql and load procedures and tables by sourcing this file : DR_semanticDep.sql
---> source DR_semanticDep.sql

2. open python script dependances.py and configure the connection according to your database

3. execute the python script. it will genere the table DR_SemanticDependencies whom contain the dependancies
---> generateDependencies(connection,table_name) : generate the dependantcies of the table 'table_name'
