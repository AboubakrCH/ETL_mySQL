
DELIMITER $$
#------------------------------------------------+
#------------- create table for test             |
DROP TABLE IF EXISTS CSVfile1;

CREATE TABLE CSVfile1 (Col VARCHAR(1000));

INSERT INTO CSVfile1 VALUES ('Adam;Paris;M;19;19-06-2001;38°C');
INSERT INTO CSVfile1 VALUES ('Eve;Paris;F;23;16-10-1996;37°C');
INSERT INTO CSVfile1 VALUES ('Gabriel;Paris;m;18;17-09-2002;36,5°C');
INSERT INTO CSVfile1 VALUES ('Mariam;Paris;F;41;13-08-1978;38Celcius');
INSERT INTO CSVfile1 VALUES ('Nadia;Londres;f;55;10-10-1965;95°F');
INSERT INTO CSVfile1 VALUES ('Inès;Madrid;F;50;22-11-1969;99,5°F');
INSERT INTO CSVfile1 VALUES ('Inconnu;77;12-12-2012');
INSERT INTO CSVfile1 VALUES ('Abnomly;Rome;1;88;02-10-2019;38°C');
INSERT INTO CSVfile1 VALUES ('Anomalies;Tunis;f;99;25-30-2020;x');
INSERT INTO CSVfile1 VALUES ('Adam;Paris;M;19;19-06-2001;38°C');
INSERT INTO CSVfile1 VALUES ('Eve;Paris;F;23;16-10-1996;37°C');
INSERT INTO CSVfile1 VALUES ('Marie;Pari;F;41;17-09-1979;38Celcius');

COMMIT;

#------------------------------------------------+
#------------- maTable                      |
#------------------------------------------------+
#-- permet de creer une vue a partir de la table passée en parametre 
DROP PROCEDURE IF EXISTS maTable;

CREATE PROCEDURE maTable (tableName VARCHAR(50),LaColonne VARCHAR(100))
BEGIN
  DECLARE V_Query VARCHAR(100);

  SET @V_Query = CONCAT('create or replace view csvfile as select ',LaColonne ,' from ',tableName);
  PREPARE Query FROM @V_Query;
  EXECUTE Query;
  
END $$

COMMIT;

CALL maTable('CSVfile1','Col');
#------------------------------------------------+
#------------- getMaxLength                      |
#------------------------------------------------+
#-- permet de déterminer la ligne qui a le plus d'element dans un fichier csv
#-- retourne le nombre de LeDelimiter de la ligne qui en a le plus

DROP FUNCTION IF EXISTS getMaxLength;

CREATE FUNCTION getMaxLength(LaColonne VARCHAR(100),LeDelimiter VARCHAR(100))
RETURNS INT
DETERMINISTIC
BEGIN
        DECLARE maxLength INT DEFAULT 0;

        SELECT MAX(length(LaColonne)-length(replace(LaColonne,LeDelimiter,''))) INTO maxLength FROM csvfile ;

 RETURN (maxLength);

END $$


#------------------------------------------------+
#------------- createTable                      |
#------------------------------------------------+
#-- permet de créer une table de NbrCol colonnes

DROP PROCEDURE IF EXISTS createTable;

CREATE PROCEDURE createTable (laTable VARCHAR(200), NbrCol INT)
BEGIN
  DECLARE myQueryCreateTable VARCHAR(500);
  DECLARE firstIteration BOOLEAN;
  DECLARE i INT DEFAULT 1;

  SET myQueryCreateTable = CONCAT('CREATE TABLE ',laTable,' (');
  SET firstIteration = TRUE;

  CREATION: LOOP

        IF i > NbrCol THEN 
            LEAVE CREATION;
        END IF;

       IF firstIteration = TRUE THEN
         SET myQueryCreateTable = CONCAT(myQueryCreateTable ,'Col',i,' VARCHAR(200)');
         SET firstIteration =FALSE;
       ELSE
         SET myQueryCreateTable = CONCAT(myQueryCreateTable ,', Col',i,' VARCHAR(200)');
       END IF;

       SET i=i+1;
  END LOOP CREATION;
  SET @myQueryCreateTable = CONCAT(myQueryCreateTable,' )');
  #--DBMS_OUTPUT.put_line(myQueryCreateTable);
  #dropTable(laTable);
  SELECT @myQueryCreateTable;
  PREPARE Query FROM @myQueryCreateTable;
  EXECUTE Query;
END $$


#------------------------------------------------+
#------------- createInsertValues                |
#------------------------------------------------+
#-- permet de générer la chaine de valeurs qui seront inséret dans une table à partir d'une chaine et d'un delimiteur


/*
CREATE OR REPLACE FUNCTION createInsertValues (mystring_old VARCHAR(1000), delimiter VARCHAR(50), maxValues INT)
RETURNS VARCHAR
BEGIN
  DECLARE myInsertValues VARCHAR(500);
  DECLARE value VARCHAR(500);
  DECLARE firstIteration BOOLEAN;
  DECLARE mystring VARCHAR(500);
  DECLARE delimiterextra VARCHAR(500);

  SET delimiterextra = '''';
  -- cas particulier : si on a ;; ou le transforme en ;-;
  SET mystring  = REGEXP_REPLACE(mystring_old,''||delimiter||delimiter||'',''||delimiter||'-'||delimiter||'');
  --print_debug(mystring_n);
  -- trim() : supprime les espaces avant et après
  SET myInsertValues  = 'VALUES (';
  SET firstIteration  =TRUE; -- on ajoute la virgule après la 1er itération ( après avoir récupérer la 1er colonnes)
  FOR i IN
   (SELECT trim(regexp_substr(mystring, '[^'||delimiter||']+', 1, LEVEL)) mot
   FROM dual
   CONNECT BY LEVEL <= maxValues+1)
   -- on peut remplacer nbrDelimiteur par regexp_count(MotsCles, delimiter)
   LOOP
      IF i.mot is null THEN
        SET value = 'null';
      ELSIF i.mot = '-' OR UPPER(i.mot) = 'NULL' THEN
       SET value = 'null';
      ELSE
        SET value = REGEXP_REPLACE(i.mot,''||delimiterextra||'','');
        SET value =''''|| value ||'''';
        

      END IF;

      IF firstIteration = TRUE THEN
        SET myInsertValues = CONCAT(myInsertValues , value);
        SET firstIteration = FALSE;
      ELSE
          SET myInsertValues = CONCAT(myInsertValues ,', ',value);
      END IF;

   END LOOP;
   SET myInsertValues = CONCAT(myInsertValues , ' )');
   --print_debug(myInsertValues);

   -- insertion les données
    RETURN (myInsertValues);
END $$

*/



DELIMITER ;

#------------------------------------------------+
#------------- Some tests                      |
#------------------------------------------------+
#-- teste pour voir les resultats des fonction
SELECT getMaxLength('Col',';') AS NBMAX FROM DUAL;
CALL createTable('HHH',3);
DESC HHH;
DROP TABLE HHH;




