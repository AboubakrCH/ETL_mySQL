-- create table DR_SemanticDependencies -------
DROP TABLE IF EXISTS DR_SemanticDependencies;
CREATE TABLE DR_SemanticDependencies
(
TableName VARCHAR(100),   
LEFTCOL VARCHAR(2000),
SEMANTICDEPENDENCY VARCHAR(4),
RIGHTCOL VARCHAR(500),
PERCENTAGE NUMERIC,          
CONSTRAINT CK_SEMDEP CHECK (SEMANTICDEPENDENCY IN ('-DF-', '-EQ-', '-LE-', '-GE-'))
);
-- CONSTRAINT PK_SEMDEP PRIMARY KEY (TableName,LEFTCOL,SEMANTICDEPENDENCY,RIGHTCOL,PERCENTAGE),


DROP TABLE IF EXISTS TABLACONTROLER;
CREATE TABLE IF NOT EXISTS TABLACONTROLER 
(COL1 VARCHAR(20), COL2 VARCHAR(20), COL3 VARCHAR(1), COL4 VARCHAR(1), COL5 VARCHAR(20));
-- INSERTION DES DONNEES
--  Cette table est issue d'une intégration de données non réussie !
INSERT INTO TABLACONTROLER VALUES ('EPINAY-SUR-SEINE', 'FRANCE', '1', '1', 'EUROPE');
INSERT INTO TABLACONTROLER VALUES ('PARIS', 'FRANCE', '1', '1', 'EUROPE');
INSERT INTO TABLACONTROLER VALUES ('VILLETANEUSE', 'FRANCE', '1', '1', 'EUROPE');
INSERT INTO TABLACONTROLER VALUES ('EPINAY-SUR-SEINE', '', '1', '1', 'EUROPE');
INSERT INTO TABLACONTROLER VALUES ('EPINAY SUR SEINE', 'FRANCE', '1', '1', 'EUROPE');
INSERT INTO TABLACONTROLER VALUES ('PARIS', 'FRANCE', '1', '1', 'EUROPE');
INSERT INTO TABLACONTROLER VALUES ('PARIS', '', '1', '1', 'EUROPE');
INSERT INTO TABLACONTROLER VALUES ('PARIS', 'FRANCE', '1', '1', 'EUROPE');
INSERT INTO TABLACONTROLER VALUES ('MARSEILLE', 'FRANCE', '1', '1', 'EUROPE');
INSERT INTO TABLACONTROLER VALUES ('ORLY-VILLE', 'FRANCE', '1', '1', 'EUROPE');
INSERT INTO TABLACONTROLER VALUES ('MARSEILLE', 'FRANC', '1', '1', 'EUROPE');
INSERT INTO TABLACONTROLER VALUES ('PARYS', 'FR', '1', '1', 'EUROPE');
INSERT INTO TABLACONTROLER VALUES ('EPINAY-SUR-SEINE', 'FRANCE', '1', '1', 'EUROPE');
INSERT INTO TABLACONTROLER VALUES ('PARIS', 'france', '1', '1', '');
INSERT INTO TABLACONTROLER VALUES ('Bruxelles', 'Belgique', '1', '1', 'europe');
INSERT INTO TABLACONTROLER VALUES ('MOSCOU', 'RUSSIE', '1', '1', 'ASIE');
INSERT INTO TABLACONTROLER VALUES ('ALGER', 'ALGERIE', '1', '1', 'AFRIQUE');
INSERT INTO TABLACONTROLER VALUES ('RABAT', 'MAROC', '1', '1', 'afrique');
INSERT INTO TABLACONTROLER VALUES ('TUNIS', 'TUNISIE', '1', '1', 'AFRIQUE');

-- --------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------


DELIMITER $$

DROP PROCEDURE IF EXISTS print_debug;

CREATE PROCEDURE print_debug(string VARCHAR(1000))

BEGIN
	SELECT string;
END $$

DELIMITER ;
-- --------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------


DELIMITER $$

DROP PROCEDURE IF EXISTS VERIFDF;



CREATE PROCEDURE VERIFDF(tableName VARCHAR(4000),
 DR_SemanticDependencies VARCHAR(4000),
  LEFTCOL VARCHAR(4000),
  RIGHTCOL VARCHAR(4000),INOUT PERCENTAGE NUMERIC)
   
BEGIN
DECLARE myQuery VARCHAR(500);
DECLARE LISTAVERIFIER_VP0 VARCHAR(500);
DECLARE LISTAVERIFIER_VP VARCHAR(500);
DECLARE VERIFDF_VP VARCHAR(500);
DECLARE X DOUBLE;
DECLARE Y DOUBLE;
DECLARE SemanticDependency VARCHAR(5);
DECLARE maxocc DOUBLE;
DECLARE inter DOUBLE;
 
    SET LISTAVERIFIER_VP0 = 'LISTAVERIFIER_VP0';
    SET LISTAVERIFIER_VP = 'LISTAVERIFIER_VP';
    SET VERIFDF_VP = 'VERIFDF_VP';
    SET PERCENTAGE = -1;


    SET @myQuery =   CONCAT('CREATE OR REPLACE VIEW ',LISTAVERIFIER_VP0
    ,' (',LEFTCOL,', ',RIGHTCOL,')AS SELECT '
    ,LEFTCOL,', ',RIGHTCOL,' FROM ',tableName);
    PREPARE stmt FROM @myQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @myQuery = CONCAT('CREATE OR REPLACE VIEW ',LISTAVERIFIER_VP
    ,' (',LEFTCOL,', ', RIGHTCOL,') AS SELECT DISTINCT * FROM '
    ,LISTAVERIFIER_VP0);
    PREPARE stmt FROM @myQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    SET @myQuery = CONCAT('CREATE OR REPLACE VIEW ',VERIFDF_VP
    ,' (',LEFTCOL,',NBROCC )AS SELECT ',LEFTCOL
    ,', COUNT(*) FROM ',LISTAVERIFIER_VP
    ,' GROUP BY ',LEFTCOL
    ,' ORDER BY ',LEFTCOL);
    PREPARE stmt FROM @myQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    SET X = 0;
    SET @myQuery = CONCAT('SELECT COUNT(NBROCC) FROM ',VERIFDF_VP,' WHERE NBROCC >1', ' INTO @outvar') ;
    PREPARE stmt FROM @myQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET X = @outvar;

    IF X > 0 THEN
        SET @myQuery = CONCAT('SELECT SUM(NBROCC) FROM ',VERIFDF_VP,' WHERE NBROCC >1', ' INTO @outvar') ;
        PREPARE stmt FROM @myQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
        SET X = @outvar;
    END IF;

    SET @myQuery = CONCAT('SELECT COUNT(*) FROM ',tableName,' INTO @outvar');
    PREPARE stmt FROM @myQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET Y = @outvar;

    SET @myQuery = CONCAT('SELECT MAX(NBROCC) FROM ',VERIFDF_VP,' INTO @outvar');
    PREPARE stmt FROM @myQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET maxocc = @outvar;

    -- --------------------------------------------------------------------------------
    -- IF PERCENTAGE = 100 THEN  ? --OR // NOP
    -- DF
    IF maxocc > 1 THEN
        SET SemanticDependency = '-DF-';
        SET PERCENTAGE = (1 - X/Y) * 100; -- this formul is juste for the case we have dependances
    ELSE
        
        SET @myQuery = CONCAT('SELECT COUNT(*) FROM ',tableName
        ,' WHERE UPPER (CAST(',LEFTCOL,' AS CHAR(10))) = UPPER (CAST(',RIGHTCOL,' AS CHAR(10)))',' INTO @outvar');
        PREPARE stmt FROM @myQuery;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        SET maxocc = @outvar;
        -- EQ
        IF maxocc > 0 THEN
          SET SemanticDependency = '-EQ-';
          -- SET X = 0; -- car si c'est eq -> X=Y and  1-X/Y = 0 donc pour evite d'avoir 0 pour equal je met x=0 pour obtenir
        ELSE
          SET @myQuery = CONCAT('SELECT COUNT(*) FROM ',tableName
          ,' WHERE UPPER ( CAST(',LEFTCOL,' AS CHAR(10)) ) > UPPER ( CAST(',RIGHTCOL,' AS CHAR(10)) ) ',' INTO @outvar');
          PREPARE stmt FROM @myQuery;
          EXECUTE stmt;
          DEALLOCATE PREPARE stmt;
          SET maxocc = @outvar;
          -- GE
          IF maxocc > 0 THEN
            SET SemanticDependency = '-GE-';
          ELSE
            SET SemanticDependency = '-LE-';
          END IF;
          -- SET X = maxocc;
        END IF;
        SET X = maxocc;
        
      SET PERCENTAGE = (X/Y) * 100; -- if it's not a DF, the formul is different
    END IF;
    -- --------------------------------------------------------------------------------

    
    
    SET @myQuery = CONCAT(' SELECT COUNT(*) FROM ',DR_SemanticDependencies
    ,' WHERE tableName = ''', tableName
    ,''' AND LEFTCOL = ''' , LEFTCOL
    , ''' AND SemanticDependency = ''',SemanticDependency
    ,''' AND RIGHTCOL = ''',RIGHTCOL
    ,''' AND PERCENTAGE = ',ROUND(PERCENTAGE),' INTO @outvar');
    -- print_debug('----',maxocc);
    PREPARE stmt FROM @myQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET inter = @outvar;
    IF inter = 0 THEN
         SET @myQuery = CONCAT('INSERT INTO ',DR_SemanticDependencies
            ,' VALUES ( ''', tableName
            ,''' , ''' , LEFTCOL
            , ''' , ''',SemanticDependency
            ,''' , ''',RIGHTCOL
            ,''', ',ROUND(PERCENTAGE),')');
        -- print_debug(myQuery);
        PREPARE stmt FROM @myQuery;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

    ELSE
        -- SET SemanticDependency = 'EXT!';
        SET PERCENTAGE = -1;

    END IF;


    
END $$

DELIMITER ;


-- --------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------

DELIMITER $$

DROP PROCEDURE IF EXISTS COMBINING_LEFTCOL;

CREATE PROCEDURE COMBINING_LEFTCOL( IN tableName VARCHAR(4000), IN DR_SemanticDependencies VARCHAR(4000))
 BEGIN

  
  DECLARE myQuery VARCHAR(2000);

  DECLARE tableName_name VARCHAR(500);
  DECLARE LEFTCOL_name VARCHAR(500);
  DECLARE RIGHTCOL_name VARCHAR(500);
  DECLARE SEMANTICDEPENDENCY_name VARCHAR(500);
  DECLARE PERCENTAGE_name VARCHAR(500);

  SET tableName_name = 'tableName';
  SET LEFTCOL_name = 'LEFTCOL';
  SET RIGHTCOL_name = 'RIGHTCOL';
  SET SEMANTICDEPENDENCY_name = 'SEMANTICDEPENDENCY';
  SET PERCENTAGE_name = 'PERCENTAGE';
  

  SET @myQuery = CONCAT('INSERT INTO ',DR_SemanticDependencies,' SELECT ',tableName_name
  ,',GROUP_CONCAT(',LEFTCOL_name,' ORDER BY ',LEFTCOL_name,'), ',SEMANTICDEPENDENCY_name
  ,',',RIGHTCOL_name,',ROUND(AVG(',PERCENTAGE_name,')) FROM   ',DR_SemanticDependencies
  ,' WHERE ',SEMANTICDEPENDENCY_name,' = ''-DF-'' '
  
  ,' AND ',tableName_name,' like ''%',tableName ,'%'' '

  -- ,' AND CONCAT( ',tableName_name
  -- ,',GROUP_CONCAT(',LEFTCOL_name,' ORDER BY ',LEFTCOL_name,'), ',SEMANTICDEPENDENCY_name
  -- ,',',RIGHTCOL_name,' ) '
  -- ,' NOT IN ( SELECT CONCAT (',tableName_name,',',LEFTCOL_name,',',SEMANTICDEPENDENCY_name
  -- ,',',RIGHTCOL_name,' )FROM   ',DR_SemanticDependencies,') '
  

  ,' GROUP BY ',
  tableName_name,', ', RIGHTCOL_name,',', SEMANTICDEPENDENCY_name);
  
  PREPARE stmt FROM @myQuery;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
END $$

DELIMITER ;

/*

DELETE FROM DR_SemanticDependencies;

CALL VERIFDF('TABLACONTROLER','DR_SemanticDependencies','COL1','COL2',@PERCENTAGE);
CALL VERIFDF('TABLACONTROLER','DR_SemanticDependencies','COL1','COL3',@PERCENTAGE);
CALL VERIFDF('TABLACONTROLER','DR_SemanticDependencies','COL1','COL4',@PERCENTAGE);
CALL VERIFDF('TABLACONTROLER','DR_SemanticDependencies','COL1','COL5',@PERCENTAGE);

CALL VERIFDF('TABLACONTROLER','DR_SemanticDependencies','COL2','COL1',@PERCENTAGE);
CALL VERIFDF('TABLACONTROLER','DR_SemanticDependencies','COL2','COL3',@PERCENTAGE);
CALL VERIFDF('TABLACONTROLER','DR_SemanticDependencies','COL2','COL4',@PERCENTAGE);
CALL VERIFDF('TABLACONTROLER','DR_SemanticDependencies','COL2','COL5',@PERCENTAGE);

CALL VERIFDF('TABLACONTROLER','DR_SemanticDependencies','COL3','COL2',@PERCENTAGE);
CALL VERIFDF('TABLACONTROLER','DR_SemanticDependencies','COL3','COL1',@PERCENTAGE);
CALL VERIFDF('TABLACONTROLER','DR_SemanticDependencies','COL3','COL4',@PERCENTAGE);
CALL VERIFDF('TABLACONTROLER','DR_SemanticDependencies','COL3','COL5',@PERCENTAGE);

CALL VERIFDF('TABLACONTROLER','DR_SemanticDependencies','COL4','COL2',@PERCENTAGE);
CALL VERIFDF('TABLACONTROLER','DR_SemanticDependencies','COL4','COL3',@PERCENTAGE);
CALL VERIFDF('TABLACONTROLER','DR_SemanticDependencies','COL4','COL1',@PERCENTAGE);
CALL VERIFDF('TABLACONTROLER','DR_SemanticDependencies','COL4','COL5',@PERCENTAGE);

CALL VERIFDF('TABLACONTROLER','DR_SemanticDependencies','COL5','COL2',@PERCENTAGE);
CALL VERIFDF('TABLACONTROLER','DR_SemanticDependencies','COL5','COL3',@PERCENTAGE);
CALL VERIFDF('TABLACONTROLER','DR_SemanticDependencies','COL5','COL4',@PERCENTAGE);
CALL VERIFDF('TABLACONTROLER','DR_SemanticDependencies','COL5','COL1',@PERCENTAGE);


CALL COMBINING_LEFTCOL('TABLACONTROLER','DR_SemanticDependencies');
SELECT  DISTINCT *  from DR_SemanticDependencies;
SELECT  count(*)  from DR_SemanticDependencies;
*/
