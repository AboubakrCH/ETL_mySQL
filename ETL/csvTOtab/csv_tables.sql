#------------------------------------------------+
#------------- create table for test  |
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

-- phpMyAdmin SQL Dump
-- version 4.9.3
-- https://www.phpmyadmin.net/
--
-- Host: localhost:8889
-- Generation Time: Feb 09, 2020 at 03:02 PM
-- Server version: 5.7.26
-- PHP Version: 7.4.1

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";

--
-- Database: `ETL_project`
--

-- --------------------------------------------------------

--
-- Table structure for table `DR_CSVFile_Col_1`
--
DROP TABLE IF EXISTS DR_CSVFile_Col_1;

CREATE TABLE `DR_CSVFile_Col_1` (
  `REFERENCE` varchar(300) DEFAULT NULL,
  `OLDVALUES` varchar(300) DEFAULT NULL,
  `SYNTACTICTYPE` varchar(300) DEFAULT NULL,
  `SUBSYNTACTICTYPE` varchar(300) DEFAULT NULL,
  `COLUMNWIDTH` int(11) DEFAULT NULL,
  `NUMBEROFWORDS` int(11) DEFAULT NULL,
  `OBSERVATION` varchar(300) DEFAULT NULL,
  `NEWVALUES` varchar(300) DEFAULT NULL,
  `SEMANTICCATEGORY` varchar(300) DEFAULT NULL,
  `SEMANTICSUBCATEGORY` varchar(300) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `DR_CSVFile_Col_1`
--

INSERT INTO `DR_CSVFile_Col_1` (`REFERENCE`, `OLDVALUES`, `SYNTACTICTYPE`, `SUBSYNTACTICTYPE`, `COLUMNWIDTH`, `NUMBEROFWORDS`, `OBSERVATION`, `NEWVALUES`, `SEMANTICCATEGORY`, `SEMANTICSUBCATEGORY`) VALUES
('csvFile_col1', '38°C', 'varchar', 'alphanum', 4, 1, NULL, NULL, NULL, NULL),
('csvFile_col1', '37°C', 'varchar', 'alphanum', 4, 1, NULL, NULL, NULL, NULL),
('csvFile_col1', '5°C', 'varchar', 'alphanum', 3, 1, NULL, NULL, NULL, NULL),
('csvFile_col1', '9', 'INT', 'alphanum', 1, 1, NULL, NULL, NULL, NULL),
('csvFile_col1', '38Celciu', 'varchar', 'alphanum', 8, 1, NULL, NULL, NULL, NULL),
('csvFile_col1', '95°F', 'varchar', 'alphanum', 4, 1, NULL, NULL, NULL, NULL);




