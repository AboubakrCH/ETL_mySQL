<?php
require_once('connectionBDD.php');


    echo "<h1 style='text-align: center;'> RÉSULTATS GENERÉ </h1>";

    
	$req = "SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='IDQMS' AND `TABLE_NAME`='csvtotab' ";
	$result = $dbh->query($req);


	echo " <center><table style='border: solid 1px black;'>";

	#############header############
	echo "<tr>";
	while($row = $result->fetch()){	
    	echo "<th>".$row["COLUMN_NAME"]."</th>";
	}
	echo "<tr>";
	###############################

	###############################	###############################
	#######################	try inverse ##########################


	$req_csvtotab = " select * from csvtotab ";
	$result_csv = $dbh->query($req_csvtotab);
	while($row_csv = $result_csv->fetch()){

		$result = $dbh->query($req);
		echo "<tr scope='col'>";

	     while($row = $result->fetch() ){
	     	echo "<td style='width:150px;border:1px solid black;'>".$row_csv[$row["COLUMN_NAME"]] ."</td>";
	     }
	     echo "</tr>";
	}

	###############################	###############################
	###############################	###############################


	echo "</table></center>";

	require_once('../View/head.html');
	
?>
