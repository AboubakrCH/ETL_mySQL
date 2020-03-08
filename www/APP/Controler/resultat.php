<?php
require_once('connectionBDD.php');

    echo "<h1 style='text-align: center;'> RÉSULTATS GENERÉ </h1>";
    
	$req_tt_tables = "SELECT * FROM meta_csvtotab ";
	$result_tt_tables = $dbh->query($req_tt_tables);
	#echo $result_tt_tables->rowCount();
 	while ($row_tt_tables = $result_tt_tables->fetch()) {

#ATTENTION AU NOM DE LA BASE DE DONNEEEEEEEEE
		#$result = $dbh->prepare("SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='IDQMS' AND `TABLE_NAME`= ? ");
		$result = $dbh->prepare("SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='csvtotab' AND `TABLE_NAME`= ? ");
		$result->execute(array($row_tt_tables['table_name']));
		echo " <center><table style='border: solid 1px black;'>";

		#echo " <center><caption>".$row_tt_tables['NAME']."</caption></center>";
		echo "<h3 style='text-align: center;'> ".$row_tt_tables['table_name']." </h3>";


		#############header############

		
		echo "<tr>";
		while($row = $result->fetch()){	
	    	echo "<th>".$row["COLUMN_NAME"]."</th>";
		}
		echo "<tr>";
		

		###############################

		###############################	###############################
		#######################	try inverse ##########################


		#$name = $row_tt_tables['NAME'];
	
		$result_csv = $dbh->query(" select * from {$row_tt_tables['table_name']} ");
		#echo  $result_csv->rowCount();
		#$rows = $result_csv->fetchAll(PDO::FETCH_ASSOC);

		while($row_csv = $result_csv->fetch()){
		#foreach ($rows as $row_csv ) {
			$result = $dbh->prepare("SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='csvtotab' AND `TABLE_NAME`= ? ");
			$result->execute(array($row_tt_tables['table_name']));
			echo "<tr scope='col'>";
		     while($row = $result->fetch() ){
		     	echo "<td style='width:150px;border:1px solid black;'>".$row_csv[$row["COLUMN_NAME"]] ."</td>";
		    
		     }

		     echo "</tr>";
		}
	}
		###############################	###############################
		###############################	###############################


		echo "</table></center>";

	require_once('../View/head.html');
	
?>
