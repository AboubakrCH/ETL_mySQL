<style>
.content {
  margin: auto;
}
</style>
<script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
<body>

<div class="content" >
<?php
require_once('connectionBDD.php');
$i=1;
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
		echo "</table></center>";


		if (strpos($row_tt_tables['table_name'], 'DR_CSVFILE_COL_') !== false) {

			#for tupes 
			$types = $dbh->prepare("SELECT COUNT('SYNTACTICTYPE') AS NBR FROM {$row_tt_tables['table_name']} WHERE SYNTACTICTYPE = ? ");

			$types->execute(array('VARCHAR'));
			$string = $types->fetch();
			
			$types->execute(array('NUMBER'));
			$number = $types->fetch();

			$types->execute(array('DATE'));
			$date = $types->fetch();

			$types->execute(array('BOOLEAN'));
			$boolean = $types->fetch();

			$types->execute(array('UNKNOWN'));
			$null = $types->fetch();

			# for normal and annormal values 
			$normal = $dbh->query("SELECT COUNT('OBSERVATION') AS NBRA FROM {$row_tt_tables['table_name']} WHERE OBSERVATION is null ");
			$nbrnormal = $normal->fetch();

			$annormal = $dbh->query("SELECT COUNT('OBSERVATION') AS NBRA FROM {$row_tt_tables['table_name']} WHERE OBSERVATION is not null ");
			$nbrannormal = $annormal->fetch();

			# for null values 
			$nullv = $dbh->query("SELECT COUNT('OLDVALUES') AS NBRA FROM {$row_tt_tables['table_name']} WHERE OLDVALUES is null or OLDVALUES= ' ' ");
			$nbrnullv = $nullv->fetch();

			$nnullv = $dbh->query("SELECT COUNT('OLDVALUES') AS NBRA FROM {$row_tt_tables['table_name']} WHERE OLDVALUES is not null  and OLDVALUES != ' '");
			$nbrnnullv = $nnullv->fetch();


?>
  <script type="text/javascript">
    google.charts.load("current", {packages:['corechart']});
    google.charts.setOnLoadCallback(drawChart);
    google.charts.setOnLoadCallback(drawChartCircle);
    function drawChart() {
      var data = google.visualization.arrayToDataTable([
        ["Syntaxique Type", "Occurence", { role: "style" } ],
        ["STRING",<?php echo $string['NBR'] ;?>, "#FF7F50"],
        ["NUMBER", <?php echo $number['NBR'] ;?>, "#8B008B"],
        ["DATE", <?php echo $date['NBR'] ;?>, "#7FFFD4"],
        ["BOOLEAN", <?php echo $boolean['NBR'] ;?>, "color: #FFD700"],
        ["NULL", <?php echo $null['NBR'] ;?>, "color: #00CED1"]
      ]);

      var view = new google.visualization.DataView(data);
      view.setColumns([0, 1,
                       { calc: "stringify",
                         sourceColumn: 1,
                         type: "string",
                         role: "annotation" },
                       2]);
      
      var options = {
        title: "The DOMINANT Syntactic type ",
        width: 400,       
        bar: {groupWidth: "95%"},
        legend: { position: "none" },
      }; 

      var chart = new google.visualization.ColumnChart(document.getElementById("columnchart".concat(<?php echo $i ;?>)));
      chart.draw(view, options);      
  }


  	
	function drawChartCircle() {

	        var data = google.visualization.arrayToDataTable([
	          ['TYPE DE DONNEE', 'OCCURENCE'],
	          ['Normal',    <?php echo $nbrnormal['NBRA'] ;?>],
	          ['Annormal',  <?php echo $nbrannormal['NBRA'] ;?>]
	        ]);

	        var options = {
	          title: 'The sementique Anomalies',
	          width: 400,
        	  height: 276,
        	  is3D: true,
	        };

	        var chart2 = new google.visualization.PieChart(document.getElementById('piechart'.concat(<?php echo $i ;?>)));

	        chart2.draw(data, options);
	      }


	  google.charts.setOnLoadCallback(drawChartNUL);
      function drawChartNUL() {
        var data = google.visualization.arrayToDataTable([
          ['Empty Or Not', 'OCCURENCE'],
          ['NOT NULL',     <?php echo $nbrnnullv['NBRA'] ;?>],
          ['NULL',      <?php echo $nbrnullv['NBRA'] ;?>]
        ]);

        var options = {
          title: 'Not Null Values',
          pieHole: 0.4,
          width: 400,
       	  height: 276,
        };

        var chart = new google.visualization.PieChart(document.getElementById('donutchart'.concat(<?php echo $i ;?>)));
        chart.draw(data, options);
      }

  </script>
  

<?php
  echo '<div style="width: 1250px; height: 300px;border: 1px solid #333;box-shadow: 8px 8px 5px #444 ; padding: 8px 12px;display: flex">';

  echo '<div id="columnchart'.$i.'" style=" display:inline;border-style: solid" ></div>' ;
  echo '<div id="piechart'.$i.'" style=" display:inline;border-style: solid"></div>' ;
  echo '<div id="donutchart'.$i.'" style=" display:inline;border-style: solid"></div>' ;

  echo '</div>';
	$i++;
		}
		

	}
		###############################	###############################
		###############################	###############################
		

	#require_once('../View/head.html');
	
?>

</div>

</body>


