<style>
.content {
  margin: auto;
}
</style>
<script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
<body>

<div class="content" >

<?php
$i=1;
	require '../vendor/autoload.php'; // include Composer's autoloader
	require_once('connectionBDD.php');

	$client = new MongoDB\Client("mongodb://localhost:27017");
	$collection = $db->meta_csvtotab;
	$database = $client->selectDatabase('csvtotab');
	$col = $database->selectCollection('meta_csvtotab');

$result = $collection->find([],['_id' => 0,'cat' => 1]);
$res2 = $collection->find([])->toArray();
$res = $result->toArray();

#var_dump( $res);

foreach ($res as $tabname) {
	echo " <center><table style='border: solid 1px black;'>";
	echo "<h3 style='text-align: center;'>".$tabname['table_name']." </h3>";



	$col = $db->selectCollection($tabname['table_name']);
	$cursor = $col->findOne([]);
	#var_dump($cursor);

	echo "<tr>";
	foreach ($cursor as $key => $colval) {
		if($key != '_id'){
			echo "<th>".$key."</th>";
		}
	}
	echo "</tr>";

	$cursor = $col->find([])->toArray();

	foreach ($cursor as $index => $values) {
		echo "<tr scope='col'>";
		foreach ($values as $key => $colval) {
			if($key != '_id'){
				#echo $value;
				echo "<td style='width:150px;border:1px solid black;'>".$colval."</td>";
			}		
		}
		echo "</tr>";
	}



	echo "</table></center>";

	if(strpos($tabname['table_name'],'DR_CSVFILE_COL_') !== false){
		$col = $db->selectCollection($tabname['table_name']);

		#for types
		$filter = array('SYNTACTICTYPE' => array( '$eq' => 'VARCHAR'));
		$string = $col->count($filter);
		echo $string;

		$filter = array('SYNTACTICTYPE' => array( '$eq' => 'NUMBER'));
		$number = $col->count($filter);

		$filter = array('SYNTACTICTYPE' => array( '$eq' => 'DAT'));
		$date = $col->count($filter);

		$filter = array('SYNTACTICTYPE' => array( '$eq' => 'BOOLEAN'));
		$boolean = $col->count($filter);

		$filter = array('SYNTACTICTYPE' => array( '$eq' => 'UNKNOWN'));
		$null = $col->count($filter);

		#for normal and annormal values
		$filter = array('OBSERVATION' => array( '$eq' => ''));
		$nbrnormal = $col->count($filter);

		$filter = array('OBSERVATION' => array( '$ne' => '' ));
		$nbrannormal = $col->count($filter);

		#for null values	
		$filter = array('OBSERVATION' => array( '$eq' => 'NULL<?>Anomaly'));
		$nbrnullv = $col->count($filter);

		$filter = array('OBSERVATION' => array( '$ne' => 'NULL<?>Anomaly'));
		$nbrnnullv = $col->count($filter);







?>
	<script type="text/javascript">
    google.charts.load("current", {packages:['corechart']});
    google.charts.setOnLoadCallback(drawChart);
    google.charts.setOnLoadCallback(drawChartCircle);
    function drawChart() {
      var data = google.visualization.arrayToDataTable([
        ["Syntaxique Type", "Occurence", { role: "style" } ],
        ["STRING",<?php echo $string ;?>, "#FF7F50"],
        ["NUMBER", <?php echo $number ;?>, "#8B008B"],
        ["DATE", <?php echo $date ;?>, "#7FFFD4"],
        ["BOOLEAN", <?php echo $boolean ;?>, "color: #FFD700"],
        ["NULL", <?php echo $null ;?>, "color: #00CED1"]
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
	          ['Normal',    <?php echo $nbrnormal ;?>],
	          ['Annormal',  <?php echo $nbrannormal ;?>]
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
          ['NOT NULL',     <?php echo $nbrnnullv ;?>],
          ['NULL',      <?php echo $nbrnullv ;?>]
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