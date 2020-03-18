<style>
.content {
  margin: auto;
}
</style>
<script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
<body>

<div class="content" >

<?php
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


	}
}
?>
 

</div>

</body>