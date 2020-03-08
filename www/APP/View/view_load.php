<?php
	if (isset($_GET['section'])) {
		$section = htmlspecialchars($_GET['section']);
	}
	else {
		$section="";
	}
 ?>
<!DOCTYPE html>
<html lang="fr">
	<head>
		<title>MYSQL-IDQMS</title>
		<meta charset="utf-8">
		<link rel="stylesheet" href="../bootstrap/styleCSS.css">
	</head>
	 <body>

		<header>
		<nav class="navbar navbar-inverse">
		  <div class="container-fluid">
			<div class="navbar-header">
			  <div class="navbar-brand">MYSQL-IDQMS</div>
			</div>
			<ul class="nav navbar-nav">
					<li class="active"><a href="index.php"><span  class="glyphicon glyphicon-home"></span> Accueil</a></li>
				</ul>
		  </div>
		</nav>
		</header>
		<div class="container">

			<form class="form " method="post" enctype="multipart/form-data" action ="load_csv.php">
				<div class="form-group">
					<h3><span>Import files</span></h3>
					
					<label>Fichier .csv</label>
					<input type="file" class="form-control" id="exampleFormControlFile1" name="csv">
					<br>
					<center><button type="submit" class="btn  btn-success btn-lg" name="submit_file"> Valider </button><center>
				</div>
				<?php
					if (isset($erreur)){
						echo '<span style = "color : red ">'.$erreur.'</span>';
					}
				?>
			</form>

			<br><br>

			</form> 
		</div>


	</body>
	
	
	
</html>
