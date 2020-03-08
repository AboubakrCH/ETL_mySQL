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

			<br><br>
			<form class="form">
				<center><h1><span>Importez votre fichier csv ou sql</span></h1></center>
				<br>
				<a href="load_csv.php?section=import">
					<button type="button" class="btn btn-primary btn-lg btn-block" name="submit_import">
					<i class="glyphicon glyphicon-import"></i> Importer</button>
				</a>
			</form> 
		</div>


	</body>
	
	
	
</html>
