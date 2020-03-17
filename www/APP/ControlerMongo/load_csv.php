<?php
	session_start();
				
		if (isset($_POST['submit_file'])) {
				$filename = $_FILES['csv']['name'];
				$file_tmpname = $_FILES['csv']['tmp_name'];
				$file_size = $_FILES['csv']['size'];
				$error = $_FILES['csv']['error'];
				$file_type = $_FILES['csv']['type'];


				$extension_autorisé = array('csv');
				$filExt = explode('.',$filename); 
				$extension_upload_csv = strtolower(end($filExt)); //extraire l'extention du fichier à télécharger

				//tester si l'extension du fichier est celle qui autorisé
				if(in_array($extension_upload_csv,$extension_autorisé)){
					if ($error === 0) {
						$fichier_csv = 'csvfile.csv'; //basename($_FILES['csv']['name']); //nom du fichier
						$file_dest = '../file_uploaded/'.$fichier_csv;
						move_uploaded_file($file_tmpname,$file_dest);

						$output = exec("C:\Users\bckha\AppData\Local\Programs\Python\Python38\python ../../csvTOmongo/csvtomongo.py 2>&1");
						echo $output;
						#sleep(10);
						header('Location: resultat.php');
					} else {
						$erreur = "Erreur format fichier";
						
					}   
					
				}
				else {
					$erreur = "Erreur format fichier";
				}
			}


		require_once('../View/view_load.php');
		require_once('../View/head.html');
 ?>
