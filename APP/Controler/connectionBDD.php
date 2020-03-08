<?php
session_start();
try {
    $dbh = new PDO('mysql:host=localhost:3308;dbname=csvtotab', 'root', 'root');
}
catch (Exception $e) {
  die('Erreur : ' . $e->getMessage());
}
?>
 