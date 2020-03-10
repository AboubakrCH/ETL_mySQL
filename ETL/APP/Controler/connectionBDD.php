<?php
session_start();
try {
    $dbh = new PDO('mysql:host=localhost;dbname=csvtotab', 'root', 'root');
}
catch (Exception $e) {
  die('Erreur : ' . $e->getMessage());
}
?>
 