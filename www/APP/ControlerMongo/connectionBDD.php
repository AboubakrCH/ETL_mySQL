<?php
session_start();
try {
    $dbh = new MongoClient();
    $db = $dbh->selectDB("csvtotab")
}
catch (Exception $e) {
  die('Erreur : ' . $e->getMessage());
}
?>
 