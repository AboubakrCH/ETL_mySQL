<?php
require '../vendor/autoload.php'; // include Composer's autoloader
session_start();
try {
    $client = new MongoDB\Client("mongodb://localhost:27017");
    $db = $client->csvtotab;
}
catch (Exception $e) {
  die('Erreur : ' . $e->getMessage());
}
?>
 