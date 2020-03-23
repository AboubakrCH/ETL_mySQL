<?php
$current = getcwd();
$output = exec('where python');
var_dump($output);
echo $current;
echo chdir('../../csvTOmongo');
$current = getcwd();
echo $current;
 var_dump( realpath('csvtomongo.py'));
 phpinfo();
?>
