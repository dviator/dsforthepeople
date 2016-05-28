//This code is meant as a prototype to show how to grab data from the postgresql database.

  GNU nano 2.2.6                          File: prototype_1.php                                                            

<?php

$conn = pg_connect("host=forthepeople.cwjurl03ucof.us-west-2.rds.amazonaws.com port=5432 dbname=DSPeople user=webuser pass$

if (!$conn) {
        echo "an error occurred";
        exit;
}



$result = pg_query($conn, "SELECT * FROM launch_pad.aid_data LIMIT 10");

$arr = pg_fetch_all($result);

$output = json_encode($arr);

echo ("{aiddata:" . $output ."}");




?>
