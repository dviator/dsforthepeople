<?php

$conn = pg_connect("host=forthepeople.cwjurl03ucof.us-west-2.rds.amazonaws.com port=5432 dbname=DSPeopl$

if (!$conn) {
        echo "an error occurred";
        exit;
}



$result = pg_query($conn, "SELECT year, ctry_iso3, ctry_cash_flow FROM launch_pad.physical_aid_data_ctr$

$arr = pg_fetch_all($result);

$output = json_encode($arr);

echo ('{"aiddata":' . $output ."}");




?>

