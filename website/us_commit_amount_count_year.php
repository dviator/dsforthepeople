
<?php

$conn = pg_connect("host=forthepeople.cwjurl03ucof.us-west-2.rds.amazonaws.com port=5432 dbname=DSPeople user=webuser password=all123");

if (!$conn) {
        echo "an error occurred";
        exit;
}



$result = pg_query($conn, "SELECT year,donor, sum(commitment_amount_usd_nominal) AS USD_COMMITED,COUNT(*) AS NBR_DONATIONS FROM launch_pad.aid_data WHERE donor='United States' GROUP BY year, donor");

$arr = pg_fetch_all($result);

$output = json_encode($arr);

echo ('{"aiddata":' . $output ."}");




?>
