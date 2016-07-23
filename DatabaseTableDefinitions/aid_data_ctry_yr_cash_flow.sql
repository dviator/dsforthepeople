/*
Created By: Michael Greis
Date: 6/4/2016
Summary: This is a view used to tell how much money is flowing into/out of a country by comparing
	the commitment amount against donor and recipient countries. The returned values
	are year of the aid data commitment, ISO 3 country code for the year (if it has data),
	where the cashflow falls on a scale of 0 to 100.

Update: 7/23/2016
Re-wrote the structure to align to 3.0 of the aid database. repointed table and removed unnecessary logic.
*/

DROP VIEW launch_pad.aid_data_ctry_yr_cash_flow;

CREATE VIEW launch_pad.aid_data_ctry_yr_cash_flow AS
SELECT ctry_year.year,
 ctry_year.CTRY_ISO3,
 ctry_year.CTRY_CASH_FLOW,
 --Calculation to see where the cash flow falls on a scale of 0 to 100 of all cash flows in the year.
 CASE 
	WHEN ctry_year.CTRY_CASH_FLOW < 0 THEN
		((@ctry_year.CTRY_CASH_FLOW) / (ctry_year.MAX_YR_CASH_FLOW + @ctry_year.MIN_YR_CASH_FLOW)) * 100
	WHEN ctry_year.CTRY_CASH_FLOW > 0 THEN
		((ctry_year.CTRY_CASH_FLOW + @ctry_year.MIN_YR_CASH_FLOW) / (ctry_year.MAX_YR_CASH_FLOW + @ctry_year.MIN_YR_CASH_FLOW)) * 100
	ELSE ctry_year.CTRY_CASH_FLOw
 END AS RATIO_CTRY_CASH_FLOW
FROM
	(
	--Grabs the max and min for the country, and calculated the total inflow/outflow
	SELECT flg_sum.year,
	 flg_sum.CTRY_ISO3 AS CTRY_ISO3,
	 SUM(flg_sum.cash_flow) AS CTRY_CASH_FLOW,
	 MAX(SUM(flg_sum.cash_flow)) OVER (PARTITION BY flg_sum.year) AS MAX_YR_CASH_FLOW,
	 MIN(SUM(flg_sum.cash_flow)) OVER (PARTITION BY flg_sum.year) AS MIN_YR_CASH_FLOW
	FROM (
		--grabs the donations for each country, filters bad data.
		SELECT year,
		--Case statement to handle exceptions in the 2007 data
		ctry.country_iso3 AS CTRY_ISO3,
		SUM(commitment_amount_usd_constant) AS CASH_FLOW,
		 'D' AS DONOR_RECEIVER_FLG
		FROM launch_pad.aid_data_3 aid
		--Added join to country to convert the iso 2 to iso 3 for the visualization
		INNER JOIN kill_floor.country_information ctry
		ON aid.donor_iso2 = ctry.country_iso2
		--Commented out the two lines below in order to allow exception records to flow through.
		WHERE ctry.country_iso3 IS NOT NULL
			AND ctry.country_iso3 NOT IN ('  ','')
			AND commitment_amount_usd_constant IS NOT NULL
		GROUP BY year, ctry.country_iso3, DONOR_RECEIVER_FLG
		UNION ALL
		--grabs the receiveing for each country, filters out bad data.
		SELECT year,
			ctry.country_iso3 AS CTRY_ISO3,
		 SUM(commitment_amount_usd_constant * -1) AS CASH_FLOW,
		 'R' AS DONOR_RECEIVER_FLG
		FROM launch_pad.aid_data_3 aid
		INNER JOIN kill_floor.country_information ctry
		ON aid.recipient_iso2  = ctry.country_iso2
		WHERE ctry.country_iso3  IS NOT NULL
			AND ctry.country_iso3  NOT IN ('  ','')
			AND commitment_amount_usd_constant IS NOT NULL
		GROUP BY year, ctry.country_iso3 , DONOR_RECEIVER_FLG) flg_sum
	--the last filtreration of bad data, removes bad/dirty ISO3's
	WHERE LENGTH(CTRY_ISO3) = 3
	AND CTRY_ISO3 NOT LIKE ('% %')
	AND year <> 9999
	AND year <> 2013
	GROUP BY flg_sum.year, flg_sum.CTRY_ISO3) ctry_year;
