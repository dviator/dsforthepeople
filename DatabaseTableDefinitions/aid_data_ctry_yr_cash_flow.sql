/*
Created By: Michael Greis
Date: 6/4/2016
Summary: This is a view used to tell how much money is flowing into/out of a country by comparing
	the commitment amount against donor and recipient countries. The returned values
	are year of the aid data commitment, ISO 3 country code for the year (if it has data),
	where the cashflow falls on a scale of 0 to 100.
	
*/
--DROP VIEW launch_pad.aid_data_ctry_yr_cash_flow;

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
		 CASE WHEN donor_iso3 = 'TW ' THEN 'TWN'
			WHEN donor_iso3 = 'LD' THEN 'NLD'
			WHEN donor_iso3 = ' OR' THEN 'NOR'
			WHEN donor_iso3 = ' ZL' THEN 'NZL'
			WHEN donor_iso3 = 'CA ' THEN 'CAN'
			WHEN donor_iso3 = 'D K' THEN 'DNK'
			WHEN donor_iso3 = 'FI ' THEN 'FIN'
			WHEN donor_iso3 = 'HU ' THEN 'HUN'
			WHEN donor_iso3 = 'I D' THEN 'IND'
			WHEN donor_iso3 = 'JP ' THEN 'JPN'
			WHEN donor_iso3 = 'SV ' THEN 'SVN'
			WHEN donor_iso3 = 'TW ' THEN 'TWN'
			ELSE donor_iso3
			END AS CTRY_ISO3,
		 SUM(commitment_amount_usd_constant) AS CASH_FLOW,
		 'D' AS DONOR_RECEIVER_FLG
		FROM launch_pad.aid_data
		--Commented out the two lines below in order to allow exception records to flow through.
		WHERE donor_iso3 IS NOT NULL
			AND donor_iso3 NOT IN ('  ','')
			AND commitment_amount_usd_constant IS NOT NULL
		GROUP BY year, donor_iso3, DONOR_RECEIVER_FLG
		UNION ALL
		--grabs the receiveing for each country, filters out bad data.
		SELECT year,
			CASE WHEN recipient_iso3 = ' AM' THEN 'NAM'
				WHEN recipient_iso3 = ' ER' THEN 'NER'
				WHEN recipient_iso3 = ' GA' THEN 'NGA'
				WHEN recipient_iso3 = ' IC' THEN 'NIC'
				WHEN recipient_iso3 = ' IU' THEN 'NIU'
				WHEN recipient_iso3 = ' PL' THEN 'NPL'
				WHEN recipient_iso3 = ' RU' THEN 'NRU'
				WHEN recipient_iso3 = 'BE ' THEN 'BEN'
				WHEN recipient_iso3 = 'BT ' THEN 'BTN'
				WHEN recipient_iso3 = 'CA ' THEN 'CAN'
				WHEN recipient_iso3 = 'CH ' THEN 'CHN'
				WHEN recipient_iso3 = 'G B' THEN 'GNB'
				WHEN recipient_iso3 = 'G Q' THEN 'GNQ'
				WHEN recipient_iso3 = 'GI ' THEN 'GIN'
				WHEN recipient_iso3 = 'H D' THEN 'HND'
				WHEN recipient_iso3 = 'HU ' THEN 'HUN'
				WHEN recipient_iso3 = 'I D' THEN 'IND'
				WHEN recipient_iso3 = 'ID ' THEN 'IDN'
				WHEN recipient_iso3 = 'IR ' THEN 'IRN'
				WHEN recipient_iso3 = 'JP ' THEN 'JPN'
				WHEN recipient_iso3 = 'K A' THEN 'KNA'
				WHEN recipient_iso3 = 'KE ' THEN 'KEN'
				WHEN recipient_iso3 = 'LB ' THEN 'LBN'
				WHEN recipient_iso3 = 'M E' THEN 'MNE'
				WHEN recipient_iso3 = 'M G' THEN 'MNG'
				WHEN recipient_iso3 = 'OM ' THEN 'OMN'
				WHEN recipient_iso3 = 'P G' THEN 'PNG'
				WHEN recipient_iso3 = 'PA ' THEN 'PAN'
				WHEN recipient_iso3 = 'SD ' THEN 'SDN'
				WHEN recipient_iso3 = 'SE ' THEN 'SEN'
				WHEN recipient_iso3 = 'SH ' THEN 'SHN'
				WHEN recipient_iso3 = 'SV ' THEN 'SVN'
				WHEN recipient_iso3 = 'TO ' THEN 'TON'
				WHEN recipient_iso3 = 'TU ' THEN 'TUN'
				WHEN recipient_iso3 = 'V M' THEN 'VNM'
				WHEN recipient_iso3 = 'VE ' THEN 'VEN'
				ELSE recipient_iso3
			END AS CTRY_ISO3,
		 SUM(commitment_amount_usd_constant * -1) AS CASH_FLOW,
		 'R' AS DONOR_RECEIVER_FLG
		FROM launch_pad.aid_data
		WHERE recipient_iso3 IS NOT NULL
			AND recipient_iso3 NOT IN ('  ','')
			AND commitment_amount_usd_constant IS NOT NULL
		GROUP BY year, recipient_iso3, DONOR_RECEIVER_FLG) flg_sum
	--the last filtreration of bad data, removes bad/dirty ISO3's
	WHERE LENGTH(CTRY_ISO3) = 3
	AND CTRY_ISO3 NOT LIKE ('% %')
	GROUP BY flg_sum.year, flg_sum.CTRY_ISO3) ctry_year;
