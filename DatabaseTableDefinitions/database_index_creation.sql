--index creation

CREATE INDEX country_date_index ON launch_pad.aid_data_3 (recipient_iso2, start_date);

CREATE INDEX country_date_location_index ON kill_floor.nytimeslocations (country, date);
