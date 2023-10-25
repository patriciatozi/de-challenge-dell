CREATE TABLE schema_covid_cases.covid_19_rates (
	country VARCHAR(100),
	country_code VARCHAR(3),
	continent VARCHAR(100),
	population BIGINT,
	indicator VARCHAR(50),
	year_week VARCHAR(50),
	source VARCHAR(50),
	note VARCHAR(50),
	weekly_count INTEGER,
	cumulative_count INTEGER,
	rate_14_day DOUBLE PRECISION,
	date DATE,
	CONSTRAINT covid_19_rates_pk PRIMARY KEY (country, date, indicator));