SELECT date, indicator, country_code, COUNT(*)
FROM cumulative_number_for_14_days_of_covid_19_cases_per_100000_view
GROUP BY date, indicator, country_code
HAVING COUNT(*) > 1;