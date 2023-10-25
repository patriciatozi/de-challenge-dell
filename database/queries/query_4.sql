SELECT (SUM(cumulative_count)/SUM(population))*100 AS "Population Density", indicator AS "Indicator", region AS Region
FROM cumulative_number_for_14_days_of_COVID_19_cases_per_100000_view
WHERE date <= '2020-07-31' AND indicator = 'cases' 
GROUP BY Region, Indicator
ORDER BY "Population Density" DESC;