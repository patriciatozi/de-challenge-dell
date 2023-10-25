SELECT MAX(cumulative_count) as "Cumulative Count", D.country AS Country, GDP AS "GDP ($ per capita)"
FROM (SELECT MAX('GDP ($ per capita)') AS GDP, cumulative_count, country
          FROM cumulative_number_for_14_days_of_COVID_19_cases_per_100000_view 
          WHERE indicator = 'cases' and country != 'EU/EEA (total)'
          GROUP BY cumulative_count, Country
          ORDER BY GDP DESC) D
GROUP BY Country, GDP
ORDER BY "Cumulative Count" DESC
LIMIT 10;