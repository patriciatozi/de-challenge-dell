SELECT MAX(date) as "Closest Date", MAX(cumulative_count) AS "Cumulative Count", country AS Country
FROM  cumulative_number_for_14_days_of_COVID_19_cases_per_100000_view
WHERE  date <= '2020-07-31' AND indicator = 'cases' AND Country != 'EU/EEA (total)'
GROUP BY Country
ORDER BY "Cumulative Count" DESC
LIMIT 1;