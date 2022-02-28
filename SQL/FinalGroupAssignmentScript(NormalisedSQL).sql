## -------------------------------------------------------------------------------------------------
# Question 1
SELECT sum(pop) AS Population_Asia FROM 
(SELECT MAX(population) as pop FROM country_stats 
WHERE iso_code IN 
	(SELECT DISTINCT(iso_code) FROM country 
	WHERE continent = "Asia")
GROUP BY iso_code) as t1;

## -------------------------------------------------------------------------------------------------
# Question 2
SELECT SUM(DISTINCT(population)) AS total_population_ASEAN
FROM country_stats
WHERE iso_code IN ('BRN', 'IDN', 'KHM', 'LAO', 'MMR',
 'MYS', 'PHL', 'SGP', 'THA', 'VNM');
 
## -------------------------------------------------------------------------------------------------
# Question 3
select distinct(source_name) 
from source
order by source_name;

## -------------------------------------------------------------------------------------------------
# Question 4
SELECT date, total_vaccinations
FROM vaccination_stats
WHERE iso_code = "SGP" AND date between '2021-03-01' AND '2021-05-31';

## -------------------------------------------------------------------------------------------------
# Question 5
 select min(vs.date) as First_batch_of_vaccinations
from vaccination_stats vs
where iso_code = "SGP"
and vs.total_vaccinations > 0;

## -------------------------------------------------------------------------------------------------
# Question 6
select sum(cs.new_cases) as Total_number_of_cases_after_first_batch_of_vaccinations
from case_stats cs
where cs.date >= 
  (select min(vs.date) as First_batch_of_vaccinations
from vaccination_stats vs
where iso_code = "SGP"
and vs.total_vaccinations>0)
and iso_code = "SGP";

## -------------------------------------------------------------------------------------------------
# Question 7
select sum(cs.new_cases) as Total_number_of_cases_before_first_batch_of_vaccinations
from case_stats cs
where cs.date <
  (select min(vs.date) as First_batch_of_vaccinations
from vaccination_stats vs
where iso_code = "SGP"
and vs.total_vaccinations>0)
and iso_code = "SGP";

## -------------------------------------------------------------------------------------------------
# Question 8
SELECT t1.date_manu, t1.iso_code, t3.new_cases/t2.population*100 as perc_new_cases, t1.vaccines, t1.total_vaccinations_byVaccines/t2.population*100 as perc_total_vac FROM
(SELECT * FROM vaccine_manu 
 WHERE iso_code = "DEU") as t1
 LEFT JOIN 
 (SELECT population, date FROM country_stats
 WHERE iso_code = "DEU") as t2
 ON t1.date_manu = t2.date
 LEFT JOIN 
 (SELECT date, new_cases
 FROM case_stats
 WHERE iso_code = "DEU") as t3
 on t1.date_manu = t3.date
 ORDER BY t1.date_manu;
 
## -------------------------------------------------------------------------------------------------
# Question 9
SELECT t1.date, t1.new_cases, t2. 20_days, t2.vaccines, t2.max_vac_20days, t3. 30_days,
t3.vaccines, t3.max_vac_30days, t4. 40_days, t4.vaccines, t4.max_vac_40days FROM
(SELECT date, new_cases
FROM case_stats 
WHERE iso_code = "DEU"
ORDER BY date) t1
LEFT JOIN
(SELECT iso_code, date_manu as 20_days, vaccines, total_vaccinations_byVaccines AS max_vac_20days
FROM vaccine_manu
WHERE iso_code = "DEU"
order by date_manu asc) t2
on datediff(t2. 20_days, t1.date)=20
LEFT JOIN
(SELECT iso_code, date_manu as 30_days, vaccines, total_vaccinations_byVaccines AS max_vac_30days
FROM vaccine_manu
WHERE iso_code = "DEU"
order by date_manu asc) t3
ON (datediff(t3. 30_days, t1.date)=30
AND t3.vaccines = t2.vaccines)
LEFT JOIN
(SELECT iso_code, date_manu as 40_days, vaccines, total_vaccinations_byVaccines AS max_vac_40days
FROM vaccine_manu
WHERE iso_code = "DEU"
order by date_manu asc) t4
ON (datediff(t4. 40_days, t1.date)=40
AND t4.vaccines = t2.vaccines);

## -------------------------------------------------------------------------------------------------
# Question 10
SELECT * FROM
(SELECT date_manu, sum(total_vaccinations_byVaccines) as total_number_of_accumulated_vaccinations 
FROM vaccine_manu
WHERE iso_code = "DEU" 
GROUP BY date_manu) t1
LEFT JOIN
(SELECT date as 21_days, new_cases as daily_new_cases_21days
 FROM case_stats
 WHERE iso_code = "DEU") t2
on datediff(t2. 21_days, t1.date_manu) = 21
LEFT JOIN
(SELECT date as 60_days, new_cases as daily_new_cases_60days
 FROM case_stats
 WHERE iso_code = "DEU") t3
on datediff(t3. 60_days, t1.date_manu) = 60
LEFT JOIN
(SELECT date as 120_days, new_cases as daily_new_cases_120days
 FROM case_stats
 WHERE iso_code = "DEU") t4
on datediff(t4. 120_days, t1.date_manu) = 120;