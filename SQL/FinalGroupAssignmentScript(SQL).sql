# Question 1
SELECT SUM(population) as Population_Asia FROM 
	(SELECT DISTINCT(iso_code), max(date), population FROM covid19data 
	WHERE continent = "Asia"
	GROUP BY iso_code, population) as t1;
    
## -------------------------------------------------------------------------------------------------    
# Question 2
SELECT SUM(DISTINCT(population)) AS total_population_ASEAN
FROM covid19data
WHERE location IN ('Brunei', 'Cambodia', 'Indonesia', 'Laos', 'Malaysia',
 'Myanmar', 'Philippines', 'Singapore', 'Thailand', 'Vietnam');

## -------------------------------------------------------------------------------------------------    
# Question 3
select distinct(source_name) 
from country_vaccinations
order by source_name;

## -------------------------------------------------------------------------------------------------    
# Question 4
SELECT date, total_vaccinations
FROM covid19data
WHERE iso_code = "SGP" AND date between '2021-03-01' AND '2021-05-31';

## -------------------------------------------------------------------------------------------------
# Question 5
select min(cvd.date) as First_batch_of_vaccinations
from covid19data cvd
where iso_code = "SGP"
and cvd.total_vaccinations > 0;

## -------------------------------------------------------------------------------------------------
# Question 6
select sum(cvd.new_cases) as Total_number_of_cases_after_first_batch_of_vaccinations
from covid19data cvd
where cvd.date >= 
  (select min(cvd.date) as First_batch_of_vaccinations
from covid19data cvd
where iso_code = "SGP"
and cvd.total_vaccinations>0)
and iso_code = "SGP";

## -------------------------------------------------------------------------------------------------
# Question 7
select sum(cvd.new_cases) as Total_number_of_cases_before_first_batch_of_vaccinations
from covid19data cvd
where cvd.date <
  (select min(cvd.date) as First_batch_of_vaccinations
from covid19data cvd
where iso_code = "SGP"
and cvd.total_vaccinations>0)
and iso_code = "SGP";

## -------------------------------------------------------------------------------------------------
# Question 8
SELECT t1.date, t1.location, t2.perc_new_cases, t1.vaccine, t1.total_vaccinations/t2.population*100 as perc_total_vac FROM
(SELECT * FROM country_vaccinations_by_manufacturer
WHERE location = "Germany") as t1
LEFT JOIN 
(SELECT date, population, new_cases/population*100 as perc_new_cases
 FROM covid19data
 WHERE location = "Germany" 
 ORDER BY date) as t2
ON t1.date = t2.date
ORDER BY date;


## -------------------------------------------------------------------------------------------------
# Question 9 
select t1.date, t1.new_cases, t2. 20_days, t2.vaccine , t2.max_vac_20days, t3. 30_days,
t3.vaccine, t3.max_vac_30days, t4. 40_days, t4.vaccine, t4.max_vac_40days from
(SELECT date, new_cases
 FROM covid19data
 WHERE location = "Germany" 
 ORDER BY date) t1
   left join
(SELECT location, date as 20_days, vaccine, total_vaccinations AS max_vac_20days
 FROM country_vaccinations_by_manufacturer
 WHERE location = "Germany" 
 order by date asc) t2
 on datediff(t2. 20_days, t1.date)=20
  left join
(SELECT location, date as 30_days, vaccine, total_vaccinations AS max_vac_30days
 FROM country_vaccinations_by_manufacturer
 WHERE location = "Germany" 
 order by date asc) t3
on (datediff(t3. 30_days, t1.date)=30
 and
 t3.vaccine = t2.vaccine)
   left join
(SELECT location, date as 40_days, vaccine, total_vaccinations AS max_vac_40days
 FROM country_vaccinations_by_manufacturer
 WHERE location = "Germany" 
 order by date asc) t4
 on (datediff(t4. 40_days, t1.date)=40
 and
 t4.vaccine = t2.vaccine);
 
 
## -------------------------------------------------------------------------------------------------
# Question 10 
 select * from 
(select date, sum(total_vaccinations) as total_number_of_accumulated_vaccinations 
 FROM country_vaccinations_by_manufacturer
 WHERE location = "Germany" 
 group by date) t1
 left join 
 (select date as 21_days, new_cases as daily_new_cases_21days
 from covid19data
 where location = "Germany" 
 ) t2
 on datediff(t2. 21_days, t1.date)=21
 
 left join 
 (select date as 60_days, new_cases as daily_new_cases_60days
 from covid19data
 where location = "Germany" 
 ) t3
 on datediff(t3. 60_days, t1.date)=60
 
 left join 
 (select date as 120_days, new_cases as daily_new_cases_120days
 from covid19data
 where location = "Germany" 
 ) t4
 on datediff(t4. 120_days, t1.date)=120;
 