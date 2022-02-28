# Group 6
# Normalization of country_vaccinations,  country_vaccinations_by_manufacturer, covid19data

## -------------------------------------------------------------------------------------------------
# 1. country table (done)
DROP TABLE IF EXISTS `country`;
CREATE TABLE `country`
(
	`iso_code` varchar(255) PRIMARY KEY,
    country VARCHAR(255),
    continent VARCHAR(255)
);

INSERT INTO country (iso_code, country, continent)
SELECT * FROM
(SELECT distinct(cv.iso_code), cv.country, c.continent 
FROM country_vaccinations cv
LEFT JOIN covid19data c ON cv.iso_code = c.iso_code 
UNION
SELECT distinct(iso_code), location, continent FROM covid19data) as t1;

## -------------------------------------------------------------------------------------------------
# 2. source 
DROP TABLE IF EXISTS `source`;
CREATE TABLE source
(
	countryiso_code VARCHAR(255),
    source_name VARCHAR(255),
    source_website VARCHAR(255),
	primary key (countryiso_code, source_name),
     CONSTRAINT `source_fk`
		FOREIGN KEY (`countryiso_code`)
        REFERENCES `country_vaccinations`.`country`(`iso_code`)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

INSERT INTO source(countryiso_code, source_name, source_website)
SELECT distinct(c.iso_code), cv.source_name, cv.source_website
FROM country_vaccinations cv
INNER JOIN country c
ON cv.country = c.country;

SELECT * FROM source;

## -------------------------------------------------------------------------------------------------
# 3. vaccine_manu 
DROP TABLE IF EXISTS `vaccine_manu`;
CREATE TABLE vaccine_manu(
	iso_code VARCHAR(255),
	date_manu DATE,
	vaccines VARCHAR(255),
	primary key (iso_code, date_manu, vaccines),
	total_vaccinations_byVaccines int DEFAULT 0,
    CONSTRAINT `country_manu_fk`
		FOREIGN KEY (`iso_code`)
        REFERENCES `country_vaccinations`.`country`(`iso_code`)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

INSERT INTO vaccine_manu (iso_code, date_manu, vaccines, total_vaccinations_byVaccines)
SELECT DISTINCT(country.iso_code), cm.date, cm.vaccine, cm.total_vaccinations
FROM country 
INNER JOIN country_vaccinations_by_manufacturer cm
ON country.country = cm.location;

SELECT * FROM vaccine_manu;


## -------------------------------------------------------------------------------------------------
# 4. country_stats 
DROP TABLE IF EXISTS `country_stats`;
CREATE TABLE country_stats (
     iso_code VARCHAR(255),
     date DATE,
     primary key (iso_code, date),
	`icu_patients` text,
	`icu_patients_per_million` text,
    `hosp_patients`text,
    `hosp_patients_per_million` text,
    `weekly_icu_admissions` text,
    `weekly_icu_admissions_per_million` text,
    `weekly_hosp_admissions` text,
    `weekly_hosp_admissions_per_million` text,
    `population` text,
    `population_density` text,
    `median_age` text,
    `aged_65_older` text,
    `aged_70_older` text,
    `hospital_beds_per_thousand` text,
    CONSTRAINT `country_stats_fk`
		FOREIGN KEY (`iso_code`)
        REFERENCES `country_vaccinations`.`country`(`iso_code`)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

INSERT INTO country_stats (iso_code, date, icu_patients, icu_patients_per_million, hosp_patients, hosp_patients_per_million, 
	weekly_icu_admissions, weekly_icu_admissions_per_million, weekly_hosp_admissions, weekly_hosp_admissions_per_million, 
	population, population_density, median_age, aged_65_older, aged_70_older, hospital_beds_per_thousand)
SELECT DISTINCT(iso_code), date, icu_patients, icu_patients_per_million, hosp_patients, hosp_patients_per_million, 
	weekly_icu_admissions, weekly_icu_admissions_per_million, weekly_hosp_admissions, weekly_hosp_admissions_per_million, 
	population, population_density, median_age, aged_65_older, aged_70_older, hospital_beds_per_thousand
FROM covid19data;

SELECT * FROM country_stats;

## -------------------------------------------------------------------------------------------------
# 5. case_stats 
DROP TABLE IF EXISTS `case_stats`;
CREATE TABLE case_stats (
     iso_code VARCHAR(255),
     date DATE,
     primary key (iso_code, date),
	`total_cases` text,
	`new_cases` text,
    `new_cases_smoothed`text,
    `total_deaths` text,
    `new_deaths` text,
    `new_deaths_smoothed` text,
    `total_cases_per_million` text,
    `new_cases_per_million` text,
    `new_cases_smoothed_per_million` text,
    `total_deaths_per_million` text,
    `new_deaths_per_million` text,
    `new_deaths_smoothed_per_million` text,
    `new_tests` text,
    `total_tests` text,
    `total_tests_per_thousand` text,
    `new_tests_per_thousand` text,
    `new_tests_smoothed` text,
    `new_tests_smoothed_per_thousand` text,
    `positive_rate` text,
    `tests_per_case` text,
    `tests_units` text,
    CONSTRAINT `case_stats_fk`
		FOREIGN KEY (`iso_code`)
        REFERENCES `country_vaccinations`.`country`(`iso_code`)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

INSERT INTO case_stats (iso_code, date, total_cases, new_cases, new_cases_smoothed, total_deaths, new_deaths, new_deaths_smoothed,
	total_cases_per_million, new_cases_per_million, new_cases_smoothed_per_million, total_deaths_per_million, new_deaths_per_million,
    new_deaths_smoothed_per_million, new_tests, total_tests, total_tests_per_thousand, new_tests_per_thousand, new_tests_smoothed_per_thousand,
    positive_rate, tests_per_case, tests_units)
SELECT DISTINCT(iso_code), date, total_cases, new_cases, new_cases_smoothed, total_deaths, new_deaths, new_deaths_smoothed,
	total_cases_per_million, new_cases_per_million, new_cases_smoothed_per_million, total_deaths_per_million, new_deaths_per_million,
    new_deaths_smoothed_per_million, new_tests, total_tests, total_tests_per_thousand, new_tests_per_thousand, new_tests_smoothed_per_thousand,
    positive_rate, tests_per_case, tests_units
FROM covid19data;

SELECT * FROM case_stats;


## -------------------------------------------------------------------------------------------------
# 6. vaccination_stats
DROP TABLE IF EXISTS `vaccination_stats`;
CREATE TABLE vaccination_stats (
     iso_code VARCHAR(255),
     date Date,
     primary key (iso_code, date),
	`daily_vaccinations_raw` decimal(20,4) unsigned DEFAULT('0.000'),
	`daily_vaccinations` decimal(20,4) unsigned DEFAULT('0.000'),
    `daily_vaccinations_per_million` decimal(20,4) unsigned DEFAULT('0.000'),
    `total_vaccinations`text,
    `total_vaccinations_per_hundred` text,
    `people_vaccinated` text,
    `people_vaccinated_per_hundred` text,
    `people_fully_vaccinated` text,
    `people_fully_vaccinated_per_hundred` text,
    `new_vaccinations` text,
    `new_vaccinations_smoothed` text,
    `new_vaccinations_smoothed_per_million` text,
    CONSTRAINT `vaccination_stats_fk`
		FOREIGN KEY (`iso_code`)
        REFERENCES `country_vaccinations`.`country`(`iso_code`)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

# Inserting info regarding covid19data first 
INSERT INTO vaccination_stats(iso_code, date, total_vaccinations,total_vaccinations_per_hundred, people_vaccinated, people_vaccinated_per_hundred, 
	people_fully_vaccinated, new_vaccinations, new_vaccinations_smoothed, new_vaccinations_smoothed_per_million)
SELECT DISTINCT(iso_code), date, total_vaccinations,total_vaccinations_per_hundred, people_vaccinated, people_vaccinated_per_hundred, 
	people_fully_vaccinated, new_vaccinations, new_vaccinations_smoothed, new_vaccinations_smoothed_per_million
FROM covid19data; 

# Inserting info regarding country_vaccinations
UPDATE vaccination_stats vs JOIN country_vaccinations cv 
ON vs.iso_code = cv.iso_code
AND vs.date = str_to_date(cv.date, "%m/%d/%Y")
SET 
vs.daily_vaccinations_raw = cv.daily_vaccinations_raw, 
vs.daily_vaccinations = cv.daily_vaccinations, 
vs.daily_vaccinations_per_million = cv.daily_vaccinations_per_million;


SELECT * FROM vaccination_stats;



