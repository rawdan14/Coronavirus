# --Below appended the SQL queries used to explore the COVID dataset for insights on infected case, death and vaccination


# --check the first imported excel on COVID death data
SELECT *
FROM dbo.COVIDDEATH
ORDER BY 3,4;

# --check the second imported excel on COVID vaccination data
SELECT *
FROM dbo.COVIDVACCINATION
ORDER BY 3,4;

# --check the data type
SELECT DATA_TYPE, COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'COVIDDEATH';

SELECT DATA_TYPE, COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'COVIDVACCINATION';

# --convert data type for COVIDDEATH dataset
ALTER TABLE dbo.COVIDDEATH
ALTER COLUMN date date
 
ALTER TABLE dbo.COVIDDEATH
ALTER COLUMN population decimal

ALTER TABLE dbo.COVIDDEATH
ALTER COLUMN total_cases decimal
 
ALTER TABLE dbo.COVIDDEATH
ALTER COLUMN total_deaths decimal
 
# --identify infected rate
SELECT location, date, total_cases, population, (total_cases / population)*100 AS infected_rate
FROM dbo.COVIDDEATH
WHERE location = 'Canada'
ORDER BY 2 desc
 
# --identify death rate of infected case
SELECT location, date, total_cases, total_deaths, (total_deaths / total_cases)*100 AS death_rate
FROM dbo.COVIDDEATH
WHERE location = 'Canada'
ORDER BY 2 desc
 
# --identify percentage of death in population
SELECT location, date, total_cases, total_deaths, (total_deaths / population)*100 AS death_percent
FROM dbo.COVIDDEATH
WHERE location = 'Canada'
ORDER BY 2 desc
 
# --compare the infected rate among countries
SELECT location, MAX(total_cases) AS highest_infection_count, population, (MAX(total_cases) / population)*100 AS highest_infected_rate
FROM dbo.COVIDDEATH
WHERE continent is not null
GROUP BY location, population
ORDER BY 4 desc
 
# --compare the death number among countries
SELECT location, MAX(total_deaths) AS highest_death_count
FROM dbo.COVIDDEATH
WHERE continent is not null
GROUP BY location
ORDER BY 2 desc
 
# --compare the death percentage of population among countries
SELECT location, MAX(total_deaths) AS highest_death_count, population, (MAX(total_deaths) / population)*100 AS highest_death_percent
FROM dbo.COVIDDEATH
WHERE continent is not null
GROUP BY location, population
ORDER BY 4 desc
 
# --compare the latest death rate of infected case of the last 30 days among countries
SELECT ttt.location, AVG(ttt.death_rate) AS avg_death_rate
FROM (
SELECT tt.location, tt.date, (tt.total_deaths / tt.total_cases)*100 AS death_rate
FROM dbo.COVIDDEATH tt
INNER JOIN (
     SELECT DISTINCT TOP 30 date
     FROM dbo.COVIDDEATH
 	ORDER BY date DESC
 ) t on tt.date = t.date
 ) ttt
 GROUP BY ttt.location
 ORDER BY avg_death_rate DESC
 
# --find out the death rate ranking of a specific location
DROP TABLE IF EXISTS temptable
 
SELECT row_number() over (ORDER BY avg_death_rate desc) AS rank, location, avg_death_rate
INTO temptable
FROM (
    SELECT ttt.location, AVG(ttt.death_rate) AS avg_death_rate
    FROM (
            SELECT tt.location, tt.date, (tt.total_deaths / tt.total_cases)*100 AS death_rate
            FROM dbo.COVIDDEATH tt
            INNER JOIN (
                          SELECT DISTINCT TOP 30 date
                          FROM dbo.COVIDDEATH
                          ORDER BY date DESC
                        ) t on tt.date = t.date
         ) ttt
    GROUP BY ttt.location
    )tttt
ORDER BY rank
 
SELECT *
FROM temptable
WHERE location = 'Canada'
 
# --identify global trend on new infected case and new death
SELECT date, SUM(CAST(new_cases AS decimal)) AS global_new_cases, 
        SUM(CAST(new_cases AS decimal))/SUM(population)*100 AS new_infected_rate, 
        SUM(CAST(new_deaths AS decimal)) AS global_new_deaths, 
        SUM(CAST(total_deaths AS decimal))/SUM(CAST(total_cases AS decimal))*100 as global_death_rate
FROM dbo.COVIDDEATH
WHERE continent is not null 
GROUP BY date
ORDER BY 1 DESC
 
# --identify global trend in vaccination
WITH vactable (location, date, population, new_vaccinations, accumulative_vaccination)
AS (
    SELECT d.location, d.date, population, new_vaccinations, SUM(CAST(v.new_vaccinations AS numeric)) OVER (PARTITION BY d.Location Order by d.location, d.Date) as accumulative_vaccination
    FROM dbo.COVIDDEATH d
    JOIN dbo.COVIDVACCINATION v
    ON d.location = v.location
    AND d.date = v.date
    WHERE d.continent is not null
    )
SELECT *, (accumulative_vaccination/population)*100 AS accumulative_vac_percentage
FROM vactable
 
# --vaccination rate vs death rate
SELECT d.location, d.date, population, total_cases, 
 		new_cases, (CAST(new_cases AS numeric)/population)*100 AS new_infected_rate, 
 		new_deaths, (CAST(total_deaths AS numeric)/CAST(total_cases AS numeric))*100 as death_rate,
 		people_vaccinated, (people_vaccinated/population)*100 AS vaccinated_rate
 FROM dbo.COVIDDEATH d
 JOIN dbo.COVIDVACCINATION v
 ON d.location =v.location AND d.date = v.date
 WHERE d.continent is not null 
 ORDER BY 1,2
 
# --from the above query result, it was observed that when there is no new vaccination, the number of people_vaccinated becomes null instead of remaining at the previous value
# --to perform data cleaning to auto-fill the null value of people_vaccinated with the last known value
 
# --(1) create temp table for the last query result
DROP TABLE IF EXISTS vactable
 
SELECT d.location, d.date, population, total_cases, 
 		new_cases, (CAST(new_cases AS numeric)/population)*100 AS new_infected_rate, 
 		new_deaths, (CAST(total_deaths AS numeric)/CAST(total_cases AS numeric))*100 as death_rate,
 		people_vaccinated, (people_vaccinated/population)*100 AS vaccinated_rate
 INTO vactable
 FROM dbo.COVIDDEATH d
 JOIN dbo.COVIDVACCINATION v
 ON d.location =v.location AND d.date = v.date
 WHERE d.continent is not null 
 ORDER BY 1,2
 
# --(2)assign serial number to the rows for subsequent auto-fill
 WITH    CTE
           AS (SELECT location, date, people_vaccinated, ROW_NUMBER() OVER (PARTITION BY location ORDER BY date) AS sn
               FROM vactable),
# --(3)use recursive CTE to auto-fill null
         FILLED
           AS (SELECT location, date, ISNULL(people_vaccinated, 0) people_vaccinated, sn
               FROM  CTE c
               WHERE sn = 1
               UNION ALL
               SELECT cc.location, cc.date, ISNULL(cc.people_vaccinated, f.people_vaccinated) people_vaccinated, cc.sn
               FROM  CTE cc
               INNER JOIN FILLED f ON cc.location = f.location AND cc.sn = f.sn + 1)
 UPDATE vactable
     SET people_vaccinated = FILLED.people_vaccinated
     FROM vactable
     INNER JOIN FILLED ON vactable.location = FILLED.location AND vactable.date = FILLED.date
# --(4)display cleaned data result
 SELECT *
 FROM vactable
 ORDER BY location, date DESC