SELECT *  
FROM layoffs;


--remove duplicates
--standardize the data
--null values or blanks
--remove unecessary columns



CREATE TABLE LAYOFFS_STAGING
LIKE LAYOFFS;

SELECT *  
FROM layoffs_staging;

INSERT LAYOFFS_STAGING
SELECT *
FROM layoffs;

SELECT *,
row_number () OVER(
PARTITION BY COMPANY, INDUSTRY,TOTAL_LAID_OFF,PERCENTAGE_LAID_OFF,'DATE', STAGE, COUNTRY,FUNDS_RAISED_MILLIONS) AS  ROW_NUM
FROM LAYOFFS_STAGING;

WITH DUPLICATE_CTE AS 
(
SELECT *,
row_number () OVER(
PARTITION BY COMPANY, INDUSTRY,TOTAL_LAID_OFF,PERCENTAGE_LAID_OFF,'DATE', STAGE, COUNTRY,FUNDS_RAISED_MILLIONS) AS  ROW_NUM
FROM LAYOFFS_STAGING
)
SELECT * 
FROM DUPLICATE_CTE 
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE COMPANY = 'CASPER';


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging2
where row_num>1;

insert into layoffs_staging2
SELECT *,
row_number () OVER(
PARTITION BY COMPANY, INDUSTRY,TOTAL_LAID_OFF,PERCENTAGE_LAID_OFF,'DATE', STAGE, COUNTRY,FUNDS_RAISED_MILLIONS) AS  ROW_NUM
FROM LAYOFFS_STAGING;

delete
FROM layoffs_staging2
where row_num > 1;


SET SQL_SAFE_UPDATES = 0;

SELECT *
FROM layoffs_staging;

select company, trim(company)
from layoffs_staging2;

UPDATE LAYOFFS_STAGING2
set company = trim(company);

SELECT DISTINCT(INDUSTRY)
FROM layoffs_staging2;



UPDATE layoffs_staging2
SET INDUSTRY ='Crypto'
where industry like 'Crypto%';


select *
from layoffs_staging2
where country like 'United States%';

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

UPDATE layoffs_staging2
SET country = trim(trailing '.' from country)
where country like 'United States%';

SELECT DISTINCT(country)
FROM layoffs_staging2;

select `date`,
STR_TO_DATE(`DATE`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` =STR_TO_DATE(`DATE`, '%m/%d/%Y');

select `date`
from layoffs_staging2;

alter table layoffs_staging2
modify column `date` Date;


SELECT *
FROM layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

