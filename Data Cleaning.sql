-- Data Cleaning
#we have added the World_layoff data into the Schema database and imported the layoff doc from the system

SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data - Spelling errors
-- 3. NULL/Blank values - Populate them when required
-- 4. Remove any rows or columns 

#Deleting the data from raw data will be wrong as we migraate different data set for work officially it may cause error, 
-- so we are creating a new data to duplicate it as staging data

CREATE TABLE layoff_staging
LIKE layoffs;

SELECT *
FROM layoff_staging;
#columns created

#Insert Data
INSERT layoff_staging
SELECT *
FROM layoffs;

#Row
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off,`date`) AS row_num
FROM layoff_staging;

#CTE or Subqurey

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
FROM layoff_staging )
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoff_staging
WHERE company = 'casper';

#DELETE (1)
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
FROM layoff_staging )
DELETE
FROM duplicate_cte
WHERE row_num > 1;

-- as it cannot be deleted from the fixed data we are creating a stage data 2 where we create the table and deleting the column, row = 2/duplicate

# Right click on layoff_staging - copy to clipboard and create statement:

CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- We have created the new table and we have added the row num into the actual data to filter out the duplicate
SELECT *
FROM layoff_staging2;

INSERT INTO layoff_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
FROM layoff_staging;

# Duplicate
SELECT *
FROM layoff_staging2
WHERE row_num > 1;

#Delete
DELETE 
FROM layoff_staging2
WHERE row_num > 1;

# 1175 error safe edit to be disabled to delete and update the query output
SELECT *
FROM layoff_staging2;

-- STANDARDIZING DATA
SELECT company,TRIM(company)
FROM layoff_staging2;

UPDATE layoff_staging2
SET company = trim(company);

SELECT DISTINCT industry
FROM layoff_staging2
order by 1;

-- Example: Crypto, Crypto Currency, CryptoCurrency
SELECT *
FROM layoff_staging2
WHERE industry LIKE 'crypto%';

UPDATE layoff_staging2
SET industry = 'crypto'
WHERE industry LIKE 'crypto%';

SELECT DISTINCT industry
FROM layoff_staging2
;

-- United States.
SELECT *
FROM layoff_staging2
WHERE country LIKE 'United States%'
;

-- Triming the Period - to remove the period instead of a Space
SELECT DISTINCT country, trim(TRAILING '.' FROM country)
FROM layoff_staging2
ORDER BY 1
;

UPDATE layoff_staging2
SET country = trim(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'
;

-- TIME SERIES: Date is in Text column how to change the format ('%m'/%d/%Y)
SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoff_staging2;

UPDATE layoff_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y')
;

SELECT `date`
FROM layoff_staging2;

-- Only do this in staging table never on a raw data, it is changing the date type (Text to date)
ALTER TABLE layoff_staging2
MODIFY COLUMN `date` date; 

-- NULL & Blank values
SELECT *
FROM layoff_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoff_staging2
WHERE industry IS NULL OR industry = '';

SELECT *
FROM layoff_staging2
WHERE company = 'airbnb';

# Filling in Travel in Industry where it is blank as per the Similar data ref
SELECT *
FROM layoff_staging2 T1
JOIN layoff_staging2 T2
	ON T1.company = T2.company
    AND T1.location = T2.location
WHERE (T1.industry IS NULL OR T1.industry = '')
AND T2.industry IS NOT NULL
;

UPDATE layoff_staging2 T1
JOIN layoff_staging2 T2
	ON T1.company = T2.company
SET T1.industry = T2.industry
WHERE (T1.industry IS NULL OR T1.industry = '')
AND T2.industry IS NOT NULL
;

UPDATE layoff_staging2
SET industry = NULL
WHERE industry = '';

UPDATE layoff_staging2 T1
JOIN layoff_staging2 T2
	ON T1.company = T2.company
SET T1.industry = T2.industry
WHERE T1.industry IS NULL 
AND T2.industry IS NOT NULL
;

SELECT *
FROM layoff_staging2
WHERE company = 'airbnb'; -- is working now, null was changed to the proper industry

SELECT *
FROM layoff_staging2
WHERE company LIKE 'Bally%'
;

-- deleting COLUMN & ROW

SELECT *
FROM layoff_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoff_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Drop a Column from the Table - Row_num
ALTER TABLE layoff_staging2
DROP COLUMN row_num
;

SELECT *
FROM layoff_staging2
;








