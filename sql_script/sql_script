-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022




SELECT *
FROM layoffs;

-- now when we are data cleaning we usually follow a few steps
-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null values or Blank values
-- 4. Remove any columns 

-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens
CREATE TABLE layoffs_staging 
LIKE layoffs;

INSERT layoffs_staging
SELECT * 
FROM layoffs;

SELECT * 
FROM layoffs_staging;





--  1. REMOVING DUPLICATES 


--  assigning row number for each row (if there is same row exist it'll assign same row number to it )
SELECT *,
ROW_NUMBER() 
OVER(PARTITION BY company,location,total_laid_off,`date`,
percentage_laid_off,industry,`source`,
stage,funds_raised,country,date_added) AS row_num
FROM layoffs_staging;

--  checking whether there is duplicate row or not using CTEs
--  if row_num >  then duplicate row 
WITH duplicate_cte AS
(
	SELECT *,
	ROW_NUMBER() 
	OVER(PARTITION BY company,location,total_laid_off,`date`,
	percentage_laid_off,industry,`source`,
	stage,funds_raised,country,date_added) AS row_num
	FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

-- ***** THERE IS NO DUPLICATE ROWS IN THE DATASET

--   IF THERE WERE DUPLICATE ROWS PRESENT THEN FOLLOWING OPERATION NEED TO PERFORM

--  this below query will give error 
-- reason: we can not delete or update CTEs
WITH duplicate_cte AS
(
	SELECT *,
	ROW_NUMBER() 
	OVER(PARTITION BY company,location,total_laid_off,`date`,
	percentage_laid_off,industry,`source`,
	stage,funds_raised,country,date_added) AS row_num
	FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;

-- we can not able to delete duplicates by using CTE
-- so we created new table which have row_num col
-- and now we are deleting the row which have row_num > 1

-- created table 
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `total_laid_off` double DEFAULT NULL,
  `date` text,
  `percentage_laid_off` text,
  `industry` text,
  `source` text,
  `stage` text,
  `funds_raised` text,
  `country` text,
  `date_added` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging2;

-- inserted previous data with row_num 
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() 
OVER(PARTITION BY company,location,total_laid_off,`date`,
percentage_laid_off,industry,`source`,
stage,funds_raised,country,date_added) AS row_num
FROM layoffs_staging;

SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

-- DELETED
SET SQL_SAFE_UPDATES = 0;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 1;

SELECT * 
FROM layoffs_staging2;





-- 2. Standardizing the data 


SELECT * 
FROM layoffs_staging2;

SELECT distinct company, TRIM(company) 
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- industry column is okay 
SELECT DISTINCT industry 
FROM layoffs_staging2
ORDER BY 1;

-- location column is okay 
SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

-- country column is okay  
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

--  we noticed that `date` column is of type text
SELECT `date`
FROM layoffs_staging2;

-- converting string to date
SELECT `date`, 
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

--  updated the `date` column 
--  but still it is of type text
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

-- changed the `date` column text to date 
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- we also have `date_added` column that needs to be converted into date 
SELECT date_added
FROM layoffs_staging2;

SELECT `date_added`, 
STR_TO_DATE(`date_added`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET date_added = STR_TO_DATE(`date_added`,'%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN date_added DATE ;





-- 3. NULLS & BLANK VALUES 


SELECT  * 
FROM layoffs_staging2
WHERE industry = '';

UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';

SELECT  * 
FROM layoffs_staging2
WHERE industry IS NULL;

SELECT * 
FROM layoffs_staging2
WHERE company = 'Appsmith';

-- if there is a industry assigned to the company we might be able to populate the data but in this case we can not  

SELECT COUNT(company) 
FROM layoffs_staging2
WHERE percentage_laid_off ='';

-- few of the ROWS have blank percentage_laid_off column
-- which is not usefull for us 
-- we neet to delete those rows 

SELECT * 
FROM layoffs_staging2;

-- deleting the rows who have blank percentage_laid_off
DELETE 
FROM layoffs_staging2
WHERE percentage_laid_off ='';


SELECT * 
FROM layoffs_staging2;




-- 4.removed any rows or columns


-- we dont need row_num column now 
-- deleting the row_num column
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;

-- DATA CLEANING IS DONE
-- 1. we removed the duplicate rows 
-- 2. we standardized the data 
-- 3. searched null or blanck values (tried to populate them)
-- 4. removed any rows or columns 
