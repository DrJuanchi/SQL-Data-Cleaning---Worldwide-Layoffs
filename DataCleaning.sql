SELECT * FROM layoffs;

#1. Remove Duplicates (if any)
#2. Standardize the Data
#3. NULL/BLANK values
#4. Remove unecessary rows/columns

#I'll change the table a lot. For security, create a new table to keep the raw data.
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * FROM layoffs_staging;

INSERT INTO layoffs_staging 
(SELECT * FROM layoffs);

SELECT * FROM layoffs_staging;


#1 - REMOVE DUPLICATES

#Check if there are any duplicates
WITH duplicate_cte AS
(
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * FROM duplicate_cte
WHERE row_num > 1;

#Cannot update the CTE, therefore create a new table from which we can delete the rows
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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE FROM layoffs_staging2
WHERE row_num > 1;

SELECT * FROM layoffs_staging2;

#2. STANDARDIZING DATA
#TRIM values from company
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

#Modify industries
SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

#Correct countries
SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

#Modify dates
SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

#Now Alter to change date to a DATETIME
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


#3. TACKLING NULL AND BLANK VALUES
#Rows with null total laid off and percentage laid off are possibly useless - to be seen when deleting useless rows and columns
SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

#Look at industry first - try to populate if there are other rows with the same company
#First change BLANKS to NULLS
UPDATE layoffs_staging2
SET industry=NULL
WHERE industry='';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL;

SELECT  t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

#Only Bally's couldn't be populated - there is only one row
SELECT * FROM layoffs_staging2
WHERE company LIKE 'Bally%';


#Cannot populate the other columns - could populate percentage laid off or total laid off if we have the total employees


#4. REMOVE ROWS AND COLUMNS THAT WE DON'T NEED
SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
#These are the main ones that are probably not going to be used -
#We need to know either the percentage or the total
#Only knowing the company that (in theory) had a layoff isn't useful for me

DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * FROM layoffs_staging2;

#Deleting the row_num - don't need it anymore
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
