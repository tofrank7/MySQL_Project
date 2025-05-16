-- Data Cleaning

SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove Any Columns

-- Very first step is to create staging table to work off of, never work on the raw data
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 1A. Use ROW_NUMBER() and PARTITION BY all columns to find the duplicates (anything with row numbers greater than 1)

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage, country
, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- The above query is all of our duplicates together
-- Example of how to see one of the reals and duplicates together (goal: we want to delete one of them):
SELECT *
FROM layoffs_staging
WHERE company = 'Casper';


-- 1B. Create another staging table (layoffs_staging2) to remove the duplicates

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

-- Our duplicates in the layoffs_staging2 table we just created, can use to check work:
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Inserting the data with row numbers into the layoffs_staging2 table we just created:
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage, country
, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- 1C. Once the data is inserted and layoffs_staging2 table is created, we can then delete duplicates (anything with row number greater than 1 since we partitioned by all columns)
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Check your work to see if duplicates are gone (row numbers should be all 1 now):
SELECT *
FROM layoffs_staging2;


-- 2. Standardizing Data
-- First step is to select each column, scroll through it, and find potential things to fix:

-- 2A. Removing extra spaces in company name
-- Scroll through the distinct list of companies. We notice exxtra spaces
SELECT DISTINCT(company)
FROM layoffs_staging2;

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- 2B. Changing anything Crypto% to be just Cyrpto so that they can be grouped together later on. If it's like Crypto, it should just be Crypto
SELECT DISTINCT industry
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- 2C. Removing a period after 'United States' in one of the rows
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- 2D. Standardizing the date (from MM-DD-YYYY to YYYY-MM-DD, and changing the data type from text to date
SELECT `date`
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Null Values or blank values

-- 3A. When scrolling through the industry column, we notice blank values AND null values
SELECT DISTINCT industry
FROM layoffs_staging2;

-- First, let's change blanks to null. This step is important for later on
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Now let's select everything and see which companies have a NULL in industry. What we can do is see if we can populate the industry for any of them
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL;

-- From the above query, let's take a closer look at Airbnb. The output shows Airbnb with the NULL value, as well as another Airbnb with Travel as the industry. So, we want to populate Travel into the Airbnb row with null 
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Airbnb%';

-- When you check Bally, it will be the only one with a null still because there wasn't another Bally row with the industry filled out (e.g., there was only 1 Bally row in the dataset)
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

-- We will join layoffs_staging2 to itself and fill in those null values where we can (might not be possible for all null values, as mentioned above with Bally)
SELECT t1.industry, t2.industry
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
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

-- 3B. Some rows don't have total_laid_off and percentage_laid_off, and we cannot data in because it's something we cannot make up. May be best to delete these rows because without this info, these rows are useless

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- 4. Remove Any Columns
-- 4A. Let's remove the row_num column, which we don't need anymore since we remove the duplicates already.
SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;