-- Exploratory Data Analysis

SELECT *
FROM layoffs_staging2;

SELECT MAX(total_laid_off), MIN(total_laid_off)
FROM layoffs_staging2;

SELECT MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT company, SUM(total_laid_off) AS sum_laid_off
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT industry, SUM(total_laid_off) AS sum_laid_off
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC
LIMIT 10;

SELECT country, SUM(total_laid_off) AS sum_laid_off
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC
LIMIT 10;

SELECT YEAR(`date`) AS `year`, SUM(total_laid_off) AS sum_laid_off
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off) AS sum_laid_off
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC
LIMIT 10;


SELECT substring(`date`, 1, 7) AS `MONTH`, 
SUM(total_laid_off) AS sum_laid_off 
FROM layoffs_staging2
WHERE substring(`date`, 1, 7) IS NOT NULL
GROUP BY substring(`date`, 1, 7)
ORDER BY 1 ASC;

WITH Rolling_Total AS
(
SELECT substring(`date`, 1, 7) AS `MONTH`, 
SUM(total_laid_off) AS sum_laid_off 
FROM layoffs_staging2
WHERE substring(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, 
sum_laid_off, 
SUM(sum_laid_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;



SELECT company, YEAR(`date`), SUM(total_laid_off) AS sum_laid_off
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_Year AS
(
SELECT company, YEAR(`date`) AS years, SUM(total_laid_off) AS sum_laid_off
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
)
SELECT *, 
DENSE_RANK() OVER(PARTITION BY years ORDER BY sum_laid_off DESC) AS ranking
FROM Company_Year
WHERE years IS NOT NULL
ORDER BY ranking ASC;

WITH Company_Year AS
(
SELECT company, YEAR(`date`) AS years, SUM(total_laid_off) AS sum_laid_off
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
),
Company_Year_Rank AS
(
SELECT *, 
DENSE_RANK() OVER(PARTITION BY years ORDER BY sum_laid_off DESC) AS ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE ranking <= 5;

SELECT *
FROM layoffs_staging2
ORDER BY company ASC;









