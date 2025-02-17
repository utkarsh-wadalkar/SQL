CREATE TABLE layoffs_stagging
LIKE layoffs;

SELECT *
FROM layoffs_stagging;

INSERT layoffs_stagging
SELECT * FROM layoffs;

-- removing duplicates

SELECT *
FROM layoffs_stagging;

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_stagging;

WITH duplicate_cte AS(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_stagging
)
SELECT * FROM duplicate_cte
WHERE row_num > 1;


CREATE TABLE `layoffs_duplicates` (
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

INSERT INTO layoffs_duplicates
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_stagging;

SELECT * FROM 
layoffs_stagging;

SELECT * FROM 
layoffs_duplicates;

DELETE FROM 
layoffs_duplicates
WHERE row_num >1;




-- Standardization of data

select trim(company)
from layoffs_duplicates;

update layoffs_duplicates
set company = trim(company);

select * from layoffs_duplicates;

alter table layoffs_duplicates
rename to layoffs_stage2;

select distinct (industry)
from layoffs_stage2;

select *
from layoffs_stage2
where industry like '%Crypto%';

update layoffs_stage2
set industry = 'Crypto'
where industry like '%Crypto%';



select distinct country
from layoffs_stage2
order by country;

select distinct country, trim(TRAILING '.' from country)
from layoffs_stage2
order by country;


update layoffs_stage2
set country = trim(TRAILING '.' from country)
where country like 'United States%';

select `date`, STR_TO_DATE(`date`, '%m/%d/%Y')
from layoffs_stage2;

update layoffs_stage2
set  `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

select * from layoffs_stage2 order by `date`;
 
 alter table layoffs_stage2
 modify `date` date; -- changed column data type to date 

-- handling nulls

select * from layoffs_stage2
where total_laid_off is NULL;

select * from layoffs_stage2
where total_laid_off is NULL AND percentage_laid_off is NULL;

select * from layoffs_stage2
where total_laid_off is NULL AND percentage_laid_off is NULL AND funds_raised_millions is NULL;
-- A lot of null values in 3 columns

select * from layoffs_stage2
where industry is NULL OR industry = '';

select * from layoffs_stage2
order by company;



-- correction in duplicates


SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num2
FROM layoffs_stage2;


WITH duplicatescte AS(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num2
FROM layoffs_stage2
)
SELECT * FROM duplicatescte
WHERE row_num > 1;

CREATE TABLE `layoffs_duplicates` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT,
  `row_num2` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select * from layoffs_duplicates;

DELETE FROM 
layoffs_duplicates
WHERE row_num2 >1;

select * from layoffs_duplicates
order by company;

select * from layoffs_stage2;

alter table layoffs_stage2 add row_num2 INT;


select *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num2
FROM layoffs_stage2;

select * from layoffs_stage2;

select *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num2
FROM layoffs_duplicates;

select * from layoffs_duplicates
order by company;

alter table layoffs_duplicates
drop column row_num;
alter table layoffs_duplicates
drop column row_num2;

alter table layoffs_duplicates
rename to layoffs_clean;

select * from layoffs_clean;

-- now duplicates are taken care of, my bad


alter table layoffs_clean
modify `date` date;

select * from layoffs_clean
where industry is NULL OR industry = ''
order by company;

select * from layoffs_clean
where company is null;

select * from layoffs_clean;
select *
from layoffs_clean t1
join layoffs_clean t2 on t1.company =t2.company 
where (t1.industry is null or t1.industry = '') and t2.industry is not null;



update layoffs_clean 
set industry = null
where industry = '';

update layoffs_clean t1
join layoffs_clean t2 on t1.company =t2.company
	SET t1.industry = t2.industry
WHERE (t1.industry is null or t1.industry = '') and t2.industry is not null;



select * from layoffs_clean
where total_laid_off is NULL AND percentage_laid_off is NULL;


select * from layoffs_clean;


CREATE TABLE `layoffs_nullval` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` date DEFAULT NULL,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_nullval
select * from layoffs_clean  
where total_laid_off is NULL AND percentage_laid_off is NULL;

select * from layoffs_nullval;

delete from layoffs_clean
where total_laid_off is NULL AND percentage_laid_off is NULL;


