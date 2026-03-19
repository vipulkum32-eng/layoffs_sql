-- CREATE TABLE ... LIKE → structure copy karta hai
create table layoffs_1
like layoffs1;

-- INSERT INTO ... SELECT → data copy karta haiinsert layoffs1
 insert layoffs_1
 select * from 
 layoffs;
 
select * from 
layoffs_1;

 SET SQL_SAFE_UPDATES = 0;
 
-- ROW_NUMBER helps identify duplicates
 select *, 
		row_number() over (partition by company,industry,total_laid_off,`date`)
		from layoffs_1;

-- Created a new table and removed duplicates using ROW_NUMBER().

CREATE TABLE layoffs_duplicate (
  `company` TEXT,
  `location` TEXT,
  `industry` TEXT,
  `total_laid_off` INT DEFAULT NULL,
  `percentage_laid_off` TEXT,
  `date` TEXT,
  `stage` TEXT,
  `country` TEXT,
  `funds_raised_millions` INT DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB 
DEFAULT CHARSET=utf8mb4 
COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_duplicate
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY company, industry, total_laid_off, `date`, 
    stage, funds_raised_millions, country) AS row_num
FROM layoffs;


-- Selects all duplicate rows from layoffs_duplicate where row_num is greater than 1.
select * from 
	layoffs_duplicate
	where row_num >1;
    
 
delete  from 
		layoffs_duplicate
		where row_num >1;
        
-- Trim extra spaces         
update layoffs_duplicate
 set company = trim(company)  
 
-- error handling
update layoffs_duplicate
set country = 'united states'
where country like 'united states.'


select country
from layoffs_duplicate
where country like 'united states.'


-- date formate
UPDATE layoffs_duplicate
		SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');
        
alter table layoffs_duplicate
modify column `date` date;

select * from layoffs_duplicate

-- null handing
select *
	from layoffs_duplicate
	where industry is null
	or industry = '';
    
    
select *
	from layoffs_duplicate
    where company = 'airbnb';
    
    
select l1.industry, l2.industry 
from layoffs_duplicate l1
join layoffs_duplicate l2
	on l1.company = l2.company
where (l1.industry is null or l1.industry ='')
    and l2.industry is not null;
    
    update layoffs_duplicate l1
join layoffs_duplicate l2
	on l1.company = l2.company
set l1.industry = l2.industry
where (l1.industry is null or l1.industry ='')
    and l2.industry is not null;
   
   
   
   
   
   
update layoffs_duplicate
    set industry = null
    where industry = '';
    
update layoffs_duplicate l1
		join layoffs_duplicate l2
		on l1.company = l2.company
set l1.industry = l2.industry
		where l1.industry is null
		and l2.industry is not null;
   
select * from 
layoffs_duplicate
		where total_laid_off is null 
		and percentage_laid_off is null; 
        
delete from 
layoffs_duplicate
		where total_laid_off is null 
		and percentage_laid_off is null;
    
  -- drop row_num column

alter table layoffs_duplicate
drop column row_num;
    
select * from layoffs_duplicate;

select max(total_laid_off), max(percentage_laid_off)
from layoffs_duplicate;    
    
select company ,sum(total_laid_off)
		from layoffs_duplicate
	group by company
	order by 2 desc;    
    
    
select min(`date`) , max(`date`)
	from layoffs_duplicate;    
    
select industry ,sum(total_laid_off)
		from layoffs_duplicate
		group by industry
		order by 2 desc;   


select country ,sum(total_laid_off)
from layoffs_duplicate
group by country
order by 2 desc;
    
    
 select stage ,sum(total_laid_off)
from layoffs_duplicate
group by stage
order by 2 desc;   
    
    
select substring(`date`,1,7)as `month` ,sum(total_laid_off)
from layoffs_duplicate
where substring(`date`,1,7) is not null
group by `month`
order by 1 ;

    
 WITH rolling_total AS (
    SELECT 
        SUBSTRING(`date`, 1, 7) AS `month`,
        SUM(total_laid_off) AS total_off
    FROM layoffs_duplicate
    WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
    GROUP BY `month`  ) 
    
 SELECT 
    `month`,
    SUM(total_off) OVER (ORDER BY `month`) AS rolling_total
FROM rolling_total;   
    

WITH company_year (company, years, total_laid_off) AS (
    SELECT 
        company,
        YEAR(`date`),
        SUM(total_laid_off)
    FROM layoffs_duplicate
    GROUP BY company, YEAR(`date`)
),
company_year_rank AS (
    SELECT *,
        DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
    FROM company_year
    WHERE years IS NOT NULL
)

SELECT *
FROM company_year_rank
WHERE ranking <= 5;
SET SQL_SAFE_UPDATES = 0;

SELECT 
    *
FROM
    layoffs_duplicate;

-- percentage_laid_off,total_laid_off null values fill avg and min.

UPDATE layoffs_duplicate l1
JOIN (
    SELECT industry, AVG(total_laid_off) AS avg_total
    FROM layoffs_duplicate
    WHERE total_laid_off IS NOT NULL
    GROUP BY industry
) l2
ON l1.industry = l2.industry
SET l1.total_laid_off = l2.avg_total
WHERE l1.total_laid_off IS NULL;


UPDATE layoffs_duplicate l1
JOIN (
    SELECT industry, AVG(percentage_laid_off) AS avg_total
    FROM layoffs_duplicate
    WHERE percentage_laid_off IS NOT NULL
    GROUP BY industry
) l2
ON l1.industry = l2.industry
SET l1.percentage_laid_off = l2.avg_total
WHERE l1.percentage_laid_off IS NULL;


update layoffs_duplicate
set percentage_laid_off=substring(percentage_laid_off,1,4);



        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
 
 


















 





























