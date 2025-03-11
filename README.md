# data_cleaning.
-- DATA CLEANING
SELECT*
FROM layoffs;
-- 1 REMOVE DUPLICATION
-- 2 STANDARDIZING DATA
-- 3 NULL VALUES OR BLANK VALUES
-- 4 REMOVE ANY COLUMS
#tạo bảng chuyển dữ liệu qua layoffs_staging bảng mới
CREATE TABLE layoffs_staging
like layoffs;
SELECT*
FROM layoffs_staging;
insert layoffs_staging
SELECT*
from layoffs;
SELECT*
from layoffs_staging;
# đẩy các cột đáng chú ý lên đầu bảng lệnh partition:lọc chung các đặc điểm gần giống nhau trong bốn yếu tố
SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company, industry ,total_laid_off,percentage_laid_off, 'date') AS row_num
from layoffs_staging;
# cho bảng vào một CTE :tăng khả năng truy vấn dữ liệun
With duplicate_cte AS
(SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company,location,
 industry ,total_laid_off,percentage_laid_off, 'date',stage
 ,country,funds_raised_millions) AS row_num
from layoffs_staging)
SELECT*
FROM duplicate_cte
where row_num > 1;
SELECT*
from layoffs_staging
WHERE company = 'ola';
SELECT*
from layoffs_staging
WHERE company ='casper';
#Xoá hàng trùng trong cơ sẳn bảng
With duplicate_cte AS
(SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company,location,
 industry ,total_laid_off,percentage_laid_off, 'date',stage
 ,country,funds_raised_millions) AS row_num
from layoffs_staging)
DELETE
FROM duplicate_cte
where row_num > 1;









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
#tạo lại cte khi đã truy vấn cũ rồi
SELECT*
from layoffs_staging2;

insert into layoffs_staging2
SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company,location,
 industry ,total_laid_off,percentage_laid_off, 'date',stage
 ,country,funds_raised_millions) AS row_num
 from layoffs_staging;
 
 SELECT*
from layoffs_staging2
where row_num >1;


 DELETE
from layoffs_staging2
where row_num >1;

SELECT*
from layoffs_staging2;
-- STANDARDIZING DATA:dọn sạch các cột có trong ex để dl thô --> gọn gàn hơn
SELECT distinct(trim(company))# cú pháp distinict(trim()): lọc kí tự đb trong tên công ty
FROM layoffs_staging2;
SELECT company,trim(company)
FROM layoffs_staging2;


UPDATE layoffs_staging2
SET company = trim(company);

SELECT distinct(industry) #tìm ngành công nghiệp
FROM layoffs_staging2
group by 1;
#tiep gộp các ngành giống nhau thành một ngành
SELECT * #tìm ngành công nghiệp
FROM layoffs_staging2
where industry like 'Crypto%';


UPDATE layoffs_staging2 #nhớ tắt safe ms update đc
set industry ='Crypto'
where industry like'Crypto%';
SET SQL_SAFE_UPDATES = 0;

UPDATE layoffs_staging2 #nhớ tắt safe ms update đc
set industry ='Crypto'
where industry like'Crypto%';

UPDATE layoffs_staging2
SET company = trim(company);
 SELECT distinct location
 from layoffs_staging2
 order by 1;
# nhớ các câu query caanf cuộn để xem có trughf nhau không nếu trùng thì phải dùng lệnh lọc 
SELECT distinct country
 from layoffs_staging2
 order by 1;

SELECT *
 from layoffs_staging2
 where country like 'United States'
 order by 1;

SELECT distinct country,trim( trailing'.'from country)#trim lopai bỏ các cột chỉ giũ lại cột trong trim()
 from layoffs_staging2 #trailing giúp bỏ các cái trùng nhau theo cú pháp '.' from
 order by 1;
 
 UPDATE layoffs_staging2
 SET country = trim( trailing'.'from country)
 WHERE country like 'United States%';



SELECT date
 from layoffs_staging2 ;
 
 SELECT date,
str_to_date( date ,'%m/%d/%y' )#chuỗi nàygiúp chuyenr dữ liueej như sắp xếp theo ý ta vd ngày thng nam sinh theo thứ tự
 from layoffs_staging2 ;
 
 SELECT date
 from layoffs_staging2 ;
UPDATE layoffs_staging2  
SET date = STR_TO_DATE(date, '%m/%d/%Y');

 alter table layoffs_staging2
 modify column date DATE;
 
 SELECT *
 from layoffs_staging2
 where total_laid_off is null
 AND percentage_laid_off is null;
 
  SELECT *#có where bỏ distinct
 from layoffs_staging2
 where industry is null
 or industry = ''; #nơi mà ngành công nghiệp vô giá trị hoặc bằng khoảng trắng
 
 SELECT *#có where bỏ distinct
 from layoffs_staging2
 where company ='Airbnb';
 
 SELECT *
 from layoffs_staging2 t1 #tạo thêm, cột tương đương cột 1 tham gia vào cột hai do trường hợp này có hai thông tin giống nhau nen phải chia cho đúng
 JOIN layoffs_staging2 t2
 on t1.company = t2.company
where (t1.industry is null or t1.industry ='')#truy vấn đủ câu hỏi vì hàng này gồm một câu null và một cái hàng trắng
and t2.industry is not null;
 
 SELECT t1.industry, t2.industry
 from layoffs_staging2 t1 #tạo thêm, cột tương đương cột 1 tham gia vào cột hai do trường hợp này có hai thông tin giống nhau nen phải chia cho đúng
 JOIN layoffs_staging2 t2
 on t1.company = t2.company
where (t1.industry is null or t1.industry ='')#truy vấn đủ câu hỏi vì hàng này gồm một câu null và một cái hàng trắng
and t2.industry is not null;
 
 
 update layoffs_staging2 t1
 join layoffs_staging2 t2
    on t1.company = t2.company
 set t1.industry = t2.industry
 where (t1.industry is null or t1.industry ='')#truy vấn đủ câu hỏi vì hàng này gồm một câu null và một cái hàng trắng
and t2.industry is not null;
 
 select *
 from layoffs_staging2 t1;
 update layoffs_staging2 
 set industry = null 
 where industry ='';
 
 update layoffs_staging2 t1
 join layoffs_staging2 t2
    on t1.company = t2.company
 set t1.industry = t2.industry
 where t1.industry is null
and t2.industry is not null;

SELECT *
FROM layoffs_staging2
where company like 'Bally%';

SELECT *
FROM layoffs_staging2;

SELECT *
 from layoffs_staging2
 where total_laid_off is null
 AND percentage_laid_off is null;
 

 
 
 DELEte #truy vấn tìm bảng lỗi null hoàn tất  cần xoá bảng null đó để bảng lương ra ms đúng
 
 from layoffs_staging2
 where total_laid_off is null
 AND percentage_laid_off is null;
 
 SELECT *
 from layoffs_staging2;
 
 ALTER TABLE layoffs_staging2
 DROP COLUMN row_column;
 #row_column khỏi bảng layoffs_staging2.
