/************************************************************************************************************************************************************************
****
****    Dropd and recreate the database and demo data
****
************************************************************************************************************************************************************************/

USE master
GO

IF DB_ID('DurableKeyDemo') IS NOT NULL
BEGIN
    ALTER DATABASE DurableKeyDemo SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DurableKeyDemo;
END
GO

CREATE DATABASE DurableKeyDemo;
GO

USE DurableKeyDemo;
GO

CREATE SCHEMA dim AUTHORIZATION dbo;
GO
CREATE SCHEMA fact AUTHORIZATION dbo;
GO

-- Make monday the first day in the week
SET DATEFIRST 1;



/************************************************************************************************************************************************************************
****
****    Create & populate a simple date dimension
****
************************************************************************************************************************************************************************/
CREATE TABLE dim.Calendar(
    CalendarSK INT NOT NULL,
    CalendarDate DATE NOT NULL,
    CalendarYear INT NOT NULL,
    MonthInYear INT NOT NULL,
    DayInYear INT NOT NULL,
    DayInMonth INT NOT NULL,
    DayInWeek INT NOT NULL,
    DayName VARCHAR(10) NOT NULL,
    IsWeekend CHAR(1) NOT NULL,
    PRIMARY KEY CLUSTERED (CalendarSK)
)
GO

WITH CTE_Nums
AS (
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNum
          ,CAST('20000101' AS DATE) AS StartDate
    FROM sys.objects AS o1
    CROSS JOIN sys.objects AS o2
   )
,CTE_Dates
AS (   
    SELECT DATEADD(DAY, RowNum, StartDate) AS CalendarDate
    FROM CTE_Nums
   )
INSERT INTO dim.Calendar (
    CalendarSK,
    CalendarDate,
    CalendarYear,
    MonthInYear,
    DayInYear,
    DayInMonth,
    DayInWeek,
    DayName,
    IsWeekend
)
SELECT CAST(FORMAT(CalendarDate, 'yyyyMMdd') AS INT) AS CalendarSK,
       CalendarDate,
       YEAR(CalendarDate) AS CalendarYear,
       MONTH(CalendarDate) AS MonthInYear,
       DATEPART(dy, CalendarDate) AS DayInYear,
       DATEPART(d, CalendarDate) AS DayInMonth,
       DATEPART(dw, CalendarDate) AS DayInWeek,
       DATENAME(dw, CalendarDate) AS DayName,
       CASE WHEN (DATEPART(dw, CalendarDate) + @@DATEFIRST) % 7 IN (0, 1) THEN 'Y' ELSE 'N' END AS IsWeekend
FROM CTE_Dates
GO




/************************************************************************************************************************************************************************
****
****    Create & populate a simple product SCD type 2 dimension
****
************************************************************************************************************************************************************************/


CREATE TABLE dim.Product (
    ProductSK INT IDENTITY NOT NULL,
    SourceSystem VARCHAR(5) NOT NULL,
    SourceId VARCHAR(12) NOT NULL,
    ProductName VARCHAR(50) NOT NULL,
    ProductDescription VARCHAR(100) NOT NULL,
    ProductCategory VARCHAR(30) NOT NULL,
    ProductPrice MONEY NOT NULL,
    ValidFromDate DATE NOT NULL DEFAULT (CAST(GETDATE() AS DATE)),
    ValidToDate DATE NOT NULL DEFAULT ('99991231'),
    IsCurrentRow BIT NOT NULL DEFAULT (1),
    MergeCheckSum AS CheckSum(ProductName, ProductDescription, ProductCategory, ProductPrice)
    PRIMARY KEY CLUSTERED (ProductSK)
)
GO
CREATE UNIQUE INDEX IX_dim_Product_#SourceSystem_#SourceId ON dim.Product(SourceSystem, SourceId)
	INCLUDE (ProductName, ProductDescription, ProductCategory, ProductPrice)
	WHERE (IsCurrentRow = 1) 
GO

INSERT INTO dim.Product(
    SourceSystem,
    SourceId,
    ProductName,
    ProductDescription,
    ProductCategory,
    ProductPrice,
    ValidFromDate,
    ValidToDate,
    IsCurrentRow
)
SELECT 'ERP',
       p.SourceId,
       p.ProductName,
       p.ProductDescription,
       P.ProductCategory,
       p.ProductPrice,
       p.ValidFromDate,
       p.ValidToDate,
       p.IsCurrentRow
FROM (
    VALUES 
    ('TBG-001', 'English Breakfast Tea - 40 Teabags',           'The best tea ever!',                       'Tea',              3.99,       '20180216', '20200731', 0),
    ('TBG-001', 'English Breakfast Tea - 40 Teabags',           'The best tea ever!',                       'Teabags',          4.49,       '20200801', '99991231', 1),
    ('LLT-012', 'English Breakfast Tea - 500g leaf',            'The best tea ever!',                       'Tea',              12.99,      '20180216', '20200630', 0),
    ('LLT-012', 'English Breakfast Tea - 500g loose leaf',      'The best tea ever!',                       'Tea',              12.99,      '20200701', '20200731', 0),
    ('LLT-012', 'English Breakfast Tea - 500g loose leaf',      'The best tea ever!',                       'Loose Leaf Tea',   13.99,      '20200801', '99991231', 1),
    ('LLT-013', 'Moroccan Mint Tea - 500g loose leaf',          'Fresh and minty',                          'Tea',              100.99,     '20180216', '20190427', 0),
    ('LLT-013', 'Moroccan Mint Tea - 500g leaf',                'Fresh and minty',                          'Tea',              10.99,      '20190428', '20200731', 0),
    ('LLT-013', 'Moroccan Mint Tea - 500g leaf',                'Fresh and minty',                          'Loose Leaf Tea',   11.99,      '20200801', '99991231', 1),
    ('TBG-022', 'Chai Tea - 40 Tea Bags',                       'Great balance of spices',                  'Tea',              4.99,       '20181205', '20200731', 0),
    ('TBG-022', 'Chai Tea - 40 Tea Bags',                       'Great balance of spices',                  'Teabags',          5.49,       '20200801', '99991231', 1),
    ('TBG-030', 'Teapot',                                       'You''ll need one of these',                'Accessories',      20.00,      '20190419', '99991231', 1),
    ('ACC-033', 'Blue Tea cozy',                                'Keeps your tea warm, or your bonce!',      'Accessories',      20.00,      '20190419', '99991231', 1),
    ('ACC-039', 'Red Tea cozy',                                 'Keeps your tea warm, or your bonce!',      'Accessories',      2.00,       '20190419', '99991231', 1),
    ('ACC-041', 'Yellow Tea cozy',                              'Keeps your tea warm, or your bonce!',      'Accessories',      20.00,      '20190419', '99991231', 1)
    ) AS p (SourceId, ProductName, ProductDescription, ProductCategory, ProductPrice, ValidFromDate, ValidToDate, IsCurrentRow)
ORDER by ValidFromDate, SourceId, ProductName
GO

/************************************************************************************************************************************************************************
****
****    Convert the product dimension to a SCD type 7
****
****    We convert the existing one as adding the demo data was simpler like that :)
****
************************************************************************************************************************************************************************/

CREATE SEQUENCE dim.ProductDK AS INT
    START WITH 1 INCREMENT BY 1
GO

ALTER TABLE dim.Product ADD ProductDK INT NULL DEFAULT (NEXT VALUE FOR dim.ProductDK);
GO

DROP TABLE IF EXISTS #productDK;

SELECT c.SourceSystem,
       c.SourceId,
       NEXT VALUE FOR dim.ProductDK AS ProductDK
INTO #productDK
FROM dim.Product AS c
GROUP BY SourceSystem,
         SourceId;

UPDATE c
SET ProductDK = dk.ProductDK
FROM dim.Product AS c
INNER JOIN #productDK AS dk
  ON dk.SourceSystem = c.SourceSystem
     AND dk.SourceId = c.SourceId;
GO

ALTER TABLE dim.Product ALTER COLUMN ProductDK INT NOT NULL;
GO

CREATE UNIQUE INDEX UQ_dim_Product_#ProductDK ON dim.Product(ProductDK) 
	INCLUDE (ProductName, ProductDescription, ProductCategory, ProductPrice)
	WHERE (IsCurrentRow = 1) 
GO

CREATE VIEW dim.ProductCurrent
AS
SELECT ProductDK,
       ProductName AS CurrentProductName,
       ProductDescription AS CurrentProductDescription,
       ProductCategory AS CurrentProductCategory
FROM dim.Product
WHERE IsCurrentRow = 1;
GO

/************************************************************************************************************************************************************************
****
****    Create & populate a simple customer SCD2 dimension
****
************************************************************************************************************************************************************************/
CREATE TABLE dim.Customer (
    CustomerSK INT IDENTITY NOT NULL,
    SourceSystem VARCHAR(10) NOT NULL,
    SourceId INT NOT NULL,
    CustomerName VARCHAR(100) NOT NULL,
    CustomerEmail VARCHAR(100) NOT NULL,
    CustomerCategory VARCHAR(30) NOT NULL,
    ValidFromDate DATE NOT NULL DEFAULT (CAST(GETDATE() AS DATE)),
    ValidToDate DATE NOT NULL DEFAULT ('99991231'),
    IsCurrentRow BIT NOT NULL DEFAULT (1),
    MergeCheckSum AS CheckSum(CustomerName, CustomerEmail, CustomerCategory)
    PRIMARY KEY CLUSTERED (CustomerSK)
)
GO
CREATE UNIQUE INDEX IX_dim_Customer_#SourceSystem_#SourceId ON dim.Customer(SourceSystem, SourceId) 
	INCLUDE	(CustomerName, CustomerEmail, CustomerCategory)
	WHERE (IsCurrentRow = 1)
GO

INSERT INTO dim.Customer(
    SourceSystem,
    SourceId,
    CustomerName,
    CustomerEmail,
    CustomerCategory,
    ValidFromDate,
    ValidToDate,
    IsCurrentRow
)
SELECT c.SourceSystem,
       c.SourceId,
       c.CustomerName,
       c.CustomerEmail,
       c.CustomerCategory,
       c.ValidFromDate,
       c.ValidToDate,
       c.IsCurrentRow
FROM (
    VALUES 
    ('Website', 1, 'Niall Langley',  'niall.langley@gmail.com',            'New Customer',       '20190419', '20190703', 0),
    ('Website', 1, 'Niall Langley',  'niall.langley@gmail.com',            'Regular Customer',   '20190704', '20200430', 0),
    ('Website', 1, 'Niall Langley',  'niall.langley@gmail.com',            'Super Customer',     '20200501', '20200831', 0),
    ('Website', 1, 'Niall Langley',  'niall.langley@gmail.com',            'Advocate',           '20200901', '99991231', 1),
    ('Website', 2, 'Alfred',         'alfred@batman.com',                  'Flavour Dabbler',    '20190419', '99991231', 1),
    ('Trade',   1, 'The tea shop',   'acounts@theteashop.co.uk',           'Trade - Regular',    '20180218', '20181111', 0),
    ('Trade',   1, 'The tea shop',   'acounts@theteashop.co.uk',           'Trade - Big Fish',   '20181112', '99991231', 1),
    ('Website', 3, 'Arthur Dent',    'arthur.dent@somewhereoutthere.com',  'Irregular Customer', '20190419', '99991231', 1),
    ('Website', 4, 'The Queen',      'lizzy@gov.uk',                       'VIP',                '20200901', '99991231', 1)
    ) AS c (SourceSystem, SourceId, CustomerName, CustomerEmail, CustomerCategory, ValidFromDate, ValidToDate, IsCurrentRow)
ORDER BY ValidFromDate, SourceSystem, SourceId
GO



/************************************************************************************************************************************************************************
****
****    Convert the product dimension to a SCD type 7
****
****    We convert the existing one as adding the demo data was simpler like that :)
****
************************************************************************************************************************************************************************/

CREATE SEQUENCE dim.CustomerDK AS INT
    START WITH 1 INCREMENT BY 1
GO

ALTER TABLE dim.Customer ADD CustomerDK INT NULL DEFAULT (NEXT VALUE FOR dim.CustomerDK);
GO

DROP TABLE IF EXISTS #customerDK;

SELECT c.SourceSystem,
       c.SourceId,
       NEXT VALUE FOR dim.CustomerDK AS CustomerDK
INTO #customerDK
FROM dim.Customer AS c
GROUP BY SourceSystem,
         SourceId;

UPDATE c
SET CustomerDK = dk.CustomerDK
FROM dim.Customer AS c
INNER JOIN #customerDK AS dk
  ON dk.SourceSystem = c.SourceSystem
     AND dk.SourceId = c.SourceId;

ALTER TABLE dim.Customer ALTER COLUMN CustomerDK INT NOT NULL;
GO

CREATE UNIQUE INDEX UQ_dim_Customer_#CustomerDK ON dim.Customer(CustomerDK)
	INCLUDE	(CustomerName, CustomerEmail, CustomerCategory)
	WHERE (IsCurrentRow = 1)
GO

CREATE VIEW dim.CustomerCurrent
AS
SELECT CustomerDK,
       CustomerName AS CurrentCustomerName,
       CustomerEmail AS CurrentCustomerEmail,
       CustomerCategory AS CurrentCustomerCategory
FROM dim.Customer
WHERE IsCurrentRow = 1;
GO





/************************************************************************************************************************************************************************
****
****    Create & populate a simple sales fact table
****
************************************************************************************************************************************************************************/
CREATE TABLE fact.Sales (
    CustomerSK INT NOT NULL,
    CustomerDK INT NOT NULL,
    ProductSK INT NOT NULL,
    ProductDK INT NOT NULL,
    OrderDateSK INT NOT NULL,
    ShipDateSK INT NOT NULL,
    OrderId INT NOT NULL,
    OrderLineId INT NOT NULL,
    Quantity INT NOT NULL,
    PricePaidPerUnit MONEY NOT NULL
    PRIMARY KEY CLUSTERED (CustomerSK, ProductSK, OrderDateSK, ShipDateSK)
)
GO



-- We randomly generate orders from a seed set, before expanding that out to cover a bunch of date ranges...
;WITH CTE_Seed
AS (
    -- Start with seed values, what a customer ordered, a range for quantity, and a percentage chance to exclude the order item
    SELECT *
    FROM (VALUES
            ('Trade', '1', 'ERP', 'LLT-012', 10, 20, 7, 10),
            ('Trade', '1', 'ERP', 'LLT-013', 1, 3, 7, 60),
            ('Trade', '1', 'ERP', 'TBG-022', 1, 2, 7, 80),
            ('Website', '1', 'ERP', 'LLT-012', 1, 1, 45, 5),
            ('Website', '1', 'ERP', 'LLT-013', 1, 1, 400, 50),
            ('Website', '1', 'ERP', 'TBG-022', 1, 1, 200, 75),
            ('Website', '3', 'ERP', 'TBG-001', 1, 1, 60, 80),
            ('Website', '4', 'ERP', 'TBG-001', 1, 1, 60, 90),
            ('Website', '4', 'ERP', 'LLT-013', 1, 1, 60, 90),
            ('Website', '4', 'ERP', 'TBG-022', 1, 1, 60, 90)
     )AS s (CustomerSourceSystem, CustomerSourceId, ProductSourceSystem, ProductSourceId, QuantityMin, QuantityMax, OrderFrequencyDays, PercentageChanceExcluded)
   )
,CTE_Random_Seed
AS (
    -- Now add the randomness in, so we get a nicer looking dataset. We join the dates to add a order on the first day of the week
    SELECT d.CalendarDate,
           s.CustomerSourceSystem,
           s.CustomerSourceId,
           s.ProductSourceSystem,
           s.ProductSourceId,
           -- Calculate a random quantity between the min and max
           ABS(CHECKSUM(NEWID()) % (s.QuantityMax - s.QuantityMin + 1)) + s.QuantityMin AS Quantity,
           -- Calculate a random discount of either 0%, 10% or 20%. Uses a cube root to skew the distrubution towards less discount more often.
           (5 - CAST(POWER(ABS(CHECKSUM(NEWID()) % 189) + 28, 1.0/3) AS INT)) * 10 AS Discount,
           -- We want to randomly ignore some rows so that the orders are not all the same
           CASE WHEN ABS(CHECKSUM(NEWID()) % 100) > s.PercentageChanceExcluded THEN 1 ELSE 0 END AS IncludeRow,
           -- We don't want all orders on the same day of the week, so we add a random number of days between 0 and 6
           ABS(CHECKSUM(NEWID())) % 6 AS DateAdjustmentDays,
           -- We want orders to ship on different days to the order. Like the discount we skew the results
           (5 - CAST(POWER(ABS(CHECKSUM(NEWID()) % 189) + 28, 1.0/3) AS INT)) AS ShipDateAdjustmentDays
    FROM CTE_Seed AS s
    CROSS APPLY (
                 SELECT CalendarDate
                 FROM dim.Calendar
                 WHERE DayInYear % s.OrderFrequencyDays = 0
                     AND CalendarDate < DATEADD(DAY, -7, GETDATE())
                     AND CalendarDate >= (SELECT MIN(ValidFromDate) FROM dim.Customer)
                )AS d
   )
,CTE_Normalised
AS (
    SELECT CalendarDate,
           CustomerSourceSystem,
           CustomerSourceId,
           ProductSourceSystem,
           ProductSourceId,
           Quantity,
           -- Use the window functions to normalise the customer order level details like discount and order id accross all order items for that customer on the same day
           MIN(Discount) OVER (PARTITION BY CalendarDate, CustomerSourceSystem, CustomerSourceId) AS Discount,
           RANK() OVER (ORDER BY CalendarDate, CustomerSourceSystem, CustomerSourceId) AS OrderId,
           ROW_NUMBER() OVER (PARTITION BY CalendarDate, CustomerSourceSystem, CustomerSourceId ORDER BY (NEWID())) AS OrderLineId,
           FIRST_VALUE(DateAdjustmentDays) OVER (PARTITION BY CalendarDate, CustomerSourceSystem, CustomerSourceId ORDER BY (NEWID())) AS DateAdjustmentDays,
           MIN(ShipDateAdjustmentDays) OVER (PARTITION BY CalendarDate, CustomerSourceSystem, CustomerSourceId) AS ShipDateAdjustmentDays
    FROM CTE_Random_Seed
    WHERE IncludeRow = 1
        AND Quantity > 0
   )
,CTE_DateAdjusted
AS (
    SELECT DATEADD(DAY, n.DateAdjustmentDays, n.CalendarDate) AS OrderDate,
           DATEADD(DAY, n.DateAdjustmentDays + n.ShipDateAdjustmentDays, n.CalendarDate) AS ShipDate,
           n.CustomerSourceSystem,
           n.CustomerSourceId,
           n.ProductSourceSystem,
           n.ProductSourceId,
           n.Quantity,
           n.Discount,
           n.OrderId,
           n.OrderLineId
    FROM CTE_Normalised AS n
   )
INSERT INTO fact.Sales
(
    CustomerSK,
    CustomerDK,
    ProductSK,
    ProductDK,
    OrderDateSK,
    ShipDateSK,
    OrderId,
    OrderLineId,
    Quantity,
    PricePaidPerUnit
)
SELECT c.CustomerSK,
       c.CustomerDK,
       p.ProductSK,
       p.ProductDK,
       CAST(FORMAT(da.OrderDate, 'yyyyMMdd') AS INT) AS OrderDateSK,
       CAST(FORMAT(da.ShipDate, 'yyyyMMdd') AS INT) AS ShipDateSK,
       da.OrderId,
       da.OrderLineId,
       da.Quantity,
       ROUND(p.ProductPrice * ((100.0 - da.Discount)  / 100), 2) AS PricePaidPerUnit
FROM CTE_DateAdjusted AS da
INNER JOIN dim.Customer as c
  ON c.SourceSystem = da.CustomerSourceSystem
     AND c.SourceId = da.CustomerSourceId
     AND da.OrderDate BETWEEN c.ValidFromDate AND c.ValidToDate
INNER JOIN dim.Product as p
  ON p.SourceSystem = da.ProductSourceSystem
     AND p.SourceId = da.ProductSourceId
     AND da.OrderDate BETWEEN p.ValidFromDate AND p.ValidToDate
ORDER BY da.OrderId, da.OrderLineId;





