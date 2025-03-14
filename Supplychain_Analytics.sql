create database supplychain_db;
create schema supplchain_schema;
CREATE OR REPLACE TABLE SupplyChainDataCopy (
    Type STRING,
    Days_for_shipping_real INT,
    Days_for_shipment_scheduled INT,
    Benefit_per_order FLOAT,
    Sales_per_customer FLOAT,
    Delivery_Status STRING,
    Late_delivery_risk INT,
    Category_Id INT,
    Category_Name STRING,
    Customer_City STRING,
    Customer_Country STRING,
    Customer_Fname STRING,
    Customer_Id INT,
    Customer_Lname STRING,
    Customer_Segment STRING,
    Customer_State STRING,
    Customer_Street STRING,
    Customer_Zipcode STRING,
    Department_Id INT,
    Department_Name STRING,
    Latitude FLOAT,
    Longitude FLOAT,
    Market STRING,
    Order_City STRING,
    Order_Country STRING,
    Order_Customer_Id INT,
    Order_Date STRING,
    Order_Id INT,
    Order_Item_Cardprod_Id INT,
    Order_Item_Discount FLOAT,
    Order_Item_Discount_Rate FLOAT,
    Order_Item_Id INT,
    Order_Item_Product_Price FLOAT,
    Order_Item_Profit_Ratio FLOAT,
    Order_Item_Quantity INT,
    Sales FLOAT,
    Order_Item_Total FLOAT,
    Order_Profit_Per_Order FLOAT,
    Order_Region STRING,
    Order_State STRING,
    Order_Status STRING,
    Order_Zipcode STRING,
    Product_Card_Id INT,
    Product_Category_Id INT,
    Product_Name STRING,
    Product_Price FLOAT,
    Product_Status INT,
    Shipping_Date STRING,
    Shipping_Mode STRING
);






CREATE STORAGE INTEGRATION my_s4_integration 
TYPE = EXTERNAL_STAGE 
STORAGE_PROVIDER = 'S3' 
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::872515296252:role/supplychainRole' 
ENABLED = TRUE 
STORAGE_ALLOWED_LOCATIONS = ('s3://supplychaindata1/');

DESC INTEGRATION my_s4_integration;


CREATE OR REPLACE STAGE my_s4_stage
STORAGE_INTEGRATION = my_s4_integration
URL = 's3://supplychaindata1/';


truncate table supplychaindata;

select * from supplychaindata;


alter table supplychaindata  drop column CUSTOMER_PASSWORD;

drop table supplychaindatacopy;

CREATE OR REPLACE FILE FORMAT my_csv_format_1
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  FIELD_DELIMITER = ',';  -- Adjust delimiter if necessary (e.g., for CSV)


truncate table supplychaindatacopy;




COPY INTO SUPPLYCHAINDATACOPY
FROM @my_s4_stage/Cleaned_DataCoSupplyChainDataset.csv
FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_DELIMITER = ',', ENCODING = 'utf-8')
ON_ERROR = CONTINUE;







select * from supplychaindatacopy;

delete from supplychaindatacopy 
where Type is null;


UPDATE "SUPPLYCHAINDATACOPY"
SET "ORDER_ZIPCODE" = COALESCE("ORDER_ZIPCODE", 0)
WHERE "ORDER_ZIPCODE" IS NULL;


select * from supplychaindatacopy;

SHOW COLUMNS IN TABLE "SUPPLYCHAINDATACOPY";


UPDATE SUPPLYCHAINDATACOPY
SET ORDER_DATE_COLUMN = CASE
    WHEN ORDER_DATE LIKE '%/%' THEN TO_TIMESTAMP(ORDER_DATE, 'MM/DD/YYYY HH24:MI')
    ELSE NULL
END
WHERE ORDER_DATE IS NOT NULL;

alter table supplychaindatacopy modify column ORDER_DATE TIMESTAMP;

alter table supplychaindatacopy add column order_date_Column timestamp;

select * from supplychaindatacopy;

UPDATE supplychaindatacopy
SET order_Date_column = order_Date;



ALTER  table  SUPPLYCHAINDATACOPY Add COLUMN shipping_date_column timestamp; 

UPDATE SUPPLYCHAINDATACOPY
SET SHIPPING_DATE_column = CASE
    WHEN  shipping_Date  LIKE '%/%' THEN TO_TIMESTAMP(shipping_Date, 'MM/DD/YYYY HH24:MI')
    WHEN Shipping_Date like   '%-%' then to_timestamp(shipping_Date,'DD-MM-YYYY HH24:MI')
    ELSE NULL
END
WHERE shipping_Date  IS NOT NULL;

select  *
from supplychaindatacopy;

alter table supplychaindatacopy drop column shipping_Date;
alter table supplychaindatacopy drop column order_Date;

alter table supplychaindatacopy add column Order_Date_Year int ;

update supplychaindatacopy 
set order_Date_year = extract(year from order_Date_column);
select order_Date_year from supplychaindatacopy;
alter table supplychaindatacopy add column order_Date_month int;

update   supplychaindatacopy 
set order_Date_month = extract(month from order_Date_column);

select order_Date_month from supplychaindatacopy;

select * from supplychaindatacopy;

alter table supplychaindatacopy add column Customer_full_name varchar(100);

update supplychaindatacopy
set Customer_full_name = concat(customer_fname, ' ',customer_lname) ;

select customer_full_name from supplychaindatacopy;


select * from supplychaindatacopy;


