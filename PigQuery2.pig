customers = LOAD '/user/cs585/project1/input/customers.csv'
    USING PigStorage(',') AS (customerID:int, name:chararray, age:int, gender:chararray, countryCode:int, salary:float);

grouped_country = GROUP customers BY countryCode;

country_code_count = FOREACH grouped_country GENERATE group AS
    countryCode, COUNT(customers) AS num_customers;

filtered_count = FILTER country_code_count BY
    num_customers > 5000 OR num_customers < 2000;

STORE filtered_count INTO '/user/cs585/project1/output/pigquery2';