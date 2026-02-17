customers = LOAD '/user/cs585/project1/input/customers.csv'
    USING PigStorage(',') AS (customerID:int, name:chararray, age:int, gender:chararray, countryCode:int, salary:float);

transactions = LOAD '/user/cs585/project1/input/transactions.csv'
    USING PigStorage(',')
    AS (transactionID:int, customerID:int, transTotal:float, transNumItems:int, transDesc:chararray);

joined = JOIN customers BY customerID,
    transactions BY customerID;

temp = FOREACH joined GENERATE
    (customers::age == 70 ? 60 :
     (int)(customers::age/10)*10) AS lower,
    customers::gender AS gender,
    transactions::transTotal AS transTotal;

bins = FOREACH temp GENERATE
    (lower == 60 ?
        CONCAT(
            CONCAT('[', (chararray)lower),
            CONCAT(',', CONCAT((chararray)(lower + 10), ']'))
        )
    :
        CONCAT(
            CONCAT('[', (chararray)lower),
            CONCAT(',', CONCAT((chararray)(lower + 10), ')'))
        )
    ) AS ageRange,
    gender,
    transTotal;


grouped = GROUP bins BY (ageRange, gender);

result = FOREACH grouped GENERATE
    group.ageRange AS AgeRange,
    group.gender AS Gender,
    MIN(bins.transTotal) AS MinTransTotal,
    MAX(bins.transTotal) AS MaxTransTotal,
    AVG(bins.transTotal) AS AvgTransTotal;

STORE result into '/user/cs585/project1/output/pigquery3';