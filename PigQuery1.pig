customers = LOAD '/user/cs585/project1/input/customers.csv'
    USING PigStorage(',') AS (customerID:int, name:chararray, age:int, gender:chararray, countryCode:int, salary:float);

transactions = LOAD '/user/cs585/project1/input/transactions.csv'
    USING PigStorage(',')
    AS (transactionID:int, customerID:int, transTotal:float, transNumItems:int, transDesc:chararray);

grouped_trans = GROUP transactions BY customerID;

trans_count = FOREACH grouped_trans GENERATE
    group AS customerID,
    COUNT(transactions) AS numTrans;

joined = JOIN trans_count BY customerID,
    customers BY customerID;

cust_counts = FOREACH joined GENERATE
    customers::name AS name,
    trans_count::numTrans AS numTrans;

all_group = GROUP cust_counts ALL;

min_value = FOREACH all_group GENERATE
    MIN(cust_counts.numTrans) AS minTransCount;

result = JOIN cust_counts BY numTrans,
    min_value BY minTransCount;

final = FOREACH result GENERATE
    cust_counts::name AS name,
    cust_counts::numTrans AS numTrans;

STORE final INTO '/user/cs585/project1/output/pigquery1';