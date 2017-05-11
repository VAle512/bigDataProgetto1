CREATE TABLE IF NOT EXISTS top5products
(id INT, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator INT,
helpfulnessDenominator INT, score INT, time BIGINT, summary STRING, text STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '*****FILE CSV*****' 
OVERWRITE INTO TABLE top5products;

CREATE VIEW ordered_products AS 
SELECT ord_prod.date, ord_prod.productId, avg(ord_prod.score) AS average 
FROM(
SELECT from_unixtime(time, 'yyyyMM') AS date, productId, score 
FROM top5products) ord_prod 
GROUP BY ord_prod.productId, ord_prod.date
ORDER BY ord_prod.date, ord_prod.productId;

SELECT topProd.ym, topProd.productId, topProd.average 
FROM(
SELECT date, productId, average, row_number() over(PARTITION BY date ORDER BY average DESC) AS rank 
FROM ordered_products) topProd 
WHERE topProd.rank <= 5;