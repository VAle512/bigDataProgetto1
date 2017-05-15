CREATE TABLE IF NOT EXISTS topProductsPerUser
(id INT, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator INT,
helpfulnessDenominator INT, score INT, time BIGINT, summary STRING, text STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/home/amazon/'
OVERWRITE INTO TABLE topProductsPerUser;

DROP VIEW IF EXISTS ordered_products;

CREATE VIEW ordered_products AS
SELECT ord_prod.userId, ord_prod.productId, avg(ord_prod.score) AS average
FROM(
SELECT userId, productId, score
FROM topProductsPerUser) ord_prod
GROUP BY ord_prod.productId, ord_prod.userId
ORDER BY ord_prod.userId, ord_prod.productId;

SELECT topProd.userId, topProd.productId, topProd.average
FROM(
SELECT userId, productId, average, row_number() over(PARTITION BY userId ORDER BY average DESC) AS rank
FROM ordered_products) topProd
WHERE topProd.rank <= 10;