DROP TABLE IF EXISTS topproductsperuser;

CREATE TABLE IF NOT EXISTS topProductsPerUser
(id INT, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator INT,
helpfulnessDenominator INT, score INT, time BIGINT, summary STRING, text STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/home/amazon/'
OVERWRITE INTO TABLE topProductsPerUser;

DROP VIEW IF EXISTS ordered_products;

CREATE VIEW ordered_products AS
SELECT userId, productId, score
FROM topProductsPerUser
GROUP BY productId, userId, score
ORDER BY userId, productId;

SELECT topProd.userId, topProd.productId, topProd.score
FROM(
SELECT userId, productId, score, row_number() over(PARTITION BY userId ORDER BY score DESC) AS rank
FROM ordered_products) topProd
WHERE topProd.rank <= 10;