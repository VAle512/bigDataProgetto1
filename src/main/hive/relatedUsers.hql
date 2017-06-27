CREATE TABLE IF NOT EXISTS relatedUsers
(id INT, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator INT,
helpfulnessDenominator INT, score INT, time BIGINT, summary STRING, text STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/home/amazon/' 
OVERWRITE INTO TABLE relatedUsers;

DROP VIEW IF EXISTS userPairs; 

CREATE VIEW userPairs as
SELECT relatedUsersTable1.userId as firstUser, relatedUsersTable2.userId as secondUser, COUNT(DISTINCT relatedUsersTable1.productId) as numCommonProducts,
 collect_set(relatedUsersTable1.productId) as commonProductsSet
FROM relatedUsers relatedUsersTable1
JOIN relatedUsers relatedUsersTable2 on relatedUsersTable1.productId = relatedUsersTable2.productId
WHERE relatedUsersTable1.score > 3 and relatedUsersTable2.score > 3 and relatedUsersTable1.userId > relatedUsersTable2.userId
GROUP BY relatedUsersTable1.userId,relatedUsersTable2.userId;

SELECT up.secondUser, up.firstUser, up.numCommonProducts ,up.commonProductsSet
FROM userPairs up
WHERE up.numCommonProducts > 2
ORDER BY up.secondUser,up.firstUser;