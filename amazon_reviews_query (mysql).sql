use amazon_reviews;

show tables;

# Find trending products based on the number of reviews within a 1-month interval
SELECT m.asin, m.title, COUNT(r.asin) AS review_count
FROM meta m
JOIN review r ON m.asin = r.asin
WHERE r.reviewTime BETWEEN '2018-09-01' AND '2018-09-30'
GROUP BY m.asin, m.title
ORDER BY review_count DESC; -- 9.578 sceond

# If we have latest data, use this query for current trending
SELECT m.asin, m.title, COUNT(r.asin) AS review_count
FROM meta m
JOIN review r ON m.asin = r.asin
WHERE r.reviewTime >= CURDATE() - INTERVAL 1 MONTH
GROUP BY m.asin, m.title
ORDER BY review_count DESC; -- 7.013 second


# Calculate the average overall review rating for products 
SELECT m.asin, m.title, m.price, AVG(r.overall) AS average_rating
FROM meta m
JOIN review r ON m.asin = r.asin
GROUP BY m.asin, m.title
ORDER BY average_rating DESC; -- 87.152 second


# Calculate the average overall review rating for products that have more than three reviews over a period of one year
SELECT m.asin, m.title, COUNT(r.asin) AS review_count, AVG(r.overall) AS average_rating
FROM meta m
JOIN review r ON m.asin = r.asin
WHERE r.reviewTime BETWEEN '2017-01-01' AND '2017-12-31'
GROUP BY m.asin, m.title
HAVING review_count > 3
ORDER BY average_rating DESC; -- 32.015 second


# Find out the product that have been viewed most often
SELECT asin, title, view_count
FROM 
	(SELECT av.asin, m.title, COUNT(av.asin) AS view_count, DENSE_RANK() OVER (ORDER BY COUNT(av.asin) DESC) AS view_rank
	FROM also_view av
JOIN 
	meta m ON av.asin = m.asin
	GROUP BY av.asin, m.title) RankedView
WHERE view_rank = 1; -- 130.360 sceond
# Seems like there is a restriction that one product can only appear at most 60 times in also_viewed from other product 
    

# Find out product that has been viewed often but had not been bought often, in here we use product appear more than 10 times in 'also_view' but less than 5 times in 'also_buy'.
SELECT viewed.asin, m.title, viewed.view_count, bought.buy_count AS buy_count
FROM
  (SELECT asin, COUNT(*) as view_count
   FROM also_view
   GROUP BY asin
   HAVING COUNT(*) >= 10) as viewed
LEFT JOIN 
  (SELECT asin, COUNT(*) as buy_count
   FROM also_buy
   GROUP BY asin
   HAVING COUNT(*) < 5) as bought
ON viewed.asin = bought.asin
JOIN meta m ON viewed.asin = m.asin
WHERE COALESCE(bought.buy_count, 0) < 5; -- 29.141 second for searching, 28.406 for fetching.


# Find out the product that appear more times in often_buy than often_view
SELECT viewed.asin, m.title, viewed.view_count, bought.buy_count AS buy_count
FROM
  (SELECT asin, COUNT(*) as view_count
   FROM also_view
   GROUP BY asin ) as viewed
LEFT JOIN 
  (SELECT asin, COUNT(*) as buy_count
   FROM also_buy
   GROUP BY asin ) as bought
ON viewed.asin = bought.asin
JOIN meta m ON viewed.asin = m.asin
WHERE bought.buy_count > viewed.view_count; -- 30.421 second for searching, 28.219 for fetching.


# Find out the top 10 brand that have highest average overall rate from review and receive more than 100 reviews
SELECT m.brand, AVG(r.overall) as average_rating, COUNT(r.asin) as total_reviews
FROM meta m
JOIN review r ON m.asin = r.asin
GROUP BY m.brand
HAVING COUNT(r.asin) > 100
ORDER BY average_rating DESC
LIMIT 10; -- 78.594 second

# Find out the popular high-reputation video game with a price of less than 30 dollars.
SELECT m.asin, m.title, m.price, AVG(r.overall) AS average_rating, COUNT(r.overall) AS number_of_reviews
FROM meta m
JOIN meta_category mc ON m.asin = mc.asin
JOIN category c ON mc.CID = c.CID
JOIN review r ON m.asin = r.asin
WHERE c.category like '%video game%' AND m.price < 30 
AND m.title not like '%controller%' AND m.title not like '%wireless%' -- remove some products that is not game but have game category. 
GROUP BY m.asin, m.title, m.price
HAVING COUNT(r.overall) > 200 
ORDER BY average_rating DESC; -- 98.078 second
# Because of the multi-value of category type in original data, there might still be some products that though not video games still contain this kind of category


# Recommend items for user based on "Also_buy", in here take 'AZZZMSZI9LKE6' as example
SELECT ab.also_buy AS recommended_asin, m.title, COUNT(*) AS recommended_level
FROM also_buy ab
JOIN review r ON ab.asin = r.asin
JOIN meta m ON ab.also_buy = m.asin
WHERE r.reviewerID = 'AZZZMSZI9LKE6'
GROUP BY ab.also_buy, m.title
ORDER BY recommended_level DESC
LIMIT 10; -- 7.391 second



# Recommend items for user based on "Also_view"
SELECT av.also_view AS recommended_asin, m.title, COUNT(*) AS recommended_level
FROM also_view av
JOIN review r ON av.asin = r.asin
JOIN meta m ON av.also_view = m.asin
WHERE r.reviewerID = 'AZZZMSZI9LKE6'
GROUP BY av.also_view, m.title
ORDER BY recommended_level DESC
LIMIT 10; -- 7.469 second



