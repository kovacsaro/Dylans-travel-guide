-- create tables and put data
--CREATE TABLE subscribers (
event_date DATE,
event_time TIME,
event TEXT,
user_id bigint);

--COPY subscribers FROM '/home/aronkovacs/DILANS/data_set/subscribers.csv' DELIMITER ',';

--CREATE TABLE customers (
event_date DATE,
event_time TIME,
event TEXT,
user_id bigint,
price INT);

--COPY customers FROM '/home/aronkovacs/DILANS/data_set/customers.csv' DELIMITER ',';

--CREATE TABLE new_readers (
event_date DATE,
event_time TIME,
event TEXT,
country TEXT,
user_id bigint,
source TEXT,
topic TEXT);

--COPY new_readers FROM '/home/aronkovacs/DILANS/data_set/new_readers.csv' DELIMITER ',';

--CREATE TABLE returning_readers (
event_date DATE,
event_time TIME,
event TEXT,
country TEXT,
user_id bigint,
topic TEXT);

--COPY returning_readers FROM '/home/aronkovacs/DILANS/data_set/returning_readers.csv' DELIMITER ',';

--COMMIT;
-----------------------------------------------------------------------------------------------------------------------
--new readers by county
SELECT country,
       COUNT(*) as new_readers_by_country
FROM new_readers
GROUP BY country
order by new_readers_by_country desc;
--readers by source
SELECT source,
       COUNT(*) as new_readers_by_source
FROM new_readers
GROUP BY source
order by new_readers_by_source desc;
--new readers by topic
SELECT topic,
       COUNT(*) as new_readers_by_topic
FROM new_readers
GROUP BY topic
order by new_readers_by_topic desc;
--returned readers by country
SELECT country,
       COUNT(DISTINCT(user_id)) as returned_readers_by_country
FROM returning_readers
GROUP BY country
order by returned_readers_by_country desc;
--returned readers by topic
SELECT topic,
       COUNT(*) as returned_readers_by_topic
FROM returning_readers
GROUP BY topic
order by returned_readers_by_topic desc;
--subscribers by country
SELECT country, COUNT(*) as subscribers_by_country
FROM new_readers
JOIN subscribers
ON new_readers.user_id = subscribers.user_id
GROUP BY country
ORDER BY subscribers_by_country DESC;
--subscribers by source
SELECT source, COUNT(*) as subscribers_by_source
FROM new_readers
JOIN subscribers
ON new_readers.user_id = subscribers.user_id
GROUP BY source
ORDER BY subscribers_by_source DESC;
--revenue by source
SELECT new_readers.source, SUM (customers.price) as revenue
FROM new_readers
JOIN customers
ON new_readers.user_id = customers.user_id
GROUP BY new_readers.source
ORDER BY revenue DESC;
--revenue by country
SELECT new_readers.country, SUM (customers.price) as revenue
FROM new_readers
JOIN customers
ON new_readers.user_id = customers.user_id
GROUP BY new_readers.country
ORDER BY revenue DESC;
--customers by country
select new_readers.country, count(distinct(customers.user_id)) as customers_by_country 
from customers 
join new_readers
on customers.user_id = new_readers.user_id
group by new_readers.country
order by customers_by_country DESC;
-- customers by source
select new_readers.source, count(distinct(customers.user_id)) as customers_by_source 
from customers 
join new_readers
on customers.user_id = new_readers.user_id
group by new_readers.source
order by customers_by_source DESC;
-- daily customers
select event_date, count(*) as daily_customers
from customers
group by event_date
order by event_date;
--daily revenue
select event_date, sum(price) 
from customers
group by event_date
order by event_date;
--avarage daily revenue
select
(select sum(price)from customers)
/
(select count(distinct(event_date)) from customers);
-- daily revenue by ebook
select e_book.event_date, sum(price) as daily_revenue_by_e_book
from  
 (select event_date, price
  from customers
  where price = 8) as e_book
group by e_book.event_date
order by e_book.event_date;
--daily revenue by course
select course.event_date, sum(price) as daily_revenue_by_course
from  
  (select event_date, price
  from customers
  where price = 80) as course
group by course.event_date
order by course.event_date;
--daily revenue by ebook and course
SELECT event_date, 
       SUM(CASE WHEN price = 8 THEN price ELSE 0 END) as daily_revenue_by_e_book,
       SUM(CASE WHEN price = 80 THEN price ELSE 0 END) as daily_revenue_by_course
FROM customers
GROUP BY event_date
ORDER BY event_date;
--most popular topic by readers
SELECT topic, COUNT(user_id) as counted_topic  
FROM
(SELECT *
  FROM
   (SELECT user_id, topic 
    FROM new_readers) AS new
  UNION ALL
  SELECT *
  FROM 
    (SELECT user_id, topic 
    FROM returning_readers) AS returned) AS new_and_returned
GROUP BY topic
ORDER BY counted_topic DESC;
--returning/new readers ratio
select 
  ((select count(distinct (user_id)) 
  from returning_readers)::float
/
  (select count(user_id)
  from new_readers)::float) as returned_and_new_readers_ratio;
--subs/new readers ratio
select
  ((select count(user_id) 
  from subscribers)::float
/
  (select count(user_id)
  from new_readers)::float) as subs_and_new_readers_ratio;
--subs/returning ratio
select
  ((select count(user_id) 
  from subscribers)::float
/
  (select count(distinct (user_id)) 
  from returning_readers)::float) as subs_and_returned_ratio;
---
select * from new_readers limit 10;
select * from returning_readers limit 10;
select * from subscribers limit 10;
select * from customers limit 10;

--FUNNEL I.
select new_users.event_date, new_users.new, returned_users.returned, subs.subs, customers.customers
from
-- Funnel #1 - NEW READERS
  (select event_date, count(*) as new
  from new_readers 
  group by event_date
  order by event_date) as new_users
left join
--Funnel #2 - RETURNING READERS 
  (select new_readers.event_date, 
    count(distinct(returning_readers.user_id)) as returned
  from returning_readers
  join new_readers
  on new_readers.user_id = returning_readers.user_id 
  group by new_readers.event_date
  order by new_readers.event_date) as returned_users
on new_users.event_date = returned_users.event_date  
left join
--Funnel #3 - SUBSCRIBERS
  (select new_readers.event_date, 
    count(distinct(subscribers.user_id)) as subs
  from subscribers 
  join new_readers
  on new_readers.user_id = subscribers.user_id 
  group by new_readers.event_date
  order by new_readers.event_date) as subs
on new_users.event_date = subs.event_date
left join
--Funnel #4 - CUSTOMERS
  (select new_readers.event_date, 
    count(distinct(customers.user_id)) as customers
  from customers 
  join new_readers
  on new_readers.user_id = customers.user_id 
  group by new_readers.event_date
  order by new_readers.event_date) as customers
on new_users.event_date = customers.event_date;

--FUNNEL II. by country & source
select new_users.event_date, new_users.country, new_users.source,
       new_users.new, returned_users.returned, subs.subs, customers.customers
from
-- Funnel #1 - NEW READERS
  (select event_date, country, source, count(*) as new
  from new_readers 
  group by event_date, country, source
  order by event_date) as new_users
left join
--Funnel #2 - RETURNING READERS 
  (select new_readers.event_date, new_readers.country, new_readers.source, 
    count(distinct(returning_readers.user_id)) as returned
  from returning_readers
  join new_readers
  on new_readers.user_id = returning_readers.user_id 
  group by new_readers.event_date, new_readers.country, new_readers.source
  order by new_readers.event_date) as returned_users
on new_users.event_date = returned_users.event_date AND new_users.country = returned_users.country AND new_users.source = returned_users.source 
left join
--Funnel #3 - SUBSCRIBERS
  (select new_readers.event_date, new_readers.country, new_readers.source, 
    count(distinct(subscribers.user_id)) as subs
  from subscribers 
  join new_readers
  on new_readers.user_id = subscribers.user_id 
  group by new_readers.event_date, new_readers.country, new_readers.source
  order by new_readers.event_date) as subs
on new_users.event_date = subs.event_date AND new_users.country = subs.country AND new_users.source = subs.source
left join
--Funnel #4 - CUSTOMERS
  (select new_readers.event_date, new_readers.country, new_readers.source, 
    count(distinct(customers.user_id)) as customers
  from customers 
  join new_readers
  on new_readers.user_id = customers.user_id 
  group by new_readers.event_date, new_readers.country, new_readers.source
  order by new_readers.event_date) as customers
on new_users.event_date = customers.event_date AND new_users.country = customers.country AND new_users.source = customers.source;

-- most profitable segments
select * from new_readers limit 10;
select * from customers limit 10;

select customers_and_new.country, customers_and_new.source, sum(customers_and_new.price)as revenue
from
   (select * 
   from customers
   join new_readers
   on customers.user_id = new_readers.user_id) as customers_and_new
group by customers_and_new.country, customers_and_new.source;

-- daily readers
select event_date, count(distinct(user_id)) as daily_readers
from
  (select event_date, user_id, event, topic 
  from new_readers
  UNION ALL  
  select event_date, user_id, event, topic 
  from returning_readers) as big_table
group by event_date
order by event_date;
-- daily activity
select event_date, count(distinct(user_id)) as daily_activity
from
  (select event_date, user_id, event 
  from new_readers
  union all
  select event_date, user_id, event 
  from returning_readers
  union all
  select event_date, user_id, event 
  from subscribers
  union all
  select event_date, user_id, event 
  from customers) as big_table
group by event_date
order by event_date;

--subs / new readers ratio in country 5
SELECT
  (SELECT COUNT(DISTINCT subscribers.user_id)
  FROM subscribers
  JOIN new_readers
  ON subscribers.user_id = new_readers.user_id
  WHERE new_readers.country = 'country_5') ::float
/
  (SELECT COUNT(DISTINCT (user_id))
  FROM new_readers
  WHERE country = 'country_5') ::float;

--subs / new readers ratio in country 7
SELECT
  (SELECT COUNT(DISTINCT subscribers.user_id)
  FROM subscribers
  JOIN new_readers
  ON subscribers.user_id = new_readers.user_id
  WHERE new_readers.country = 'country_7') ::float
/
  (SELECT COUNT(DISTINCT (user_id))
  FROM new_readers
  WHERE country = 'country_7') ::float;

--subs / new readers ratio in country 2
SELECT
  (SELECT COUNT(DISTINCT subscribers.user_id)
  FROM subscribers
  JOIN new_readers
  ON subscribers.user_id = new_readers.user_id
  WHERE new_readers.country = 'country_2') ::float
/
  (SELECT COUNT(DISTINCT (user_id))
  FROM new_readers
  WHERE country = 'country_2') ::float;

