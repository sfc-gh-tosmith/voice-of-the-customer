-- TSmith 9/12/25. Adding attributes to the tables that will be needed for analyses later on.
select * from bank_reviews_table_gpt5 limit 200;
/* 
Bank Reviews will need:
- Date (daily, within last 3 months)
- Branch number (1 through 50)
- Region number ( 1 through 5)
- Customer identifier ( 1 through 700)
- Review Category (to be discovered by using AISQL)
- Extract sentiment about: cleanliness, promptness of service, overall, ability to accomplish what they needed (to be discovered by using AISQL)
- Sentiment number (to be discovered by using AISQL)

Customer support will need:
- Date (daily, within last 3 months)
- Customer identifier ( 1 through 700)
- App version (1.0, 1.1, 1.3)
- Platform (mobile, desktop, app)
- Problem Category (to be discovered by using AISQL)
- Extract sentiment about: Login, Account Summary, App speed, ability to find what they needed (discoverability), overall (to be discovered by using AISQL)
- Sentiment number (to be discovered by using AISQL)
*/

create or replace table bank_reviews_to_process as 
SELECT
  TRANSCRIPT_TEXT,
  DATEADD(
    'day',
    ABS(MOD(RANDOM(), DATEDIFF('day', DATEADD('month', -3, CURRENT_DATE()), CURRENT_DATE()) + 1)),
    DATEADD('month', -3, CURRENT_DATE())
  ) AS REVIEW_DATE,
  UNIFORM(1, 51, RANDOM())  AS BRANCH_NUMBER,
  UNIFORM(1, 6, RANDOM())   AS REGION_NUMBER,
  UNIFORM(1, 701, RANDOM()) AS CUSTOMER_ID
FROM VOICE_OF_CUSTOMER.PUBLIC.BANK_REVIEWS_TABLE_GPT5;

select * from bank_reviews_to_process;

create or replace table customer_support_tickets_to_process as
SELECT
  t.TRANSCRIPT_TEXT,
  DATEADD(
    'day',
    ABS(MOD(RANDOM(), DATEDIFF('day', DATEADD('month', -3, CURRENT_DATE()), CURRENT_DATE()) + 1)),
    DATEADD('month', -3, CURRENT_DATE())
  ) AS support_ticket_DATE,
  UNIFORM(1, 701, RANDOM()) AS CUSTOMER_ID,
  ARRAY_CONSTRUCT('1.0.1','1.1.0','1.2.1')[UNIFORM(0, 2, RANDOM())]::STRING AS APP_VERSION,
  ARRAY_CONSTRUCT('mobile','desktop','app')[UNIFORM(0, 2, RANDOM())]::STRING AS PLATFORM
FROM VOICE_OF_CUSTOMER.PUBLIC.CUSTOMER_SUPPORT_TRANSCRIPTS_GPT5 t;

select * from customer_support_tickets_to_process;

select  UNIFORM(1, 701, RANDOM()) AS CUSTOMER_ID;

-- Create customer lifetime value table
CREATE OR REPLACE TABLE customer_lifetime_value as
with allCustomers as (select customer_id from customer_support_tickets_to_process
union 
select customer_id from bank_reviews_to_process)
select customer_id, UNIFORM(1, 100001, RANDOM()) lifetime_value from allCustomers ;

select * from customer_lifetime_value;

UPDATE customer_lifetime_value 
SET lifetime_value = UNIFORM(90000, 140000, RANDOM()) 
WHERE customer_id IN (537, 381, 330, 593);

with allCustomers as (select customer_id, support_ticket_date from customer_support_tickets_to_process
union all 
select customer_id, review_date from bank_reviews_to_process)
select customer_id, count(*) 
from allCustomers
group by customer_id
order by count(*) desc;

