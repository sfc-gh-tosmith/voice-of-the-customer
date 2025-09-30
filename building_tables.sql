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
