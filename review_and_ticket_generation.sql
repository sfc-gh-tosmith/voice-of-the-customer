-- Customer support calls
CREATE OR REPLACE TABLE generation_table AS
SELECT column1 as category,
       ARRAY_CONSTRUCT('Very positive', 'very negative', 'somewhat positive', 'somewhat negative', 'neutral')[uniform(0, 4, RANDOM())] as tone,
       ARRAY_CONSTRUCT('English', 'German', 'French', 'Spanish', 'Portuguese')[uniform(0, 4, RANDOM())] as language
FROM (VALUES 
    ('"My account" page won\'t load'), 
    ('Sign in problem: passkey error'), 
    ('Sign in problem: duo MFA  error'), 
    ('Sign in problem: app crashes after sign in'), 
    ('Account summary never fully loads'), 
    ('Credit card statement "create pdf" button not working'))
CROSS JOIN (SELECT seq4() as n FROM TABLE(generator(rowcount=>200)))
ORDER BY RANDOM()
LIMIT 1000;

select * from generation_table;

create or replace table public.customer_support_transcripts_gpt5 as
select category, 
    tone, 
    language,
    AI_COMPLETE(
        model => 'openai-gpt-5-mini',
        prompt => prompt('I need you to help me write a customer support call transcript. The context is a financial services company (Bank Corp) with a mobile app. The transcript needs to be written so that it fits the following category {0}. The customer should also have the following tone: {1}. It should be written in the following language {2}. The transcript should be no more than 200 words.', category, tone, language)
    ) transcript_text
from generation_table
limit 1000;

select * from customer_support_transcripts_gpt5 limit 200;





-- Retail bank location reviews
CREATE OR REPLACE TABLE bank_review_generation_table AS
SELECT column1 as category,
       ARRAY_CONSTRUCT('Very positive', 'very negative', 'somewhat positive', 'somewhat negative', 'neutral')[uniform(0, 4, RANDOM())] as tone,
       ARRAY_CONSTRUCT('English', 'German', 'French', 'Spanish', 'Portuguese')[uniform(0, 4, RANDOM())] as language
FROM (VALUES 
    ('Wait time inside branch'), 
    ('Wait time in drive thru'), 
    ('Experience and professionalism of teller'), 
    ('Experience and professionalism banker/advisor'), 
    ('Conditions of facility'), 
    ('Experience accessing ecurity deposit box'))
CROSS JOIN (SELECT seq4() as n FROM TABLE(generator(rowcount=>200)))
ORDER BY RANDOM()
LIMIT 1000;

select * from bank_review_generation_table;

create or replace table public.bank_reviews_table_gpt5 as
select category, 
    tone, 
    language,
    AI_COMPLETE(
        model => 'openai-gpt-5-mini',
        prompt => prompt('I need you to help me write a customer review. The context is a financial services company (Bank Corp) with retail banking locations. The transcript needs to be written so that it fits the following category {0}. The customer should also have the following tone: {1}. It should be written in the following language {2}. The review should be no more than 100 words.', category, tone, language)
    ) transcript_text
from bank_review_generation_table
limit 1000;

select * from bank_reviews_table_gpt5 limit 200;