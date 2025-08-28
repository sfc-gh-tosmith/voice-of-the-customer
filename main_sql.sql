---- Voice of the Customer ----
-- Voice of the Customer is a Framework created by Snowflake’s Sales Engineer team. This solution leverages Snowflake’s Cortex Functions, including LLMs and AI SQL, to handle the summarization, categorization, translation, and sentiment analysis of large text objects such as call transcripts, chat histories, and feedback data. The process provides a comprehensive and detailed view of customer interactions, enabling better insights and decision-making.

-- How can this help your organization?
    -- Financial sector: Quickly detect emerging concerns about specific financial products or services, Identify customer confusion points
    -- Retail sector: Rapidly flag widespread product defect reports or stock availability issues.
    -- Hospitality: Monitor customer sentiment regarding service quality at different locations.
    -- Telecom: Identify common customer confusion points, Gauge customer sentiment and understanding towards service changes.

---- Table of Contents ----
-- 1. Setup & Topic Extraction (if necessary)
-- 2. Main query for translation, sentiment, and categorization
-- 3. Example incremental processing code (stream, task, stored procedure)


---- 1. Setup ----

-- For the main example query to work, each customer interaction record should have the full transcript in a single text body. The individuals/actors should be defined in the transcript. For example "Customer: Good morning Agent: Hello, how can I help you?". The transcript does not need to be in English because we handle translation.

-- 1a. Creating example database and example data tables --
CREATE OR REPLACE DATABASE VOICE_OF_CUSTOMER;

CREATE OR REPLACE SCHEMA VOICE_OF_CUSTOMER.DEMO;



CREATE or REPLACE file format csvformat
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  type = 'CSV';

CREATE or REPLACE stage call_transcripts_data_stage
  file_format = csvformat;

CREATE or REPLACE table CALL_TRANSCRIPTS ( 
  transcript varchar
);

-- Upload the CSV into the stage in the UI for the demo. This data may already be in a table somewhere as views.
-- Note that you can also use AI_TRANSCRIBE to transcribe audio files in a stage.
-- https://docs.snowflake.com/en/user-guide/snowflake-cortex/ai-audio

COPY into CALL_TRANSCRIPTS
  from @call_transcripts_data_stage;

SELECT * FROM CALL_TRANSCRIPTS;

-- 1b. Creating language detection UDF --
-- This python UDF will detect the language of the transcription
CREATE OR REPLACE FUNCTION check_language_udf(str_to_check VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9' -- Or your preferred supported Python version
PACKAGES = ('langdetect')
HANDLER = 'check_language'
AS
$$
from langdetect import detect

def check_language(str_to_check: str) -> str:
    try:
        # Ensure the input is treated as a string
        text = str(str_to_check)
        lang_short = detect(text)
        return lang_short
    except:
        # Return 'unknown' for empty or undetectable strings.
        return 'unknown'
$$;

-- These queries will dynamically get your primary_topics and secondary_topics dynamically out of the data.
-- If you want to manually create the table of categories and sub categories you can do so and skip to the main query.
-- This query translates the transcripts and uses AI to look over a large list of them that are concatenated. The AI_AGG function that is in PrPr right now is purpose built for this kind of query.  
WITH BaseTranscripts AS (
  SELECT
    TRANSCRIPT,
    voice_of_customer.DEMO.CHECK_LANGUAGE_UDF(TRANSCRIPT) AS original_language
  FROM voice_of_customer.DEMO.CALL_TRANSCRIPTS
  WHERE LENGTH(TRANSCRIPT) > 5
),
TranslatedTranscripts AS (
  SELECT
    TRANSCRIPT,
    original_language,
    CASE
      WHEN original_language = 'en' THEN TRANSCRIPT
      ELSE snowflake.cortex.complete( -- Could also use CORTEX.TRANSLATE(). Faster, but consumes more credits
        'mixtral-8x7b',
        [
          {
            'role': 'system',
            'content': 'Translate the transcript into English, maintaining the structure of the conversation.'
          },
          { 'role': 'user', 'content': transcript }
        ],
        {}
      ):choices[0]:messages :: VARCHAR
    END AS translated_transcript
  FROM BaseTranscripts
)

SELECT AI_AGG(
    translated_transcript,
    $$You are an expert at recognizing patterns in customer support transcripts. You will receive a set of customer support call transcripts. Your job is to analyze them and come up with all the different products metioned in the calls.Future transcripts will be categorized into the topics that you generate.
*NOTE* - Each category should be made up of a maximum of 5 words.
- DO NOT respond with any preamble. Only return 8 categories.$$ -- Note you can adjust the number of categories here
)
FROM TranslatedTranscripts;

--- Output, copied into array below
-- Mortgages
-- Auto Loans
-- Credit Cards
-- Debit Cards
-- Online Banking
-- Personal Loans
-- Home Equity
-- Account Access


-- This function will classify the calls for the primary topic.
CREATE OR REPLACE TABLE GET_TOPICS_SUBTOPICS AS (
WITH BaseTranscripts AS (
  SELECT
    TRANSCRIPT,
    voice_of_customer.DEMO.CHECK_LANGUAGE_UDF(TRANSCRIPT) AS original_language
  FROM voice_of_customer.DEMO.CALL_TRANSCRIPTS
  WHERE LENGTH(TRANSCRIPT) > 5
),
TranslatedTranscripts AS (
  SELECT
    TRANSCRIPT,
    original_language,
    CASE
      WHEN original_language = 'en' THEN TRANSCRIPT
      ELSE snowflake.cortex.complete( -- Could also use CORTEX.TRANSLATE(). Faster, but consumes more credits
        'mixtral-8x7b',
        [
          {
            'role': 'system',
            'content': 'Translate the transcript into English, maintaining the structure of the conversation.'
          },
          { 'role': 'user', 'content': transcript }
        ],
        {}
      ):choices[0]:messages :: VARCHAR
    END AS translated_transcript
  FROM BaseTranscripts
)
SELECT
transcript,
-- array copied here
AI_CLASSIFY(transcript, 
['Mortgages',
'Auto Loans',
'Credit Cards',
'Debit Cards',
'Online Banking',
'Personal Loans',
'Home Equity',
'Account Access',
'Other']):labels AS category_val,
REGEXP_REPLACE(category_val, '[^a-zA-Z]', '') as primary_category
FROM translatedtranscripts
);

-- Check output
SELECT * FROM GET_TOPICS_SUBTOPICS; 

-- This will get the stratified subcategories for each category. The data will be paritioned by primary category
-- Then each primary category will get it's individual list of sub categories.
CREATE OR REPLACE PROCEDURE EXTRACT_SUBCATEGORIES_BY_PRIMARY_CATEGORY(
    TABLE_NAME STRING,
    TRANSCRIPT_COLUMN STRING DEFAULT 'transcript',
    PRIMARY_CATEGORY_COLUMN STRING DEFAULT 'primary_category'
)
RETURNS TABLE (
    primary_category STRING,
    subcategory STRING,
    total_transcripts NUMBER,
    sample_transcript STRING
)
LANGUAGE SQL
AS
$$
DECLARE
    result_cursor CURSOR FOR
        SELECT 
            primary_category,
            subcategory,
            total_transcripts,
            sample_transcript
        FROM results_table;
    
    sql_statement STRING;
    
BEGIN
    -- Create a temporary table to store results
    CREATE OR REPLACE TEMPORARY TABLE results_table (
        primary_category STRING,
        subcategory STRING,
        total_transcripts NUMBER,
        sample_transcript STRING
    );
    
    -- Build dynamic SQL query with AI_AGG and FLATTEN to create individual rows
    sql_statement := 
        'INSERT INTO results_table ' ||
        'WITH ai_results AS ( ' ||
            'SELECT ' ||
                '"' || PRIMARY_CATEGORY_COLUMN || '" AS primary_category, ' ||
                'AI_AGG("' || TRANSCRIPT_COLUMN || '", ' ||
                '''Based on these customer service transcripts, identify and list the main subcategories or specific types of issues within this category. ' ||
                'Provide a comma-separated list of 3-7 specific subcategories that represent the most common themes or issue types. Also include an other category in the list' ||
                'Focus on actionable, specific subcategories rather than generic ones. ' ||
                'For example, for banking: "Payment Processing Issues, Account Access Problems, Interest Rate Inquiries, Fee Disputes". ' ||
                'Return only the comma-separated list without explanation.''' ||
                ') AS subcategories_list, ' ||
                'COUNT(*) AS total_transcripts, ' ||
                'ANY_VALUE("' || TRANSCRIPT_COLUMN || '") AS sample_transcript ' ||
            'FROM "' || TABLE_NAME || '" ' ||
            'WHERE "' || PRIMARY_CATEGORY_COLUMN || '" IS NOT NULL ' ||
            'GROUP BY "' || PRIMARY_CATEGORY_COLUMN || '" ' ||
        '), ' ||
        'flattened AS ( ' ||
            'SELECT ' ||
                'ar.primary_category, ' ||
                'TRIM(f.value::STRING) AS subcategory, ' ||
                'ar.total_transcripts, ' ||
                'ar.sample_transcript ' ||
            'FROM ai_results ar, ' ||
            'LATERAL FLATTEN(input => SPLIT(ar.subcategories_list, '','')) f ' ||
            'WHERE TRIM(f.value::STRING) != '''' ' ||
        ') ' ||
        'SELECT ' ||
            'primary_category, ' ||
            'subcategory, ' ||
            'total_transcripts, ' ||
            'sample_transcript ' ||
        'FROM flattened ' ||
        'ORDER BY total_transcripts DESC, primary_category, subcategory';
    
    -- Execute the dynamic SQL
    EXECUTE IMMEDIATE sql_statement;
    
    -- Return results using cursor
    OPEN result_cursor;
    RETURN TABLE(result_cursor);
    
END;
$$;

-- call sproc to get the list of sub topics
CALL EXTRACT_SUBCATEGORIES_BY_PRIMARY_CATEGORY('GET_TOPICS_SUBTOPICS', 'TRANSCRIPT', 'PRIMARY_CATEGORY');

-- The output will be a table of the primary_topics and the related sub_topic for each primary.
-- If you want you can manually create this table if you know your primary topics and sub topics you want to classify as.
CREATE OR REPLACE TABLE CUSTOMER_INTERACTION_TOPICS AS
    SELECT * FROM TABLE(RESULT_SCAN('01bea820-0e11-5c19-0000-41590b96c29a'));

SELECT * FROM CUSTOMER_INTERACTION_TOPICS;

---- 2. Main query for translation, sentiment, and categorization ----
CREATE OR REPLACE TABLE PROCESSED_CUSTOMER_INTERACTIONS AS
WITH BaseTranscripts AS (
  SELECT
    TRANSCRIPT,
    voice_of_customer.DEMO.CHECK_LANGUAGE_UDF(TRANSCRIPT) AS original_language
  FROM voice_of_customer.DEMO.CALL_TRANSCRIPTS
  WHERE LENGTH(TRANSCRIPT) > 5
),
TranslatedTranscripts AS (
  SELECT
    TRANSCRIPT,
    original_language,
    CASE
      WHEN original_language = 'en' THEN TRANSCRIPT
      ELSE snowflake.cortex.complete( -- Could also use CORTEX.TRANSLATE(). Faster, but consumes more credits
        'mixtral-8x7b',
        [
          {
            'role': 'system',
            'content': 'Translate the transcript into English, maintaining the structure of the conversation.'
          },
          { 'role': 'user', 'content': transcript }
        ],
        {}
      ):choices[0]:messages :: VARCHAR
    END AS translated_transcript
  FROM BaseTranscripts
),
TopicAnalysis AS (
  SELECT
    TRANSCRIPT,
    original_language,
    translated_transcript,
    SNOWFLAKE.CORTEX.SENTIMENT(translated_transcript) AS sentiment,
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
      translated_transcript,
      (
        SELECT
          ARRAY_AGG(DISTINCT primary_category) AS all_topics_array
        FROM
          CUSTOMER_INTERACTION_TOPICS
      ),
      {
        'task_description': 'Return a classification of the topic of the customer interaction identified in the transcript' -- This may not be necessary, shown to demonstrate the option
      }
    ):label::text AS primary_category_fin -- Parse primary topic and cast as string
  FROM TranslatedTranscripts
),
SubtopicAnalysis AS (
    SELECT
        TRANSCRIPT,
        original_language,
        translated_transcript,
        sentiment,
        primary_category_fin as primary_category,
        SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
            translated_transcript,
            (
                SELECT
                    ARRAY_AGG(DISTINCT subcategory) AS all_subtopics_array
                FROM
                    CUSTOMER_INTERACTION_TOPICS s
                WHERE
                    LOWER(primary_category_fin) = LOWER(primary_category) 
            ),
            {
                'task_description': 'Return a classification of the topic of the customer interaction identified in the transcript'
            }
        ):label::text AS secondary_category -- Parse secondary topic and cast as string
    FROM
        TopicAnalysis
)
SELECT
  *
FROM
  SubtopicAnalysis;

SELECT * FROM PROCESSED_CUSTOMER_INTERACTIONS;

-- Average sentiment by primary topic
SELECT ROUND(AVG(sentiment),2) as AVG_SENTIMENT_SCORE, primary_category from processed_customer_interactions
GROUP BY primary_category
ORDER BY AVG_SENTIMENT_SCORE DESC;

-- Average sentiment by both primary and secondary topic
SELECT Round(AVG(sentiment),2) as AVG_SENTIMENT_SCORE, category, secondary_category from processed_customer_interactions
GROUP BY primary_category, secondary_category
ORDER BY AVG_SENTIMENT_SCORE DESC;

-- Snowflake's AI capabilities allow you to easily identify trends in customer interactions. No need to set up additional systems, just use simple SQL calls.
-- Possible visualizations/dashboards:
    -- Overall sentiment over time. Gauge the overall health of customer satisfaction and assess the impact of company-wide initiatives on customer perception.
    -- Sentiment broken out by topic and/or subtopic over time. Pinpoint exactly which product features, service aspects, or customer journey points are driving dissatisfaction or delight
    -- Call volume per topic or per sentiment type (Positive, Negative, Neutral). Understanding the volume of discussions around specific topics helps optimize operational staffing, prioritize agent training, and identify high-impact areas for process improvements
    -- Average sentiment by language. Highlight a need for enhanced localization or improved communication tactics
    -- Top Primary topics by language. Line above



    

---- 3. Example incremental processing code (stream, task, stored procedure) ----

-- Create a stream on the table where customer interactions land
CREATE OR REPLACE STREAM voice_of_customer.public.CALL_TRANSCRIPTS_STREAM
ON TABLE voice_of_customer.public.CALL_TRANSCRIPTS
APPEND_ONLY = TRUE; -- Set to TRUE if you only care about new inserts.
                     -- Set to FALSE if you also need to track updates/deletes,
                     -- which would require more complex logic in the SP (e.g., MERGE).

-- Create a table where the processed interactions will be written
CREATE OR REPLACE TABLE voice_of_customer.public.PROCESSED_CALL_TRANSCRIPTS (
    SOURCE_TRANSCRIPT VARCHAR, -- Original transcript from the stream
    ORIGINAL_LANGUAGE VARCHAR,
    TRANSLATED_TRANSCRIPT VARCHAR,
    SENTIMENT NUMBER(3,2), -- Assuming sentiment is a score, adjust precision/scale as needed
    PRIMARY_CATEGORY VARCHAR,
    SECONDARY_CATEGORY VARCHAR,
    PROCESSING_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() -- Timestamp of when this record was processed
);

-- Create a stored procedure that takes any new records, performs the translation, sentiment, and topic categorizations, then writes them to the processed interaction table.
CREATE OR REPLACE PROCEDURE voice_of_customer.public.PROCESS_NEW_TRANSCRIPTS_SP()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
  -- Use a MERGE statement if you need to handle updates to transcripts
  -- For an append-only stream and target table, INSERT is sufficient.
  INSERT INTO voice_of_customer.public.PROCESSED_CALL_TRANSCRIPTS (
    SOURCE_TRANSCRIPT,
    ORIGINAL_LANGUAGE,
    TRANSLATED_TRANSCRIPT,
    SENTIMENT,
    PRIMARY_CATEGORY,
    SECONDARY_CATEGORY
    -- PROCESSING_TIMESTAMP will use its default value
  )
 WITH BaseTranscripts AS (
  SELECT
    TRANSCRIPT,
    voice_of_customer.DEMO.CHECK_LANGUAGE_UDF(TRANSCRIPT) AS original_language
  FROM voice_of_customer.DEMO.CALL_TRANSCRIPTS
  WHERE LENGTH(TRANSCRIPT) > 5
),
TranslatedTranscripts AS (
  SELECT
    TRANSCRIPT,
    original_language,
    CASE
      WHEN original_language = 'en' THEN TRANSCRIPT
      ELSE snowflake.cortex.complete( -- Could also use CORTEX.TRANSLATE(). Faster, but consumes more credits
        'mixtral-8x7b',
        [
          {
            'role': 'system',
            'content': 'Translate the transcript into English, maintaining the structure of the conversation.'
          },
          { 'role': 'user', 'content': transcript }
        ],
        {}
      ):choices[0]:messages :: VARCHAR
    END AS translated_transcript
  FROM BaseTranscripts
),
TopicAnalysis AS (
  SELECT
    TRANSCRIPT,
    original_language,
    translated_transcript,
    SNOWFLAKE.CORTEX.SENTIMENT(translated_transcript) AS sentiment,
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
      translated_transcript,
      (
        SELECT
          ARRAY_AGG(DISTINCT primary_category) AS all_topics_array
        FROM
          CUSTOMER_INTERACTION_TOPICS
      ),
      {
        'task_description': 'Return a classification of the topic of the customer interaction identified in the transcript' -- This may not be necessary, shown to demonstrate the option
      }
    ):label::text AS primary_category_fin -- Parse primary topic and cast as string
  FROM TranslatedTranscripts
),
SubtopicAnalysis AS (
    SELECT
        TRANSCRIPT,
        original_language,
        translated_transcript,
        sentiment,
        primary_category_fin as primary_category,
        SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
            translated_transcript,
            (
                SELECT
                    ARRAY_AGG(DISTINCT subcategory) AS all_subtopics_array
                FROM
                    CUSTOMER_INTERACTION_TOPICS s
                WHERE
                    LOWER(primary_category_fin) = LOWER(primary_category) 
            ),
            {
                'task_description': 'Return a classification of the topic of the customer interaction identified in the transcript'
            }
        ):label::text AS secondary_category -- Parse secondary topic and cast as string
    FROM
        TopicAnalysis
)
SELECT
  *
FROM
  SubtopicAnalysis;
  WHERE translated_transcript IS NOT NULL; -- Optional: ensure translation was successful

  RETURN 'Successfully processed new transcripts from stream.';
EXCEPTION
  WHEN OTHER THEN
    RETURN 'Error processing transcripts: ' || SQLERRM;
END;
$$;

-- Step 4: Create a task that reads from that stream every 8 hours
-- This task will execute the stored procedure.
CREATE OR REPLACE TASK voice_of_customer.public.PROCESS_NEW_TRANSCRIPTS_TASK
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 */8 * * * UTC' -- Runs every 8 hours (at 00:00, 08:00, 16:00 UTC)
  WHEN SYSTEM$STREAM_HAS_DATA('voice_of_customer.public.CALL_TRANSCRIPTS_STREAM') -- Only run if the stream has new data
AS
  CALL voice_of_customer.public.PROCESS_NEW_TRANSCRIPTS_SP();
