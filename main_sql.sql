---- Voice of the Customer ----
-- Voice of the Customer is a Framework created by Snowflake’s Solution Innovation Team (SIT). This solution leverages Snowflake’s Cortex Functions, including LLMs and AI SQL, to handle the summarization, categorization, translation, and sentiment analysis of large text objects such as call transcripts, chat histories, and feedback data. The process provides a comprehensive and detailed view of customer interactions, enabling better insights and decision-making.

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

CREATE or REPLACE file format csvformat
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  type = 'CSV';

CREATE or REPLACE stage call_transcripts_data_stage
  file_format = csvformat
  url = 's3://sfquickstarts/misc/call_transcripts/';

CREATE or REPLACE table CALL_TRANSCRIPTS ( 
  date_created date,
  language varchar(60),
  country varchar(60),
  product varchar(60),
  category varchar(60),
  damage_type varchar(90),
  transcript varchar
);

COPY into CALL_TRANSCRIPTS
  from @call_transcripts_data_stage;

-- 1b. Creating language detection UDF --
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


-- Here we are creating a table of topics and subtopics, like one we would have if we knew what our customers usually call about. 
-- If we don't know what customers usually call about or if we would like to use AI to determine the topics automatically, see code later on. 
CREATE or replace TABLE CUSTOMER_INTERACTION_TOPICS (
    topic VARCHAR(255),
    subtopic VARCHAR(255)
);

-- Populate the topic/subtopic table
INSERT INTO CUSTOMER_INTERACTION_TOPICS (topic, subtopic) VALUES
('Damaged helmet buckles', 'Buckle completely broken off'),
('Damaged helmet buckles', 'Buckle cracked and not latching'),
('Damaged helmet buckles', 'Buckle difficult to open or close'),
('Damaged helmet buckles', 'N/A'),
('Delivery issues', 'Package never arrived'),
('Delivery issues', 'Received wrong item'),
('Delivery issues', 'Package arrived late'),
('Delivery issues', 'N/A'),
('Scratched helmet surfaces', 'Deep scratches on visor'),
('Scratched helmet surfaces', 'Minor cosmetic scratches on shell'),
('Scratched helmet surfaces', 'Scratches from shipping'),
('Scratched helmet surfaces', 'N/A'),
('Product quality concerns', 'Stitching coming undone on jacket'),
('Product quality concerns', 'Helmet padding feels loose'),
('Product quality concerns', 'Color of product different than advertised'),
('Product quality concerns', 'N/A'),
('Order processing and confirmation', 'Never received order confirmation email'),
('Order processing and confirmation', 'Order details in confirmation are incorrect'),
('Order processing and confirmation', 'Unable to track order'),
('Order processing and confirmation', 'N/A'),
('Issues with zippers on DryProof670 jackets', 'Zipper jammed and won\'t move'),
('Issues with zippers on DryProof670 jackets', 'Zipper pull broke off'),
('Issues with zippers on DryProof670 jackets', 'Zipper teeth not aligning'),
('Issues with zippers on DryProof670 jackets', 'N/A'),
('Order delays and updates', 'No updates on delayed order'),
('Order delays and updates', 'Inconsistent delivery estimates'),
('Order delays and updates', 'Need more information on the reason for the delay'),
('Order delays and updates', 'N/A'),
('Replacement or refund requests', 'Requesting replacement for damaged item'),
('Replacement or refund requests', 'Seeking a full refund for unsatisfactory product'),
('Replacement or refund requests', 'Inquiring about the return process'),
('Replacement or refund requests', 'N/A'),
('Confirmation of order details', 'Want to confirm shipping address'),
('Confirmation of order details', 'Need to verify items included in the order'),
('Confirmation of order details', 'Checking the estimated delivery date'),
('Confirmation of order details', 'N/A'),
('Missing price tags', 'Price tag missing from helmet'),
('Missing price tags', 'Jacket arrived without any tags'),
('Missing price tags', 'Unable to determine the original price'),
('Missing price tags', 'N/A');

SELECT * FROM CUSTOMER_INTERACTION_TOPICS;



-- This query translates the transcripts and uses AI to look over a large list of them that are concatenated. The AI_AGG function that is in PrPr right now is purpose built for this kind of query.  
WITH BaseTranscripts AS (
  SELECT
    TRANSCRIPT,
    voice_of_customer.public.CHECK_LANGUAGE_UDF(TRANSCRIPT) AS original_language
  FROM voice_of_customer.public.CALL_TRANSCRIPTS
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
SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet',
    [
        {
            'role':'system',
            'content': 'You are an expert at recognizing patterns in customer support transcripts. You will receive an array of customer support call transcripts. Your job is to analyze the transcripts and come up with 8 topics that encompass what the transcripts are about. Future transcripts will be categorized into the topics that you generate. \n *NOTE* - Each category should be made up of maximum 5 words\n - DO NOT respond with any preamble. Only return 8 categories.'
        },
        {
            'role':'user',
            'content': (
                        select array_agg(*)::varchar from (
                        select translated_transcript from TranslatedTranscripts
                        SAMPLE(30)
                        )
                        )
        }
    ],{});

-- Run above query a couple of times to see what the broad patterns are. Then consolidate into a final list. 
    -- Try 1
    -- 1. Product Quality Issues
    -- 2. Damaged Merchandise Returns
    -- 3. Missing Price Tags
    -- 4. Shipping and Delivery Problems
    -- 5. Product Replacement Process
    -- 6. Color Fading Complaints
    -- 7. Customer Service Response
    -- 8. Order Verification Process
    -- Try 2
    -- 1. Defective Helmet Buckles
    -- 2. Broken Jacket Zippers
    -- 3. Faded Color Issues
    -- 4. Missing Price Tags
    -- 5. Product Exchange Process
    -- 6. Shipping and Delivery
    -- 7. Quality Control Problems
    -- 8. Customer Service Response


    

---- 2. Main query for translation, sentiment, and categorization ----
CREATE OR REPLACE TABLE PROCESSED_CUSTOMER_INTERACTIONS AS
WITH BaseTranscripts AS (
  SELECT
    TRANSCRIPT,
    voice_of_customer.public.CHECK_LANGUAGE_UDF(TRANSCRIPT) AS original_language
  FROM voice_of_customer.public.CALL_TRANSCRIPTS
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
          ARRAY_AGG(DISTINCT topic) AS all_topics_array
        FROM
          CUSTOMER_INTERACTION_TOPICS
      ),
      {
        'task_description': 'Return a classification of the topic of the customer interaction identified in the transcript' -- This may not be necessary, shown to demonstrate the option
      }
    ):label::text AS primary_topic -- Parse primary topic and cast as string
  FROM TranslatedTranscripts
),
SubtopicAnalysis AS (
    SELECT
        TRANSCRIPT,
        original_language,
        translated_transcript,
        sentiment,
        primary_topic,
        SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
            translated_transcript,
            (
                SELECT
                    ARRAY_AGG(DISTINCT subtopic) AS all_subtopics_array
                FROM
                    CUSTOMER_INTERACTION_TOPICS
                WHERE
                    LOWER(topic) = LOWER(primary_topic) 
            ),
            {
                'task_description': 'Return a classification of the topic of the customer interaction identified in the transcript'
            }
        ):label::text AS secondary_topic -- Parse secondary topic and cast as string
    FROM
        TopicAnalysis
)
SELECT
  *
FROM
  SubtopicAnalysis;

-- Average sentiment by primary topic
SELECT ROUND(AVG(sentiment),2) as AVG_SENTIMENT_SCORE, primary_topic from processed_customer_interactions
GROUP BY primary_topic
ORDER BY AVG_SENTIMENT_SCORE DESC;

-- Average sentiment by both primary and secondary topic
SELECT Round(AVG(sentiment),2) as AVG_SENTIMENT_SCORE, primary_topic, secondary_topic from processed_customer_interactions
GROUP BY primary_topic, secondary_topic
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
    PRIMARY_TOPIC VARCHAR,
    SECONDARY_TOPIC VARCHAR,
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
    PRIMARY_TOPIC,
    SECONDARY_TOPIC
    -- PROCESSING_TIMESTAMP will use its default value
  )
  WITH BaseTranscripts AS (
    SELECT
      stream_transcripts.TRANSCRIPT, -- Select from the stream
      voice_of_customer.public.CHECK_LANGUAGE_UDF(stream_transcripts.TRANSCRIPT) AS original_language
    FROM voice_of_customer.public.CALL_TRANSCRIPTS_STREAM stream_transcripts -- Read from the stream
    WHERE LENGTH(stream_transcripts.TRANSCRIPT) > 5
      AND stream_transcripts.METADATA$ACTION = 'INSERT' -- Process only new rows
  ),
  TranslatedTranscripts AS (
    SELECT
      TRANSCRIPT,
      original_language,
      CASE
        WHEN original_language = 'en' THEN TRANSCRIPT
        ELSE snowflake.cortex.complete(
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
            ARRAY_AGG(DISTINCT topic) AS all_topics_array
          FROM
            CUSTOMER_INTERACTION_TOPICS -- Make sure this table is accessible
        )
      ):label::text AS primary_topic
    FROM TranslatedTranscripts
  ),
  SubtopicAnalysis AS (
    SELECT
      TRANSCRIPT, -- This is the original transcript
      original_language,
      translated_transcript,
      sentiment,
      primary_topic,
      SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        translated_transcript,
        (
          SELECT
            ARRAY_AGG(DISTINCT subtopic) AS all_subtopics_array
          FROM
            CUSTOMER_INTERACTION_TOPICS -- Make sure this table is accessible
          WHERE
            LOWER(topic) = LOWER(primary_topic)
        )
      ):label::text AS secondary_topic
    FROM
      TopicAnalysis
  )
  SELECT
    TRANSCRIPT, -- This will be the SOURCE_TRANSCRIPT in the target table
    original_language,
    translated_transcript,
    sentiment,
    primary_topic,
    secondary_topic
  FROM
    SubtopicAnalysis
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
