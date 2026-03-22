/* NYKAA MARKETING DATA CLEANING & TRANSFORMATION PROJECT
Author: Ja'Lisa Little
Tools: PostgreSQL 18
Dataset: https://www.kaggle.com/datasets/nalisha/nykaa-marketing-campaign-performance-dataset
Description: This script cleans raw marketing campaign data, handles multi-channel 
attribution using string unnesting, and creates aggregated views for Power BI reporting.
*/

-- SECTION 1: DATABASE SETUP & DATA IMPORT

CREATE Table raw_nykaa_campaigns (
    Campaign_ID VARCHAR(50) PRIMARY KEY,
    Campaign_Type VARCHAR(255),
    Target_Audience VARCHAR(100),
    Duration INTEGER,
    Channel_Used VARCHAR(100),
    Impressions INTEGER,
    Clicks INTEGER,
    Leads INTEGER,
    Conversions INTEGER,
    Revenue INTEGER,
    Acquisition DECIMAL(10,2),
    ROI DECIMAL(10,2),
    Campaign_Language Text,
    Engagement_Score DECIMAL(10,2),
    Customer_Segment TEXT,
    Campaign_Date DATE
);

--Set datestyle to ensure session respects CSV date format
Set DateStyle TO 'ISO, DMY'; 

--Import CSV
COPY raw_nykaa_campaigns(
    campaign_id, campaign_type, target_audience, duration, channel_used, 
    impressions, clicks, leads, conversions, revenue, acquisition, 
    roi, campaign_language, engagement_score, customer_segment, campaign_date
)

FROM 'C:\Users\Public\Nykaa Marketing Data\nykaa_campaign_data.csv' 
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', ESCAPE '"');



-- SECTION 2: THE ATTRIBUTION LOGIC FOR CHANNELS
-- This view splits comma-separated strings into individual channel rows 
-- and allocates revenue/costs proportionally to prevent double-counting.
CREATE VIEW nykaa_channel_breakdown AS

  WITH channel_data AS (
  SELECT *, 
    string_to_array(channel_used, ',') AS channel_array
  FROM raw_nykaa_campaigns
),
  
expanded_channel_data AS (
  SELECT *, 
    TRIM(unnest(channel_array)) AS Single_Channel, --Trim to get rid of any spaces, unnest the array
    array_length(channel_array, 1) AS Channel_Count
  FROM channel_data
)
  
--Final columns with channels allocated across impressions, clicks, leads, conversions, revenue, etc
  SELECT 
    campaign_id,
    campaign_date,
    campaign_type,
    target_audience,
    duration,
    Single_Channel AS Channel,

    --Added decimal for accurate division    
    ROUND((impressions::DECIMAL / Channel_Count), 2) AS Allocated_Impressions,
    ROUND((clicks::DECIMAL / Channel_Count), 2) AS Allocated_Clicks,
    ROUND((leads::DECIMAL / Channel_Count), 2) AS Allocated_Leads,
    ROUND((conversions::DECIMAL / Channel_Count), 2) AS Allocated_Conversions,
    ROUND((revenue::DECIMAL / Channel_Count), 2) AS Allocated_Revenue,
    ROUND((acquisition::DECIMAL / Channel_Count), 2) AS Allocated_Acquisition_Cost,

    --Add remaining columns
    roi,
    campaign_language,
    engagement_score,
    customer_segment

FROM expanded_channel_data;



-- SECTION 3: VIEWS FOR PBI REPORTING

--Create a view for grouping data by customer segment and language to see which demo is brings in the most revenue
CREATE VIEW nykaa_revenue_by_segment AS

  SELECT
    customer_segment,
    campaign_language,

  COUNT(DISTINCT campaign_id) AS total_campaigns,
  ROUND(SUM(allocated_revenue), 2) AS total_revenue_generated,
  --avg value of a conversion for this segement
  ROUND(SUM(allocated_revenue) / NULLIF(SUM(allocated_conversions), 0), 2) AS avg_revenue_per_conversion

  FROM nykaa_channel_breakdown
  GROUP BY customer_segment, campaign_language
  ORDER BY total_revenue_generated DESC;

--Create a view for time-series by month to analyze monthly trends
CREATE VIEW nykaa_monthly_trends AS

  SELECT
    DATE_TRUNC('month', campaign_date) AS campaign_month,
    campaign_type,
    ROUND(SUM(allocated_revenue), 2) AS monthly_revenue,
    ROUND(SUM(allocated_acquisition_cost), 2) AS monthly_cac

  FROM nykaa_channel_breakdown
  GROUP BY 1, 2
  ORDER By 1 DESC;
