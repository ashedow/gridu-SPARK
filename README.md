# Capstone Spark project

## Data

Steps to generate input data for your project:
1. Copy the `bigdata-input-generator` folder in the root directory of your exam repo
2. Install the required python packages and run the main script from the root directory of the exam repo 
   to generate the datasets (you can use `virtualenv` for this step):
```bash
pip install -r bigdata-input-generator/requirements.txt
python bigdata-input-generator/main.py
```
3. Add `capstone-dataset` (generated folder) to `.gitignore` in your exam repo

### Data structure
#### Mobile App clickstream projection

Schema:
* userId: String
* eventId: String
* eventTime: Timestamp
* eventType: String
* attributes: Map[String, String]


There could be events of the following types that form a user engagement session:
* app_open
* search_product
* view_product_details
* purchase 
* app_close

Events of app_open type may contain the attributes relevant to the marketing analysis:
* campaign_id
* channel_id

Events of purchase type contain purchase_id attribute.

Sample: mobile-app-clickstream_sample

#### Generated dataset: 

Purchases projection

* Schema:
   * purchaseId: String
   * purchaseTime: Timestamp
   * billingCost: Double
   * isConfirmed: Boolean

## Tasks

**General requirements**

The requirements that apply to all tasks:
* Use Spark version 2.4 or higher
* The logic should be covered with unit tests
* The output should be saved as PARQUET files.
* Will be a plus: README file in the project documenting the solution. 
* Will be a plus: Integrational tests that cover the main method.

### 1. Build Purchases Attribution Projection

The projection is dedicated to enabling a subsequent analysis of marketing campaigns and channels. 
The target schema:
* purchaseId: String
* purchaseTime: Timestamp
* billingCost: Double
* isConfirmed: Boolean 

// a session starts with app_open event and finishes with app_close 

* sessionId: String
* campaignId: String  // derived from app_open#attributes#campaign_id
* channelIid: String    // derived from app_open#attributes#channel_id

**Requirements for implementation of the projection building logic:**
1. Implement it by utilizing default Spark SQL capabilities.
2. Implement it by using a custom UDF.

### 2.  Calculate Marketing Campaigns And Channels Statistics 
Use the purchases-attribution projection to build aggregates that provide the following insights:
Top Campaigns: 
- What are the Top 10 marketing campaigns that bring the biggest revenue (based on billingCost of confirmed purchases)?


Channels engagement performance: 
- What is the most popular (i.e. Top) channel that drives the highest amount of unique sessions (engagements)  with the App in each campaign?

**Requirements for task #2:**
1. Should be implemented by using plain SQL on top of Spark DataFrame API
2. Will be a plus: an additional alternative implementation of the same tasks by using Spark DataFrame API only (without plain SQL)

## Solution

* create virtualenv
* clone
* cd to progect
* install requirements
* `export PYTHONPATH=${PWD} pwd`
* run like `python jobs/main.py`

### 1.1
Using standard DataFrame API implemented a function with custom creation of sessionId field based on event types. Cleared attributes field to be in the same format as the rest of the dataset. Retrieved needed columns from it. 

### 1.2

### 2.1
Using both plain SQL and DataFrame API got the names of top marketing campaigns hat bring the biggest revenue.


### 2.2
Using both plain SQL and DataFrame API got the names of the channels that drives the highest amount of unique sessions in each campaign.
