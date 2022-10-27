# Capstone project

In the final task of this course, you will need to figure efficiency of the marketing campaigns and channels.

**Task#1** Build Purchases Attribution Projection
Requirements for implementation of the projection building logic:
- Implement it by utilizing default Spark SQL capabilities.
- Implement it by using a custom Aggregator or UDAF.

**Task#2** Calculate Marketing Campaigns And Channels Statistics
- Top Campaigns
- Channels engagement performance

**Task#3** Organize data warehouse and calculate metrics for time period.
-Convert input dataset to parquet
-Calculate metrics from Task #2 for different time periods


## Data structure
### Mobile App clickstream projection

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

## Generated dataset: 

[Data Source](https://github.com/gridu/INTRO_SPARK-SCALA_FOR_STUDENTS)
Steps to generate input data for your project:

* Copy the `bigdata-input-generator` folder in the root directory of your exam repo
* Install the required python packages and run the main script from the root directory of the exam repo to generate the datasets (you can use virtualenv for this step):
```
pip install -r bigdata-input-generator/requirements.txt
python bigdata-input-generator/main.py
```
* Add `capstone-dataset` (generated folder) to `.gitignore` in your exam repo

Purchases projection

* Schema:
   * purchaseId: String
   * purchaseTime: Timestamp
   * billingCost: Double
   * isConfirmed: Boolean






## Solution
sbt project compiled with Scala 3

### Usage

This is a normal sbt project. You can compile code with `sbt compile`, run it with `sbt run`, and `sbt console` will start a Scala 3 REPL.

