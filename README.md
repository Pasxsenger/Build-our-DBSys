# Build-our-DBSys
This is the course project of DSCI 551.<br>
Author: Xuesong Shen & Haoze Zhu

## Google Doc
https://docs.google.com/document/d/19oX2cgOgHJJUakujflavoMcYnCBVIXrBk3PaOoyqP6k/edit

## System Structure
![system diagram](https://github.com/Pasxsenger/Build-our-DBSys/blob/main/Pictures/ourDB_Sys_Structure.drawio.png)

## Function Diagram
![function diagram](https://github.com/Pasxsenger/Build-our-DBSys/blob/main/Pictures/functions.png)
https://1drv.ms/u/s!AhfT-JkcYBnml3jXaKHAECIvk3xX?e=exyYEb



## Relational
### Dataset
#### [NBA games data](https://www.kaggle.com/datasets/nathanlauga/nba-games)
| File Name    | File Format | File Size |
| ------------ | ----------- | --------- |
| teams | csv         |       4 KB    |
| players | csv         |      266 KB     |
| ranking | csv         |     15.5 MB      |
| games | csv         |      4.1 MB     |
| games_details | csv         |      93.1 MB     |


## Non-relational
### Dataset
#### [Indonesia's Top E-Commerce Tweets](https://www.kaggle.com/datasets/robertvici/indonesia-top-ecommerce-unicorn-tweets/code)
| File Name    | File Format | File Size |
| ------------ | ----------- | --------- |
| bliblidotcom_stats | json         |    31.4 MB       |
| bliblidotcom_tweet | json         |    20.9 MB       |
| bukalapak_stats | json         |    27.9 MB       |
| bukalapak_tweet | json         |    17.7 MB       |
| lazadaID_stats | json         |    72.4 MB       |
| lazadaID_tweet | json         |    43 MB       |
| ShopeeID_stats | json         |    145.5 MB       |
| ShopeeID_tweet | json         |    82 MB       |
| tokopedia_stats | json         |    17.5 MB       |
| tokopedia_tweet | json         |    11.1 MB       |

## Query Language
### 0. Database and Table
#### Creating a Database 
Format: Create a new database named [database name]; <br>
Example: Create a new database named NBA;

#### Using a Specific Database 
Format: Switch to database [database name]; <br>
Example: Switch to database NBA;

#### Deleting a Database 
Format: Delete database [database name]; <br>
Example: Delete database NBA; 

#### Showing Databases
Format: Databases; <br>
Example: Databases;

#### Creating a Table 
Format: Set up a new table named [table name] with columns [column names]; <br>
Example: Set up a new table named teams with columns team_id, city, owner;

#### Dropping a Table
Format: Drop table [table name]; <br>
Example: Drop table teams;

#### Showing Tables 
Format: Tables; <br>
Example: Tables;

### 1. Retrieving Data (Projection & Filtering)
Format: Show [column(s)]/[all] of [table] where [condition(s)] [line m-n]; <br>
Example: Show team_id, city of teams where city = LA line 2-10;

### 2. Connecting Tables (Block-based Nested Loop Join)
Format: Connect [table1] with [table2] based on [common feature];  <br>
Example: Connect teams with players based on team_id = team_id;

### 3. Grouping and Aggregation
Format: Summarize [column] on [column]/[all] from [table] using [aggregation];<br>
Example: Summarize yearfounded on teams_id from teams using min;<br>
Aggregation: avg/sum (numeric only); count/min/max

### 4. Sorting Data (External Merge Sort)
Format: Sort [table] by [column] in [asc/desc] order; <br>
Example: Sort teams by yearfounded in asc order;

### 5. Inserting Data
Format: Add [row] to [table]; <br>
Example: Add John Doe, Guard to players;

### 6. Updating Data
Format: Change [column] to [new value] for [table] with [condition] ;<br>
Example: Change position to forward for players with name = John Doe;

### 7. Deleting Data
Format: Remove row with [condition] from [table];<br>
Example: Remove row with name = John Doe from players;
