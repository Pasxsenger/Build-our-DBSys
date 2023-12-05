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
Format: Set up a new table named [table name] with columns [column names];
Format: Set up a new collection named [collection name] with [json object];
Example: Set up a new table named teams with columns team_id, city, owner;
Example: Set up a new collection named teams with {"ID": "1", "Name": "USC"};


#### Dropping a Table
Format: Drop table/collection [table/collection name];
Example: Drop table teams; drop collection teams;

#### Showing Tables 
Format: Tables;
Format: collections;
Example: Tables;

### 1. Retrieving Data (Projection & Filtering)
Format: Show [column/field(s)]/[all] of [table/collection] where [condition(s)] [line m-n];<br>
Example: Show team_id, city of teams where city = LA line 2-10;

### 2. Connecting Tables (Block-based Nested Loop Join)
Format: Connect [table1/collection1] with [table2/collection2] based on [common feature];<br>
Example: Connect teams with players based on team_id = team_id;

### 3. Grouping and Aggregation
Format: Summarize [column/field] on [column/field]/[all] from [table/collection] using [aggregation];<br>
Example: Summarize yearfounded on teams_id from teams using min;<br>
Aggregation: avg/sum (numeric only); count/min/max

### 4. Sorting Data (External Merge Sort)
Format: Sort [table/collection] by [column/field] in [asc/desc] order;<br>
Example: Sort teams by yearfounded in asc order;

### 5. Inserting Data
Format: Add [row] to [table/collection];<br>
Example: Add John Doe, Guard to players;

### 6. Updating Data
Format: Change [column/field] to [new value] for [table] with [condition];<br>
Example: Change position to forward for players with name = John Doe;

### 7. Deleting Data
Format: Remove row with [condition] from [table];<br>
Example: Remove row with name = John Doe from players;

## System operation

Change the current path to “ourDB/Code”, then run “python3 main.py”.

![enter system](https://github.com/Pasxsenger/Build-our-DBSys/blob/main/Pictures/enter_system.png)

To enter the SQL database system, run the command “sql”;

![choose sql](https://github.com/Pasxsenger/Build-our-DBSys/blob/main/Pictures/choose_sql.png)

To enter the NoSql database system, run the command “nosql”;

![choose nosql](https://github.com/Pasxsenger/Build-our-DBSys/blob/main/Pictures/choose_nosql.png)

Run the command “databases;” to see all databases in the database system.

![SQL database](https://github.com/Pasxsenger/Build-our-DBSys/blob/main/Pictures/databases_sql.png)
![nosql databases](https://github.com/Pasxsenger/Build-our-DBSys/blob/main/Pictures/databases_nosql.png)

To use the database nba in the SQL databases, run the command “switch to database nba;”.

To show the tables in the database, run the command “tables;”.

To use the database eco in the NoSQL databases, run the command “switch to database eco;”.

To show the collections in the database, run the command “collections;”.


![switch SQL database](https://github.com/Pasxsenger/Build-our-DBSys/blob/main/Pictures/switch_sql_base.png)
![switch nosql databases](https://github.com/Pasxsenger/Build-our-DBSys/blob/main/Pictures/switch_nosql_base.png)


After switching to a specific database, one can use other query operations or CRUD commands on that database.

To exit the SQL database system, run the command “exit”.

All the runtime files in the running folders will be deleted at the same time.

To exit ourDB, run the command “exit”.

![exit system](https://github.com/Pasxsenger/Build-our-DBSys/blob/main/Pictures/exit_system.png)
