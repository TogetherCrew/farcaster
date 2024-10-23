# Farcaster Ingestion & Cypher Overview

The `helpers.py` file contains helpers functions to ingest data from S3, process it, and load it into a Neo4j database, with specific logic for handling Farcaster channels and users. It consists of two key classes: **FarcasterIngester** (which handles data ingestion) and **FarcasterCyphers** (which manages Neo4j queries). 

## How It Works

### 1. **FarcasterIngester Class**
   - **Purpose:** This class extends the base `Ingestor` class and adds custom logic for ingesting Farcaster data, specifically handling channel and user relationships.
   - **Key Workflow:**
     1. **Initialization:**
        - When initialized, it sets up a connection to the **"tc-farcaster-data"** S3 bucket and loads channel data into the `scraper_data` attribute.
        - It also sets up an instance of `FarcasterCyphers`, which handles Neo4j interactions.
     2. **Data Loading & Processing:**
        - The ingester reads the latest Farcaster channel data from S3, converting it into a Pandas DataFrame.
        - The `create_or_merge_channels()` method filters the DataFrame to retain only relevant columns (`id`, `url`, `name`, `description`), then saves this data as a CSV in S3.
        - The S3 file URL is passed to the **Cypher** methods for insertion into the Neo4j database.
     3. **Handling Cypher Queries:**
        - For each function that processes a different type of data (e.g., users, wallets, channel members), there’s a corresponding method in the **Cyphers** class that handles the database operations.
        - The `run()` method defines the overall ingestion flow. It starts with channel ingestion and can be extended to include other data types like users, channel members, and relationships.
     4. **Output:**
        - After executing each ingestion step, URLs of saved CSV files are printed to provide confirmation that data is processed and uploaded to S3.

### 2. **FarcasterCyphers Class**
   - **Purpose:** Extends the base `Cypher` class, implementing custom Cypher queries for creating and merging Farcaster-specific nodes and relationships.
   - **Key Workflow:**
     1. **Handling Neo4j Connections:**
        - The class inherits methods for establishing connections to Neo4j, executing queries, and handling retries on failure.
     2. **Channel Merging:**
        - The `create_or_merge_channels()` method receives the URLs of CSV files saved by the **Ingester**. 
        - It runs a Cypher `LOAD CSV` command to insert or merge channel data into the Neo4j database.
        - The use of `MERGE` ensures that channels are not duplicated, updating existing entries or creating new ones as needed.
     3. **Query Logging:**
        - Each query method is decorated with logging helpers (`@count_query_logging`) to provide real-time feedback on the number of nodes created or merged.
        - This makes it easy to monitor the progress of data ingestion directly from the logs.

### 3. **AWS S3 Integration**
   - **Purpose:** The S3 integration enables loading and saving data during the ingestion process.
   - **Key Workflow:**
     1. **Loading Data:**
        - The `FarcasterIngester` starts by fetching the most recent data files from S3, reading them into memory for processing.
     2. **Saving Data:**
        - As data is transformed into a suitable format (e.g., DataFrame), it’s saved back to S3 in CSV format. This step involves chunking large files to avoid memory issues.
     3. **Data Reusability:**
        - Once saved to S3, the processed data is accessible via generated URLs, making it available for subsequent database insertion or analysis.

### Putting It All Together
The typical workflow for ingesting Farcaster data is as follows:

1. **Load Data from S3:** 
   - The `FarcasterIngester` fetches the latest Farcaster data from S3 and loads it into memory.
   
2. **Transform Data:**
   - Data is processed into Pandas DataFrames, filtered to retain necessary columns, and saved back to S3 as CSVs.
   
3. **Run Cypher Queries:**
   - The CSV URLs are passed to the **FarcasterCyphers** class, which executes Cypher queries to create or merge data into Neo4j.
   
4. **Save Processed Data:**
   - Final datasets are saved back to S3, ensuring that the data is up-to-date and accessible for further ingestion.

### Extending the Workflow
- To handle additional data types (e.g., users, wallets), implement the respective methods in both `FarcasterIngester` and `FarcasterCyphers`.
- Adjust the `run()` method in **FarcasterIngester** to define the sequence of ingestion steps, adding calls to new ingestion methods as needed.

