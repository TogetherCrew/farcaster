# Farcaster Data Pipeline

A data pipeline for processing Farcaster social data into Neo4J.

## How It Works

### Core Components

#### ingest.py
The main ingestion script handles the data flow:
1. Loads raw data from S3
2. Creates database indexes for each object type
3. Processes channels and their followers
4. Links users to channels and wallets
5. Processes and links casts

#### helpers.py
Provides database interaction and utility functions:
- Query execution with retry logic
- Data chunking for large datasets
- S3 operations
- Text sanitization

#### cyphers.py
Contains all Neo4j queries for:
- Creating/updating nodes
- Establishing relationships
- Setting node properties
- Managing indexes

### Data Flow
```
Raw S3 Data → Channel Processing → User Processing → Cast Processing → Neo4j Graph
```

## Object Definitions

### Nodes

#### User (:User:Farcaster)
```
{
    fid: string,            // Farcaster ID
    username: string,       // @handle
    displayName: string,    // Display name
    bio: string,           // User bio
    powerBadge: string     // User's power badge status
}
```

#### Cast (:Cast:Farcaster)
```
{
    hash: string,          // Unique cast identifier
    authorFid: string,     // FID of author
    threadHash: string,    // Parent thread identifier
    parentHash: string,    // Direct parent cast
    text: string,          // Cast content
    repliesCount: int,     // Number of replies
    recastsCount: int,     // Number of recasts
    likesCount: int        // Number of likes
}
```

#### Channel (:Channel:Farcaster)
```
{
    channelId: string,     // Unique channel ID
    name: string,          // Channel name
    url: string,           // Channel URL
    description: string,   // Channel description
    moderatorFids: array   // List of moderator FIDs
}
```

#### Wallet (:Wallet:Farcaster)
```
{
    address: string        // Wallet address
}
```

### Relationships

```
[:POSTED]  User → Cast     // User created cast
[:FOLLOW]  User → Channel  // User follows channel
[:MEMBER]  User → Channel  // User is channel member
[:ACCOUNT] User → Wallet   // User owns wallet
```

## Data Processing Examples

### Creating a User-Cast Relationship
```cypher
MATCH (cast:Cast:Farcaster)
WHERE NOT (cast)-[:POSTED]-()
WITH cast 
MATCH (author:User:Farcaster)
WHERE author.fid = cast.authorFid 
MERGE (author)-[r:POSTED]->(cast)
```

### Linking Users to Channels
```cypher
MATCH (user:User {fid: rows.fid})
MATCH (channel:Channel {channelId: channelId})
MERGE (user)-[r:FOLLOW]->(channel)
```

### Processing Wallet Connections
```cypher
MATCH (user:User:Farcaster {fid: rows.fid})
MERGE (wallet:Wallet:Farcaster {address: rows.custody_address})
MERGE (user)-[r:ACCOUNT]->(wallet)
SET r.source = 'Farcaster'
SET r.type = 'custody_address'
```