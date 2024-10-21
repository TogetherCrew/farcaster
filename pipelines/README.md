# fetch-farcaster
Collects data from selected Farcaster channels including members, followers, and casts.


## Setup

- Make sure you're in the project root
- Create .env file with...

```
NEYNAR_API_KEY=your_key
AWS_ACCESS_KEY=your_key
AWS_SECRET_ACCESS_KEY=your_key
BUCKET_NAME=your_bucket
CUTOFF=7  # days of data to fetch
CHANNEL_IDS=["optimism","arbitrum"]  # array of channel IDs
```

## Usage
`python -m pipelines.fetch-farcaster.fetch`


## What it does
- Fetches channel metadata, i.e. channel admins
- Fetches all channel members and followers
- Fetches additional channels for all members and followers
- Fetches all channel casts from the last X days, where X is value entered into `CUTOFF`
- Saves everything to S3 with timestamp in filename
- Handles rate limits and retries automatically

## Output
Data is saved to S3 as: `data_farcaster_YYYY-MM-DD-HH-MM.json`

## Todos
- API response formats
- Recommended best practices, i.e. avoid manipulating retrieved data if possible
