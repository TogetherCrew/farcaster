# Farcaster

Collection of scripts for fetching and processing data from Farcaster. 
Each directory within `pipelines` contains standalone pipelines that can be run independently, i.e. `fetch`, `ingest`, `analyze`.

```
Structure
Copy├── pipelines/
│   ├── fetch-farcaster/         etc, etc
│   └── ... other pipelines
```


## Requirements
- Python 3.2+
- `virtualenv`
- `.env` with required credentials, refer to  `dotenv.template`

## Quick Start
- Clone the repo
- Create venv, i.e. `python -m venv env`
- `pip install -r requirements.txt`
- Populate `.env` file
- Run desired pipeline:  `python -m pipeline.<>directory<>.<script>`, i.e. `python -m pipelines.fetch-farcaster-data.fetch`
