# Sleeper Fantasy Football Stats Analysis

A comprehensive statistical analysis tool for your Sleeper fantasy football league. Fetches live league data and computes detailed matchup analytics using DuckDB for fast, local querying.

## Features

- **16 Different Stats** covering performance, consistency, luck, and head-to-head matchups
- **Live Data** fetched directly from Sleeper API
- **Fast Queries** using in-memory DuckDB database
- **Beautiful Output** formatted for easy reading

## Stats Included

### Core Performance
- **Overall Standings** — Win/loss records and average points per game
- **Top 10 Scores** — Highest individual week performances
- **Top 10 Lowest Scores** — Lowest individual week performances

### Frequency Analysis
- **Most High Scores by Week** — How many times each team led the league
- **Most Low Scores by Week** — How many times each team finished last

### Head-to-Head
- **Most Wins vs Lowest Opponent** — Clutch wins when facing weak weeks
- **Most Losses vs Highest Opponent** — Tough losses facing hot teams
- **Head-to-Head Records** — Complete record vs each opponent

### Advanced Metrics
- **Consistency Score** — Standard deviation (lower = more consistent)
- **Avg Points in Wins** — Average scoring when winning
- **Avg Points in Losses** — Average scoring when losing
- **Most Dominant Wins** — Top 10 biggest point margins
- **Closest Matchups** — Top 10 games decided by smallest margins
- **Lucky/Unlucky Record** — Actual vs expected record based on points scored
- **Week-to-Week Volatility** — Biggest scoring swings between consecutive weeks

## Setup

### Requirements
- Python 3.9+
- Sleeper League ID (from your league URL)

### Installation

1. Clone the repo
```bash
git clone https://github.com/yourusername/hoyt-stats.git
cd hoyt-stats
```

2. Create a virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
```

3. Install dependencies
```bash
pip install -r requirements.txt
```

4. Create `league_id.txt` with your Sleeper league ID
```bash
echo "YOUR_LEAGUE_ID_HERE" > league_id.txt
```

## Usage

### Jupyter Notebook (Recommended)
The easiest way to explore stats and customize queries:

```bash
jupyter notebook sleeper_stats_analysis.ipynb
```

Run all cells to fetch data and generate the full stats report.

### Python Script
For batch analysis or automation:

```bash
python sleeper_stats.py
```

## File Structure

```
hoyt-stats/
├── README.md                          # This file
├── league_id.txt                      # Your Sleeper league ID
├── requirements.txt                   # Python dependencies
├── sleeper_stats.py                   # Standalone Python script
└── sleeper_stats_analysis.ipynb       # Jupyter notebook (recommended)
```

## How It Works

1. **Fetch Data** — Connects to Sleeper API and retrieves league info, rosters, and matchups
2. **Transform** — Converts raw API data into pandas DataFrames
3. **Register** — Loads data into in-memory DuckDB database
4. **Query** — Runs SQL queries to compute stats
5. **Display** — Outputs formatted results with emojis and alignment

All analysis happens locally—no external databases or API calls during querying.

## Customization

The notebook is designed to be easily customizable:

- **Change Week Range** — Modify `WHERE week >= 1 AND week <= 14` to analyze different weeks
- **Custom Queries** — Use DuckDB with standard SQL to explore the `matchups` table
- **Matchups Table Schema**:
  - `week` — Week number
  - `team_name` — Team name
  - `points` — Points scored in that matchup
  - `won` — 1 if team won, 0 if lost
  - `roster_id`, `matchup_id`, `top_player_id`, `players_json` — Additional data

## Data Accuracy

Stats are calculated only for **completed weeks** (weeks 1-14 for a typical season). Unplayed weeks are automatically filtered out.

## API Reference

Uses the free [Sleeper API](https://api.sleeper.app/v1/):
- `/league/{league_id}` — League information
- `/league/{league_id}/users` — Team owners
- `/league/{league_id}/rosters` — Roster assignments
- `/league/{league_id}/matchups/{week}` — Matchup data

## License

MIT

## Questions?

Feel free to customize this for your league and explore the data!
