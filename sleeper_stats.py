"""
Fantasy Football Stats Analysis using Sleeper API and DuckDB
Fetches league matchups and player data for comprehensive stats exploration
"""

import requests
import json
import duckdb
from datetime import datetime
from typing import Dict, List, Any
import pandas as pd

# Read league ID from file
with open('league_id.txt', 'r') as f:
    LEAGUE_ID = f.read().strip()

BASE_URL = "https://api.sleeper.app/v1"

class SleeperStats:
    def __init__(self, league_id: str):
        self.league_id = league_id
        self.db = duckdb.connect(':memory:')
        self.league_info = None
        self.users = {}
        self.rosters = {}
        self.matchups = {}
        
    def fetch_league_info(self):
        """Fetch league information"""
        print(f"Fetching league info for league {self.league_id}...")
        response = requests.get(f"{BASE_URL}/league/{self.league_id}")
        self.league_info = response.json()
        print(f"League: {self.league_info.get('name', 'N/A')}")
        return self.league_info
    
    def fetch_users(self):
        """Fetch all users in the league"""
        print("Fetching users...")
        response = requests.get(f"{BASE_URL}/league/{self.league_id}/users")
        users_list = response.json()
        self.users = {user['user_id']: user for user in users_list}
        return self.users
    
    def fetch_rosters(self):
        """Fetch all rosters"""
        print("Fetching rosters...")
        response = requests.get(f"{BASE_URL}/league/{self.league_id}/rosters")
        rosters_list = response.json()
        self.rosters = {roster['roster_id']: roster for roster in rosters_list}
        return self.rosters
    
    def fetch_matchups(self):
        """Fetch all matchups for all weeks"""
        print("Fetching matchups for all weeks...")
        matchups_by_week = {}
        
        # Try to get matchups for weeks 1-17 (standard NFL season)
        for week in range(1, 18):
            try:
                response = requests.get(f"{BASE_URL}/league/{self.league_id}/matchups/{week}")
                if response.status_code == 200:
                    matchups_by_week[week] = response.json()
                    print(f"  Week {week}: {len(matchups_by_week[week])} matchups")
            except Exception as e:
                print(f"  Week {week}: Error or no data - {e}")
                break
        
        self.matchups = matchups_by_week
        return self.matchups
    
    def fetch_playoff_matchups(self):
        """Fetch playoff matchups"""
        print("Fetching playoff matchups...")
        playoff_weeks = {}
        
        for week in range(1, 5):
            try:
                response = requests.get(f"{BASE_URL}/league/{self.league_id}/matchups/{week + 17}")
                if response.status_code == 200:
                    playoff_weeks[week + 17] = response.json()
                    print(f"  Playoff week {week}: {len(playoff_weeks[week + 17])} matchups")
            except Exception as e:
                break
        
        self.matchups.update(playoff_weeks)
        return self.matchups
    
    def get_player_name(self, player_id: str) -> str:
        """Fetch player name from Sleeper"""
        try:
            response = requests.get(f"{BASE_URL}/player/nfl/{player_id}")
            data = response.json()
            return data.get('full_name', f'Player_{player_id}')
        except:
            return f'Player_{player_id}'
    
    def build_database(self):
        """Build DuckDB database with matchup data"""
        print("\nBuilding DuckDB database...")
        
        # Create matchups table
        matchup_records = []
        
        for week, matchups in self.matchups.items():
            for matchup in matchups:
                roster_id = matchup['roster_id']
                matchup_id = matchup['matchup_id']
                
                # Get team name
                roster = self.rosters.get(roster_id, {})
                owner_id = roster.get('owner_id')
                user = self.users.get(owner_id, {})
                team_name = user.get('metadata', {}).get('team_name', f'Team_{roster_id}')
                
                # Get points
                points = matchup.get('points', 0)
                
                # Get player scores
                players = matchup.get('players', [])
                player_scores = {}
                for player_id in players:
                    score = matchup.get('players_points', {}).get(player_id, 0)
                    if score > 0:
                        player_scores[player_id] = score
                
                # Get top scorer
                if player_scores:
                    top_player_id = max(player_scores, key=player_scores.get)
                    top_player_score = player_scores[top_player_id]
                else:
                    top_player_id = None
                    top_player_score = 0
                
                matchup_records.append({
                    'week': week,
                    'roster_id': roster_id,
                    'team_name': team_name,
                    'matchup_id': matchup_id,
                    'points': points,
                    'top_player_id': top_player_id,
                    'top_player_score': top_player_score,
                    'players_json': json.dumps(player_scores)
                })
        
        # Create matchups dataframe
        matchups_df = pd.DataFrame(matchup_records)
        self.db.register('matchups_raw', matchups_df)
        
        # Create winners view
        self.db.execute("""
            CREATE TABLE matchups AS
            WITH ranked_matchups AS (
                SELECT 
                    week,
                    roster_id,
                    team_name,
                    matchup_id,
                    points,
                    top_player_id,
                    top_player_score,
                    players_json,
                    ROW_NUMBER() OVER (PARTITION BY week, matchup_id ORDER BY points DESC) as rank_in_matchup
                FROM matchups_raw
            )
            SELECT 
                week,
                roster_id,
                team_name,
                matchup_id,
                points,
                top_player_id,
                top_player_score,
                players_json,
                CASE WHEN rank_in_matchup = 1 THEN 1 ELSE 0 END as won,
                rank_in_matchup
            FROM ranked_matchups
        """)
        
        print("Database built successfully!")
        self.db.execute("SELECT COUNT(*) as record_count FROM matchups").fetchall()
        print(f"Loaded {self.db.execute('SELECT COUNT(*) FROM matchups').fetchall()[0][0]} matchup records")
    
    # ===== STATISTICS QUERIES =====
    
    def stat_1_highest_scores_by_week(self):
        """Who had the most highest scores by week"""
        print("\n=== STAT 1: Most Highest Scores by Week ===")
        query = """
            SELECT 
                team_name,
                COUNT(*) as count_of_highest_scores,
                STRING_AGG(CAST(week as VARCHAR), ', ') as weeks
            FROM (
                SELECT 
                    week,
                    team_name,
                    points,
                    ROW_NUMBER() OVER (PARTITION BY week ORDER BY points DESC) as rank
                FROM matchups
            )
            WHERE rank = 1
            GROUP BY team_name
            ORDER BY count_of_highest_scores DESC
        """
        result = self.db.execute(query).fetchall()
        df = self.db.execute(query).df()
        print(df)
        return df
    
    def stat_2_top_10_scores_overall(self):
        """Who had the top 10 scores overall"""
        print("\n=== STAT 2: Top 10 Scores Overall ===")
        query = """
            SELECT 
                team_name,
                week,
                points,
                ROW_NUMBER() OVER (ORDER BY points DESC) as rank
            FROM matchups
            ORDER BY points DESC
            LIMIT 10
        """
        df = self.db.execute(query).df()
        print(df)
        return df
    
    def stat_3_lowest_scores_by_week(self):
        """Who had the most lowest scores by week"""
        print("\n=== STAT 3: Most Lowest Scores by Week ===")
        query = """
            SELECT 
                team_name,
                COUNT(*) as count_of_lowest_scores,
                STRING_AGG(CAST(week as VARCHAR), ', ') as weeks
            FROM (
                SELECT 
                    week,
                    team_name,
                    points,
                    ROW_NUMBER() OVER (PARTITION BY week ORDER BY points ASC) as rank
                FROM matchups
            )
            WHERE rank = 1
            GROUP BY team_name
            ORDER BY count_of_lowest_scores DESC
        """
        df = self.db.execute(query).df()
        print(df)
        return df
    
    def stat_3b_top_10_lowest_scores_overall(self):
        """Top 10 lowest scores overall"""
        print("\n=== STAT 3B: Top 10 Lowest Scores Overall ===")
        query = """
            SELECT 
                team_name,
                week,
                points,
                ROW_NUMBER() OVER (ORDER BY points ASC) as rank
            FROM matchups
            ORDER BY points ASC
            LIMIT 10
        """
        df = self.db.execute(query).df()
        print(df)
        return df
    
    def stat_4_most_wins_vs_lowest_score(self):
        """Most wins against the lowest score"""
        print("\n=== STAT 4: Most Wins Against Lowest Score ===")
        query = """
            WITH weekly_lowest AS (
                SELECT 
                    week,
                    MIN(points) as lowest_score
                FROM matchups
                GROUP BY week
            ),
            wins_vs_lowest AS (
                SELECT 
                    m.team_name,
                    m.week,
                    m.points,
                    wl.lowest_score,
                    m.won
                FROM matchups m
                JOIN weekly_lowest wl ON m.week = wl.week
                WHERE m.points = wl.lowest_score AND m.won = 1
            )
            SELECT 
                team_name,
                COUNT(*) as wins_with_lowest_score,
                AVG(points) as avg_points,
                STRING_AGG(CAST(week as VARCHAR), ', ') as weeks
            FROM wins_vs_lowest
            GROUP BY team_name
            ORDER BY wins_with_lowest_score DESC
        """
        df = self.db.execute(query).df()
        print(df)
        return df
    
    def stat_5_most_losses_vs_highest_score(self):
        """Most losses against the highest score"""
        print("\n=== STAT 5: Most Losses Against Highest Score ===")
        query = """
            WITH weekly_highest AS (
                SELECT 
                    week,
                    MAX(points) as highest_score
                FROM matchups
                GROUP BY week
            ),
            losses_vs_highest AS (
                SELECT 
                    m.team_name,
                    m.week,
                    m.points,
                    wh.highest_score,
                    m.won
                FROM matchups m
                JOIN weekly_highest wh ON m.week = wh.week
                WHERE m.points = wh.highest_score AND m.won = 0
            )
            SELECT 
                team_name,
                COUNT(*) as losses_with_highest_score,
                AVG(points) as avg_points,
                STRING_AGG(CAST(week as VARCHAR), ', ') as weeks
            FROM losses_vs_highest
            GROUP BY team_name
            ORDER BY losses_with_highest_score DESC
        """
        df = self.db.execute(query).df()
        print(df)
        return df
    
    def stat_6_avg_points_in_win(self):
        """Average points in a win"""
        print("\n=== STAT 6: Average Points in a Win ===")
        query = """
            SELECT 
                team_name,
                COUNT(*) as wins,
                AVG(points) as avg_points_in_win,
                MIN(points) as min_win,
                MAX(points) as max_win
            FROM matchups
            WHERE won = 1
            GROUP BY team_name
            ORDER BY avg_points_in_win DESC
        """
        df = self.db.execute(query).df()
        print(df)
        return df
    
    def stat_7_avg_points_in_loss(self):
        """Average points in a loss"""
        print("\n=== STAT 7: Average Points in a Loss ===")
        query = """
            SELECT 
                team_name,
                COUNT(*) as losses,
                AVG(points) as avg_points_in_loss,
                MIN(points) as min_loss,
                MAX(points) as max_loss
            FROM matchups
            WHERE won = 0
            GROUP BY team_name
            ORDER BY avg_points_in_loss DESC
        """
        df = self.db.execute(query).df()
        print(df)
        return df
    
    def run_all_stats(self):
        """Run all statistics"""
        print("=" * 80)
        print("FANTASY FOOTBALL LEAGUE STATISTICS")
        print("=" * 80)
        
        self.stat_1_highest_scores_by_week()
        self.stat_2_top_10_scores_overall()
        self.stat_3_lowest_scores_by_week()
        self.stat_3b_top_10_lowest_scores_overall()
        self.stat_4_most_wins_vs_lowest_score()
        self.stat_5_most_losses_vs_highest_score()
        self.stat_6_avg_points_in_win()
        self.stat_7_avg_points_in_loss()
    
    def interactive_query(self, query: str):
        """Run a custom SQL query against the database"""
        try:
            df = self.db.execute(query).df()
            print(df)
            return df
        except Exception as e:
            print(f"Error executing query: {e}")
            return None


def main():
    print("Initializing Sleeper Stats Analyzer...")
    
    # Create stats object
    stats = SleeperStats(LEAGUE_ID)
    
    # Fetch data
    stats.fetch_league_info()
    stats.fetch_users()
    stats.fetch_rosters()
    stats.fetch_matchups()
    stats.fetch_playoff_matchups()
    
    # Build database
    stats.build_database()
    
    # Run all stats
    stats.run_all_stats()
    
    # Interactive shell
    print("\n" + "=" * 80)
    print("INTERACTIVE MODE - Enter SQL queries (or 'quit' to exit)")
    print("Available table: 'matchups' with columns: week, roster_id, team_name, matchup_id,")
    print("                 points, top_player_id, top_player_score, won")
    print("=" * 80)
    
    while True:
        try:
            query = input("\nEnter SQL query: ").strip()
            if query.lower() == 'quit':
                break
            if query:
                stats.interactive_query(query)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main()
