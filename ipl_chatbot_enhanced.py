import pandas as pd
from groq import Groq
from sqlalchemy import create_engine, text
import time
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class IPLStatsEnhancedChatbot:
    def __init__(self, database_url: str, groq_api_key: str):
        """Initialize the Enhanced IPL Stats Chatbot with PostgreSQL backend"""
        self.database_url = database_url
        self.client = Groq(api_key=groq_api_key)
        self.engine = None
        self._connect_database()
        self._create_data_summary()
        self._initialize_bowling_classifications()
    
    def _connect_database(self):
        """Connect to PostgreSQL database"""
        print("Connecting to PostgreSQL database...")
        try:
            self.engine = create_engine(self.database_url)
            # Test connection
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM ipl_balls")).fetchone()
                print(f"✅ Connected to database with {result[0]:,} records")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            raise
    
    def _initialize_bowling_classifications(self):
        """Initialize bowling style classifications"""
        self.pace_styles = ['rm', 'rfm', 'rmf', 'lf', 'lfm', 'lmf']
        self.spin_styles = ['ob', 'lb', 'sla', 'lbg', 'lws']
        
    def _create_data_summary(self):
        """Create a comprehensive summary of the database schema for the LLM"""
        try:
            with self.engine.connect() as conn:
                # Get basic stats
                stats = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(DISTINCT season) as seasons,
                        COUNT(DISTINCT match_id) as matches,
                        MIN(date) as first_date,
                        MAX(date) as last_date,
                        MIN(year) as first_year,
                        MAX(year) as last_year
                    FROM ipl_balls
                """)).fetchone()
                
                # Get teams
                teams = conn.execute(text("""
                    SELECT DISTINCT batting_team 
                    FROM ipl_balls 
                    WHERE batting_team != '' 
                    ORDER BY batting_team
                """)).fetchall()
                
                team_list = [team[0] for team in teams][:15]  # Top 15 teams
                
        except Exception as e:
            print(f"Error getting database stats: {e}")
            stats = (277935, 17, 1000, '2008-04-18', '2024-05-26', 2008, 2024)
            team_list = ['CSK', 'MI', 'RCB', 'KKR', 'SRH', 'DC', 'PBKS', 'RR', 'GT', 'LSG']

        self.data_summary = f"""
Enhanced PostgreSQL IPL Database Schema:
- Total records: {stats[0]:,} ball-by-ball records
- Seasons: {stats[5]}-{stats[6]} ({stats[1]} seasons)
- Total matches: {stats[2]:,}
- Date range: {stats[3]} to {stats[4]}
- Teams: {team_list}

CRITICAL TABLE STRUCTURE - ipl_balls:
Core Columns (ALWAYS use these exact names with quotes):
- Match Info: "season", "year", "date", "venue", "match_id", "innings", "batting_team", "bowling_team"
- Ball Details: "over", "ball", "batter", "bowler", "runs_batter", "runs_total" 
- Results: "isFour", "isSix", "isWicket" (BOOLEAN columns)
- Player Info: "bowling_style", "bat_hand"
- Current Stats: "curr_batter_runs", "curr_batter_balls", "curr_batter_fours", "curr_batter_sixes"
- Match Results: "playerofmatch", "winner"

BOWLING STYLE CLASSIFICATION:
- Pace Bowlers: bowling_style IN ('rm','rfm','rmf','lf','lfm','lmf') OR bowling_style LIKE '%rm%' OR bowling_style LIKE '%rf%'
- Spin Bowlers: bowling_style IN ('ob','lb','sla','lbg','lws') OR bowling_style NOT LIKE ANY pace patterns

PHASE DEFINITIONS:
- Powerplay: "over" <= 6
- Middle overs: "over" >= 7 AND "over" <= 15
- Death overs: "over" >= 16

STATISTICAL CALCULATIONS:
For Batting Stats:
- Strike Rate = (Total Runs / Total Balls) * 100
- Batting Average = Total Runs / Times Out (where batter got out, not just balls faced)
- To find dismissals: COUNT balls where "isWicket" = TRUE AND "batter" = specific batter

For Bowling Stats:
- Economy Rate = (Total Runs Conceded / Total Balls) * 6
- Bowling Average = Total Runs Conceded / Total Wickets
- Strike Rate = Total Balls / Total Wickets
- Use only records where bowler actually bowled the ball

For Death Overs Analysis:
- Filter: "over" >= 16
- For pace vs batters: ADD bowling style filters
- Include strike rates, averages, boundaries

IMPORTANT NOTES:
1. ALL column names MUST be in quotes: "batter", "over", etc.
2. Boolean columns: "isFour", "isSix", "isWicket" 
3. For dismissals: Use "isWicket" = TRUE to identify when batter got out
4. For bowling analysis: Only count balls actually bowled by the bowler
5. Minimum thresholds: 100+ balls for batting, 50+ balls for bowling stats
"""

    def _get_query_from_llm(self, user_question: str) -> str:
        """Use Groq LLM to convert natural language to SQL query"""
        
        prompt = f"""
You are an expert PostgreSQL analyst specializing in cricket statistics. Convert the user's question to a precise SQL query.

Database Schema:
{self.data_summary}

CRITICAL QUERY RULES:
1. Table name: ipl_balls (MUST use exactly this)
2. ALL column names in quotes: "batter", "bowler", "over", "runs_batter", etc.
3. ALWAYS filter out empty names: WHERE "batter" != '' AND "batter" IS NOT NULL
4. For batting stats, calculate proper averages using dismissals
5. For bowling stats, use proper wicket counts and balls bowled
6. ALWAYS include minimum thresholds for meaningful stats
7. Sort by the main metric DESC and LIMIT 10-15 results
8. ALWAYS use proper PostgreSQL syntax with explicit casts ::numeric, ::int
9. For year filtering, use "year" = YYYY (integer column)
10. Use CASE WHEN for conditional counts

EXAMPLE QUERIES:

Top Run Scorers with complete stats:
SELECT 
    "batter",
    COUNT(*) as balls_faced,
    SUM("runs_batter") as total_runs,
    SUM("isFour"::int) as fours,
    SUM("isSix"::int) as sixes,
    ROUND(AVG("runs_batter"::numeric), 2) as avg_per_ball,
    ROUND((SUM("runs_batter")::numeric / NULLIF(COUNT(*), 0)) * 100, 2) as strike_rate,
    COUNT(CASE WHEN "isWicket" = TRUE THEN 1 END) as times_out,
    ROUND(SUM("runs_batter")::numeric / NULLIF(COUNT(CASE WHEN "isWicket" = TRUE THEN 1 END), 0), 2) as batting_average
FROM ipl_balls 
WHERE "batter" != '' AND "batter" IS NOT NULL
GROUP BY "batter"
HAVING SUM("runs_batter") > 500
ORDER BY total_runs DESC LIMIT 10;

Death Overs vs Pace:
SELECT 
    "batter",
    COUNT(*) as death_balls_vs_pace,
    SUM("runs_batter") as death_runs,
    ROUND((SUM("runs_batter")::numeric / NULLIF(COUNT(*), 0)) * 100, 2) as death_sr_vs_pace,
    SUM("isFour"::int) + SUM("isSix"::int) as boundaries
FROM ipl_balls 
WHERE "over" >= 16 
    AND ("bowling_style" LIKE '%rm%' OR "bowling_style" LIKE '%rf%' OR "bowling_style" IN ('rm','rfm','rmf','lf','lfm','lmf'))
    AND "batter" != '' AND "batter" IS NOT NULL
GROUP BY "batter"
HAVING COUNT(*) >= 30
ORDER BY death_runs DESC LIMIT 10;

Top Wicket Takers:
SELECT 
    "bowler",
    COUNT(CASE WHEN "isWicket" = TRUE THEN 1 END) as wickets,
    COUNT(*) as balls_bowled,
    SUM("runs_total") as runs_conceded,
    ROUND(SUM("runs_total")::numeric / NULLIF(COUNT(CASE WHEN "isWicket" = TRUE THEN 1 END), 0), 2) as bowling_avg,
    ROUND((SUM("runs_total")::numeric / NULLIF(COUNT(*), 0)) * 6, 2) as economy_rate
FROM ipl_balls 
WHERE "bowler" != '' AND "bowler" IS NOT NULL
GROUP BY "bowler"
HAVING COUNT(CASE WHEN "isWicket" = TRUE THEN 1 END) >= 10
ORDER BY wickets DESC LIMIT 10;

User Question: {user_question}

Return ONLY the complete SQL query with proper filtering and statistics:
"""

        try:
            response = self.client.chat.completions.create(
                model="llama3-8b-8192",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=800
            )
            
            query_code = response.choices[0].message.content.strip()
            
            # Clean up the response
            if "```sql" in query_code:
                query_code = query_code.split("```sql")[1].split("```")[0].strip()
            elif "```" in query_code:
                query_code = query_code.split("```")[1].strip()
            
            # Remove trailing semicolons
            query_code = query_code.rstrip(';').strip()
            
            return query_code
            
        except Exception as e:
            print(f"Error getting query from LLM: {e}")
            return None
    
    def _execute_query(self, query_code: str):
        """Execute SQL query and return results as DataFrame"""
        try:
            print(f"Executing query: {query_code[:100]}...")
            start_time = time.time()
            
            result_df = pd.read_sql(query_code, self.engine)
            
            execution_time = time.time() - start_time
            print(f"Query executed in {execution_time:.2f}s, returned {len(result_df)} rows")
            
            return result_df
            
        except Exception as e:
            print(f"Error executing query: {e}")
            return None
    
    def _format_result(self, result, user_question: str) -> str:
        """Format the query result into a comprehensive readable response"""
        if result is None:
            return "Sorry, I couldn't process your query. Please try rephrasing your question."
        
        if isinstance(result, pd.DataFrame) and len(result) == 0:
            return "No data found matching your criteria."
        
        try:
            question_lower = user_question.lower()
            return self._format_enhanced_dataframe(result, question_lower)
                
        except Exception as e:
            return f"Error formatting result: {e}"
    
    def _format_enhanced_dataframe(self, df, question_lower):
        """Format DataFrame results with comprehensive cricket statistics"""
        try:
            formatted = f"🏏 **Cricket Analytics Results:**\n\n"
            
            for i, (_, row) in enumerate(df.head(12).iterrows(), 1):
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"**{i:2d}.**"
                
                # Get player name
                name = row.get('batter', row.get('bowler', str(row.iloc[0])))
                
                # Format based on available columns with comprehensive stats
                stats_parts = []
                
                # Batting Statistics
                if 'total_runs' in df.columns:
                    stats_parts.append(f"**{row['total_runs']:,} runs**")
                elif 'runs' in df.columns:
                    stats_parts.append(f"**{row['runs']:,} runs**")
                elif 'death_runs' in df.columns:
                    stats_parts.append(f"**{row['death_runs']:,} death runs**")
                
                if 'strike_rate' in df.columns and pd.notna(row['strike_rate']):
                    stats_parts.append(f"SR: {row['strike_rate']:.1f}")
                elif 'death_sr_vs_pace' in df.columns and pd.notna(row['death_sr_vs_pace']):
                    stats_parts.append(f"Death SR: {row['death_sr_vs_pace']:.1f}")
                elif 'death_sr' in df.columns and pd.notna(row['death_sr']):
                    stats_parts.append(f"Death SR: {row['death_sr']:.1f}")
                
                if 'batting_average' in df.columns and pd.notna(row['batting_average']) and row['batting_average'] > 0:
                    stats_parts.append(f"Avg: {row['batting_average']:.1f}")
                
                if 'fours' in df.columns and 'sixes' in df.columns:
                    stats_parts.append(f"4s/6s: {row['fours']}/{row['sixes']}")
                elif 'boundaries' in df.columns:
                    stats_parts.append(f"Boundaries: {row['boundaries']}")
                
                # Bowling Statistics  
                if 'wickets' in df.columns:
                    stats_parts.append(f"**{row['wickets']} wickets**")
                
                if 'bowling_avg' in df.columns and pd.notna(row['bowling_avg']):
                    stats_parts.append(f"Avg: {row['bowling_avg']:.1f}")
                
                if 'economy_rate' in df.columns and pd.notna(row['economy_rate']):
                    stats_parts.append(f"Eco: {row['economy_rate']:.1f}")
                
                # Additional context
                if 'balls_faced' in df.columns and row['balls_faced'] > 0:
                    stats_parts.append(f"({row['balls_faced']} balls)")
                elif 'death_balls_vs_pace' in df.columns and row['death_balls_vs_pace'] > 0:
                    stats_parts.append(f"({row['death_balls_vs_pace']} vs pace)")
                elif 'balls_bowled' in df.columns and row['balls_bowled'] > 0:
                    stats_parts.append(f"({row['balls_bowled']} balls)")
                
                # Join all stats
                stats_str = " | ".join(stats_parts) if stats_parts else ""
                
                formatted += f"{medal} **{name}** - {stats_str}\n"
            
            if len(df) > 12:
                formatted += f"\n... and {len(df) - 12} more results"
            
            return formatted
            
        except Exception as e:
            return f"Error formatting results: {e}"
    
    def _try_enhanced_fallback_queries(self, question: str) -> str:
        """Enhanced fallback queries with comprehensive statistics"""
        question_lower = question.lower()
        
        try:
            # Top run scorers with full batting stats
            if (any(word in question_lower for word in ['top', 'best', 'highest']) and any(word in question_lower for word in ['run', 'scorer', 'batsman', 'batter'])) and not any(word in question_lower for word in ['death', '2024', '2023', '2022', 'pace', 'bowling']):
                query = '''
                SELECT 
                    "batter",
                    COUNT(*) as balls_faced,
                    SUM("runs_batter") as total_runs,
                    SUM("isFour"::int) as fours,
                    SUM("isSix"::int) as sixes,
                    ROUND((SUM("runs_batter")::numeric / NULLIF(COUNT(*), 0)) * 100, 2) as strike_rate,
                    COUNT(CASE WHEN "isWicket" = TRUE THEN 1 END) as times_out,
                    ROUND(SUM("runs_batter")::numeric / NULLIF(COUNT(CASE WHEN "isWicket" = TRUE THEN 1 END), 0), 2) as batting_average
                FROM ipl_balls 
                WHERE "batter" != '' AND "batter" IS NOT NULL
                GROUP BY "batter"
                HAVING SUM("runs_batter") > 500
                ORDER BY total_runs DESC LIMIT 12
                '''
                result = self._execute_query(query)
                return self._format_result(result, question)
            
            # Top wicket takers with bowling stats
            elif any(word in question_lower for word in ['wicket', 'bowler']) and 'top' in question_lower:
                query = '''
                SELECT 
                    "bowler",
                    COUNT(CASE WHEN "isWicket" = TRUE THEN 1 END) as wickets,
                    COUNT(*) as balls_bowled,
                    SUM("runs_total") as runs_conceded,
                    ROUND(SUM("runs_total")::numeric / NULLIF(COUNT(CASE WHEN "isWicket" = TRUE THEN 1 END), 0), 2) as bowling_avg,
                    ROUND((SUM("runs_total")::numeric / NULLIF(COUNT(*), 0)) * 6, 2) as economy_rate
                FROM ipl_balls 
                WHERE "bowler" != '' AND "bowler" IS NOT NULL
                GROUP BY "bowler"
                HAVING COUNT(CASE WHEN "isWicket" = TRUE THEN 1 END) >= 15
                ORDER BY wickets DESC LIMIT 12
                '''
                result = self._execute_query(query)
                return self._format_result(result, question)
            
            # Death overs vs pace with comprehensive stats
            elif ('death' in question_lower and 'pace' in question_lower) or ('death over' in question_lower and ('pace' in question_lower or 'vs pace' in question_lower)):
                query = '''
                SELECT 
                    "batter",
                    COUNT(*) as death_balls_vs_pace,
                    SUM("runs_batter") as death_runs,
                    ROUND((SUM("runs_batter")::numeric / NULLIF(COUNT(*), 0)) * 100, 2) as death_sr_vs_pace,
                    SUM("isFour"::int) + SUM("isSix"::int) as boundaries,
                    SUM("isFour"::int) as fours,
                    SUM("isSix"::int) as sixes
                FROM ipl_balls 
                WHERE "over" >= 16 
                    AND ("bowling_style" LIKE '%rm%' OR "bowling_style" LIKE '%rf%' OR "bowling_style" IN ('rm','rfm','rmf','lf','lfm','lmf'))
                    AND "batter" != '' AND "batter" IS NOT NULL
                GROUP BY "batter"
                HAVING COUNT(*) >= 25
                ORDER BY death_runs DESC LIMIT 12
                '''
                result = self._execute_query(query)
                return self._format_result(result, question)
            
            # Strike rate in death overs
            elif ('strike rate' in question_lower or 'sr' in question_lower) and 'death' in question_lower:
                query = '''
                SELECT 
                    "batter",
                    COUNT(*) as death_balls,
                    SUM("runs_batter") as death_runs,
                    ROUND((SUM("runs_batter")::numeric / NULLIF(COUNT(*), 0)) * 100, 2) as death_sr,
                    SUM("isFour"::int) + SUM("isSix"::int) as boundaries
                FROM ipl_balls 
                WHERE "over" >= 16 AND "batter" != '' AND "batter" IS NOT NULL
                GROUP BY "batter"
                HAVING COUNT(*) >= 30 AND SUM("runs_batter") > 100
                ORDER BY death_sr DESC LIMIT 12
                '''
                result = self._execute_query(query)
                return self._format_result(result, question)
            
            # Season-specific queries (2024, 2023, etc.)
            elif any(year in question_lower for year in ['2024', '2023', '2022', '2021']):
                year = None
                for y in ['2024', '2023', '2022', '2021']:
                    if y in question_lower:
                        year = int(y)
                        break
                
                if year:
                    query = f'''
                    SELECT 
                        "batter",
                        SUM("runs_batter") as total_runs,
                        COUNT(*) as balls_faced,
                        ROUND((SUM("runs_batter")::numeric / NULLIF(COUNT(*), 0)) * 100, 2) as strike_rate,
                        SUM("isFour"::int) as fours,
                        SUM("isSix"::int) as sixes,
                        COUNT(CASE WHEN "isWicket" = TRUE THEN 1 END) as times_out,
                        ROUND(SUM("runs_batter")::numeric / NULLIF(COUNT(CASE WHEN "isWicket" = TRUE THEN 1 END), 0), 2) as batting_average
                    FROM ipl_balls 
                    WHERE "year" = {year} AND "batter" != '' AND "batter" IS NOT NULL
                    GROUP BY "batter"
                    HAVING SUM("runs_batter") > 200
                    ORDER BY total_runs DESC LIMIT 12
                    '''
                    result = self._execute_query(query)
                    return self._format_result(result, question)
            
            else:
                return self._get_helpful_suggestions()
                       
        except Exception as e:
            return f"Sorry, I encountered an error while processing your question. Please try a different query."
    
    def _get_helpful_suggestions(self):
        """Provide helpful query suggestions"""
        return """I can help you with detailed IPL cricket analytics! Try asking about:

🏏 **Batting Analysis:**
• "Top run scorers in IPL history" (with strike rate, average, boundaries)
• "Best batters in death overs vs pace bowling"
• "Highest strike rates in powerplay"
• "Best batting averages in IPL 2024"

🎯 **Bowling Analysis:**  
• "Top wicket takers in IPL" (with economy, average, strike rate)
• "Best bowling figures in death overs"
• "Most economical bowlers in powerplay"

📊 **Advanced Queries:**
• "Strike rate analysis in death overs"
• "Left-handed batsmen vs spin bowling"
• "Team performance in specific seasons"

**Example:** Try "Best batters vs pace bowling in death overs" for comprehensive statistics!"""
    
    def ask(self, question: str) -> str:
        """Main method with enhanced query handling"""
        print(f"\nQuestion: {question}")
        print("Generating SQL query...")
        
        # Try to get SQL query from LLM
        query_code = self._get_query_from_llm(question)
        if not query_code:
            return self._try_enhanced_fallback_queries(question)
        
        # Execute the query
        result = self._execute_query(query_code)
        
        # If query failed, try enhanced fallback
        if result is None or (isinstance(result, pd.DataFrame) and len(result) == 0):
            print("Primary query failed, trying enhanced fallback...")
            return self._try_enhanced_fallback_queries(question)
        
        # Format and return result
        formatted_result = self._format_result(result, question)
        print(f"\nAnswer:\n{formatted_result}")
        return formatted_result
    
    def refresh_materialized_views(self):
        """Refresh materialized views for optimal performance"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("REFRESH MATERIALIZED VIEW IF EXISTS batting_summary;"))
                conn.execute(text("REFRESH MATERIALIZED VIEW IF EXISTS bowling_summary;"))
                conn.commit()
                print("✅ Materialized views refreshed successfully")
        except Exception as e:
            print(f"Error refreshing views: {e}")