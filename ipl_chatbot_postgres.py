import pandas as pd
from groq import Groq
from sqlalchemy import create_engine, text
import time
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class IPLStatsPostgresChatbot:
    def __init__(self, database_url: str, groq_api_key: str):
        """Initialize the IPL Stats Chatbot with PostgreSQL backend"""
        self.database_url = database_url
        self.client = Groq(api_key=groq_api_key)
        self.engine = None
        self._connect_database()
        self._create_data_summary()
    
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
    
    def _create_data_summary(self):
        """Create a summary of the database schema for the LLM"""
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
                        MIN(season) as first_season,
                        MAX(season) as last_season
                    FROM ipl_balls
                """)).fetchone()
                
                # Get teams
                teams = conn.execute(text("""
                    SELECT DISTINCT batting_team 
                    FROM ipl_balls 
                    WHERE batting_team != '' 
                    ORDER BY batting_team
                """)).fetchall()
                
                team_list = [team[0] for team in teams]
        except Exception as e:
            print(f"Error getting database stats: {e}")
            # Fallback values
            stats = (277935, 17, 1000, '2008-04-18', '2024-05-26', 2008, 2024)
            team_list = ['CSK', 'MI', 'RCB', 'KKR', 'SRH', 'DC', 'PBKS', 'RR', 'GT', 'LSG']

        self.data_summary = f"""
PostgreSQL IPL Database Schema:
- Total records: {stats[0]:,}
- Seasons: {stats[5]}-{stats[6]} ({stats[1]} seasons)
- Total matches: {stats[2]:,}
- Date range: {stats[3]} to {stats[4]}
- Teams: {team_list}

Table: ipl_balls (using actual CSV column names with quotes)
Key Columns for Analysis:
- Match Info: "season", "year", "date", "venue", "match_id", "batting_team", "bowling_team", "innings"
- Ball Details: "over", "ball", "ball_no", "runs_batter", "runs_total", "isFour", "isSix", "isWicket"
- Players: "batter", "bowler", "non_striker", "bat_hand", "bowling_style", "batting_captain", "bowling_captain"
- Performance: "team_runs", "team_wickets", "curr_batter_runs", "curr_batter_balls", "curr_batter_fours", "curr_batter_sixes"
- Context: "batting_partners", "Required RR", "Current RR", "winProbabilty", "predictedScore"
- Shot Analysis: "shotType", "shotControl", "wagonX", "wagonY", "wagonZone"
- Match Results: "winner", "toss_winner", "toss_decision", "playerofmatch", "playerofseries"

Phase Definitions:
- Powerplay: "over" <= 6
- Middle overs: "over" >= 7 AND "over" <= 15
- Death overs: "over" >= 16
- Super overs: "isSuperOver" = TRUE

Materialized Views Available (for faster queries):
- mv_top_run_scorers: Pre-aggregated batting statistics
- mv_top_bowlers: Pre-aggregated bowling statistics  
- mv_death_overs_batters: Death overs batting performance

Performance Tips:
- Use materialized views for common aggregations
- Indexes available on: "season", "batter", "bowler", "over", "batting_team", "bowling_style", etc.
- Boolean columns: "isFour", "isSix", "isWicket", "isSuperOver"
- IMPORTANT: All column names must be quoted because they are case-sensitive
"""

    def _get_query_from_llm(self, user_question: str) -> str:
        """Use Groq LLM to convert natural language to SQL query"""
        
        prompt = f"""
You are an expert in SQL and IPL cricket analytics. 
Convert the user's natural language question into a PostgreSQL SQL query.

Database Info:
{self.data_summary}

CRITICAL INSTRUCTIONS:
1. Use PostgreSQL syntax
2. Table name is 'ipl_balls' 
3. ALL column names MUST be quoted because they're case-sensitive: "batter", "over", "runs_batter", etc.
4. Use materialized views when possible for better performance:
   - mv_top_run_scorers for overall batting stats
   - mv_top_bowlers for bowling stats
   - mv_death_overs_batters for death overs batting
5. For pace bowling: WHERE "bowling_style" LIKE '%rm%' OR "bowling_style" LIKE '%rfm%' OR "bowling_style" LIKE '%lfm%'
6. For spin bowling: WHERE "bowling_style" NOT LIKE '%rm%' AND "bowling_style" NOT LIKE '%rfm%' AND "bowling_style" NOT LIKE '%lfm%'
7. Boolean columns: "isFour", "isSix", "isWicket", "isSuperOver"
8. Always use LIMIT for rankings (10-20 results)
9. Use proper aggregations: SUM(), COUNT(), AVG(), etc.
10. Sort results with ORDER BY for rankings

EXAMPLES:
- "top run scorers": SELECT * FROM mv_top_run_scorers LIMIT 10;
- "best batters vs pace in death overs": 
  SELECT "batter", SUM("runs_batter") as runs FROM ipl_balls 
  WHERE "over" >= 16 AND ("bowling_style" LIKE '%rm%' OR "bowling_style" LIKE '%rfm%' OR "bowling_style" LIKE '%lfm%') 
  GROUP BY "batter" ORDER BY runs DESC LIMIT 10;
- "top wicket takers": SELECT * FROM mv_top_bowlers LIMIT 10;
- "highest run scorers in 2024": 
  SELECT "batter", SUM("runs_batter") as total_runs FROM ipl_balls 
  WHERE "year" = 2024 GROUP BY "batter" ORDER BY total_runs DESC LIMIT 10;

User Question: {user_question}

Return ONLY the SQL query:
"""

        try:
            response = self.client.chat.completions.create(
                model="llama3-8b-8192",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=500
            )
            
            query_code = response.choices[0].message.content.strip()
            
            # Clean up the response
            if "```sql" in query_code:
                query_code = query_code.split("```sql")[1].split("```")[0].strip()
            elif "```" in query_code:
                query_code = query_code.split("```")[1].strip()
            
            # Remove any trailing semicolons and clean
            query_code = query_code.rstrip(';').strip()
            
            return query_code
            
        except Exception as e:
            print(f"Error getting query from LLM: {e}")
            return None
    
    def _execute_query(self, query_code: str):
        """Execute SQL query and return results as DataFrame"""
        try:
            print(f"Executing query: {query_code}")
            start_time = time.time()
            
            # Execute query and return DataFrame
            result_df = pd.read_sql(query_code, self.engine)
            
            execution_time = time.time() - start_time
            print(f"Query executed in {execution_time:.2f}s, returned {len(result_df)} rows")
            
            return result_df
            
        except Exception as e:
            print(f"Error executing query: {e}")
            print(f"Query: {query_code}")
            return None
    
    def _format_result(self, result, user_question: str) -> str:
        """Format the query result into a readable response"""
        if result is None:
            return "Sorry, I couldn't process your query. Please try rephrasing your question."
        
        if isinstance(result, pd.DataFrame) and len(result) == 0:
            return "No data found matching your criteria."
        
        try:
            question_lower = user_question.lower()
            
            if isinstance(result, pd.DataFrame):
                return self._format_dataframe(result, question_lower)
            else:
                return str(result)
                
        except Exception as e:
            return f"Error formatting result: {e}"
    
    def _format_dataframe(self, df, question_lower):
        """Format DataFrame results with cricket context"""
        try:
            # Determine what we're showing
            if any(word in question_lower for word in ['run', 'scorer', 'batting']):
                metric_name = "runs"
            elif any(word in question_lower for word in ['wicket', 'bowler']):
                metric_name = "wickets"
            elif 'strike rate' in question_lower:
                metric_name = "strike rate"
            else:
                metric_name = "points"
            
            formatted = f"🏏 **Top {min(len(df), 15)} Results:**\n\n"
            
            for i, (_, row) in enumerate(df.head(15).iterrows(), 1):
                # Medal emojis for top 3
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i:2d}."
                
                # Get player name
                name = row.get('batter', row.get('bowler', str(row.iloc[0])))
                
                # Get main statistic (usually the numeric column with highest values)
                main_stat = None
                stat_name = metric_name
                
                # Look for key columns
                if 'total_runs' in df.columns:
                    main_stat = row['total_runs']
                    stat_name = "runs"
                elif 'runs' in df.columns:
                    main_stat = row['runs']
                    stat_name = "runs"
                elif 'wickets' in df.columns:
                    main_stat = row['wickets']
                    stat_name = "wickets"
                elif 'death_runs' in df.columns:
                    main_stat = row['death_runs']
                    stat_name = "death runs"
                elif 'strike_rate' in df.columns or 'death_sr' in df.columns:
                    main_stat = row.get('strike_rate', row.get('death_sr'))
                    stat_name = "SR"
                else:
                    # Get the first numeric column
                    numeric_cols = df.select_dtypes(include=['number']).columns
                    if len(numeric_cols) > 0:
                        main_stat = row[numeric_cols[0]]
                
                if main_stat is not None:
                    if isinstance(main_stat, float):
                        if stat_name == "SR":
                            value_str = f"{main_stat:.2f}"
                        else:
                            value_str = f"{main_stat:,.0f}" if main_stat >= 100 else f"{main_stat:.1f}"
                    else:
                        value_str = f"{main_stat:,}"
                    
                    formatted += f"{medal} **{name}** - {value_str} {stat_name}\n"
                else:
                    formatted += f"{medal} **{name}**\n"
            
            if len(df) > 15:
                formatted += f"\n... and {len(df) - 15} more results"
            
            return formatted
            
        except Exception as e:
            return f"Error formatting results: {e}"
    
    def _try_fallback_queries(self, question: str) -> str:
        """Try predefined SQL queries for common questions"""
        question_lower = question.lower()
        
        try:
            # Common query patterns with optimized SQL using proper column names
            if any(word in question_lower for word in ['top', 'best', 'highest']) and any(word in question_lower for word in ['run', 'scorer']):
                query = 'SELECT "batter", SUM("runs_batter") as total_runs FROM ipl_balls WHERE "batter" != \'\' GROUP BY "batter" ORDER BY total_runs DESC LIMIT 10'
                result = self._execute_query(query)
                return self._format_result(result, question)
            
            elif any(word in question_lower for word in ['wicket', 'bowler']) and 'top' in question_lower:
                query = 'SELECT "bowler", COUNT(*) as wickets FROM ipl_balls WHERE "isWicket" = TRUE AND "bowler" != \'\' GROUP BY "bowler" ORDER BY wickets DESC LIMIT 10'
                result = self._execute_query(query)
                return self._format_result(result, question)
            
            elif 'death over' in question_lower and 'pace' in question_lower:
                query = '''
                SELECT "batter", SUM("runs_batter") as runs 
                FROM ipl_balls 
                WHERE "over" >= 16 AND ("bowling_style" LIKE '%rm%' OR "bowling_style" LIKE '%rfm%' OR "bowling_style" LIKE '%lfm%')
                AND "batter" != ''
                GROUP BY "batter" 
                HAVING SUM("runs_batter") > 50
                ORDER BY runs DESC 
                LIMIT 10
                '''
                result = self._execute_query(query)
                return self._format_result(result, question)
            
            elif 'death over' in question_lower and 'strike rate' in question_lower:
                query = '''
                SELECT "batter", 
                       SUM("runs_batter") as death_runs,
                       COUNT(*) as death_balls,
                       ROUND((SUM("runs_batter")::numeric / NULLIF(COUNT(*), 0)) * 100, 2) as death_sr
                FROM ipl_balls 
                WHERE "over" >= 16 AND "batter" != ''
                GROUP BY "batter"
                HAVING COUNT(*) >= 30
                ORDER BY death_sr DESC 
                LIMIT 10
                '''
                result = self._execute_query(query)
                return self._format_result(result, question)
            
            else:
                return "I couldn't understand your question. Please try asking about:\n\n" + \
                       "• Top run scorers or wicket takers\n" + \
                       "• Performance in death overs, powerplay, or middle overs\n" + \
                       "• Strike rates or averages\n" + \
                       "• Specific seasons (2008-2024)\n" + \
                       "• Performance vs pace or spin bowling\n" + \
                       "• Team-specific statistics\n\n" + \
                       "Example: 'Who are the top 10 run scorers in IPL history?'"
                       
        except Exception as e:
            return f"Sorry, I encountered an error while processing your question: {str(e)}"
    
    def ask(self, question: str) -> str:
        """Main method to ask questions about IPL stats"""
        print(f"\nQuestion: {question}")
        print("Generating SQL query...")
        
        # Try to get SQL query from LLM
        query_code = self._get_query_from_llm(question)
        if not query_code:
            return self._try_fallback_queries(question)
        
        # Execute the query
        result = self._execute_query(query_code)
        
        # If query failed, try fallback
        if result is None:
            return self._try_fallback_queries(question)
        
        # Format and return the result
        formatted_result = self._format_result(result, question)
        print(f"\nAnswer:\n{formatted_result}")
        return formatted_result
    
    def refresh_materialized_views(self):
        """Refresh materialized views for updated data"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("REFRESH MATERIALIZED VIEW mv_top_run_scorers"))
                conn.execute(text("REFRESH MATERIALIZED VIEW mv_top_bowlers"))
                conn.execute(text("REFRESH MATERIALIZED VIEW mv_death_overs_batters"))
                conn.commit()
            print("✅ Materialized views refreshed!")
        except Exception as e:
            print(f"Error refreshing views: {e}")

def main():
    # Initialize the chatbot
    print("🏏 IPL Stats PostgreSQL Chatbot")
    print("=" * 50)
    
    # Get configuration from environment variables
    DATABASE_URL = os.getenv('DATABASE_URL')
    GROQ_API_KEY = os.getenv('GROQ_API_KEY')
    
    if not DATABASE_URL or not GROQ_API_KEY:
        print("❌ Error: Missing required environment variables")
        print("Please create a .env file with DATABASE_URL and GROQ_API_KEY")
        return
    
    # Initialize chatbot
    try:
        chatbot = IPLStatsPostgresChatbot(DATABASE_URL, GROQ_API_KEY)
        print("✅ PostgreSQL Chatbot initialized successfully!")
    except Exception as e:
        print(f"❌ Error initializing chatbot: {e}")
        return
    
    print("\nPostgreSQL-powered queries - Much faster performance! 🚀")
    print("You can ask questions like:")
    print("- Who are the top 10 run scorers in IPL history?")
    print("- Best batters vs pace bowling in death overs")
    print("- Top wicket takers in IPL 2024")
    print("- Strike rate in death overs")
    print("\nType 'quit' to exit\n")
    
    # Main chat loop
    while True:
        try:
            question = input("\n🤔 Ask your IPL question: ").strip()
            
            if question.lower() in ['quit', 'exit', 'bye']:
                print("👋 Thanks for using IPL Stats PostgreSQL Chatbot!")
                break
            
            if not question:
                print("Please enter a valid question.")
                continue
            
            # Get answer
            chatbot.ask(question)
            
        except KeyboardInterrupt:
            print("\n👋 Thanks for using IPL Stats PostgreSQL Chatbot!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()