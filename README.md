# 🏏 IPL Stats Chatbot

An intelligent chatbot that can answer complex queries about IPL statistics using natural language processing, PostgreSQL, and the Groq API.

## Features

- **Natural Language Queries**: Ask questions in plain English about IPL statistics
- **PostgreSQL Backend**: Fast and efficient data retrieval
- **Comprehensive Analysis**: Supports simple to advanced cricket analytics
- **Phase-wise Analysis**: Death overs, powerplay, middle overs analysis
- **Player Performance**: Batting vs bowling matchups, LHB vs RHB analysis
- **Match Situations**: Playoffs, finals, pressure situations
- **Groq API Integration**: Uses Groq's LLM for query understanding

## Setup

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up Environment Variables**:
   - Create a `.env` file in the project root
   - Add your database URL and Groq API key:
     ```
     # Database Configuration
     DATABASE_URL=your_postgres_connection_string
     
     # Groq API Key
     GROQ_API_KEY=your_groq_api_key
     ```

3. **Get Groq API Key**:
   - Sign up at [Groq Console](https://console.groq.com)
   - Get your API key and add it to the `.env` file

4. **Database Setup**:
   - The application requires a PostgreSQL database with the IPL data
   - The database should have the `ipl_balls` table with the required schema

## Usage

```bash
python ipl_chatbot.py
```

Enter your Groq API key when prompted, then start asking questions!

## Sample Queries

### Simple Queries
- "Who are the top 10 run scorers in IPL history?"
- "Most sixes hit by a player in a single season"
- "Best bowling figures in IPL"

### Advanced Queries
- "Best strike rate in death overs vs pace bowling"
- "How do left-handed batsmen perform in powerplay?"
- "Top wicket takers in playoffs"
- "Average runs scored per over in death overs by Mumbai Indians"
- "Best batsmen against spin bowling in middle overs"
- "Bowlers with best economy rate vs right-handed batsmen in death overs"

### Phase-wise Analysis
- "Powerplay performance by teams in 2023"
- "Death overs bowling analysis for Bumrah"
- "Middle overs batting average for Kohli"

### Match Situation Analysis
- "Performance in finals across all seasons"
- "Best performers in Super Overs"
- "Playoff performance statistics"

## Dataset Features

The chatbot can analyze:
- **80+ columns** of detailed IPL data
- **Ball-by-ball information** for all IPL matches
- **Player characteristics** (batting hand, bowling style)
- **Shot analysis** (wagon wheel, shot types)
- **Match context** (venues, teams, seasons)
- **Performance metrics** (strike rates, economy rates)

## Technical Details

- **LLM Model**: Uses Groq's Llama3-8b-8192 for query understanding
- **Data Processing**: Pandas for efficient data manipulation
- **Query Generation**: Natural language to pandas query conversion
- **Error Handling**: Comprehensive error handling for robust operation

## Architecture

1. **User Input**: Natural language question
2. **LLM Processing**: Groq API converts question to pandas query
3. **Query Execution**: Execute pandas operations on IPL dataset
4. **Result Formatting**: Format and display results in readable format

## Example Session

```
🏏 IPL Stats Chatbot
=====================================

🤔 Ask your IPL question: Who scored the most runs in death overs in 2023?

Question: Who scored the most runs in death overs in 2023?
Generating pandas query...
Generated query: df[(df['over'] >= 16) & (df['year'] == 2023)].groupby('batter')['runs_batter'].sum().sort_values(ascending=False).head(10)

Answer:
batter
Shubman Gill        245
Faf du Plessis      198
Glenn Maxwell       187
...
```

## Notes

- Ensure you have a stable internet connection for Groq API calls
- Large dataset queries might take a few seconds to process
- The chatbot handles various cricket terminology and context automatically