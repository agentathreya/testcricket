from flask import Flask, render_template, request, jsonify
from ipl_chatbot_enhanced import IPLStatsEnhancedChatbot
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# Initialize the Enhanced PostgreSQL chatbot
try:
    DATABASE_URL = os.getenv('DATABASE_URL')
    GROQ_API_KEY = os.getenv('GROQ_API_KEY')
    
    if not DATABASE_URL or not GROQ_API_KEY:
        raise ValueError("Missing required environment variables: DATABASE_URL and GROQ_API_KEY must be set in .env file")
        
    chatbot = IPLStatsEnhancedChatbot(DATABASE_URL, GROQ_API_KEY)
    print("✅ Enhanced PostgreSQL Chatbot initialized successfully!")
except Exception as e:
    print(f"❌ Error initializing Enhanced PostgreSQL chatbot: {e}")
    chatbot = None

@app.route('/')
def index():
    return render_template('fullscreen_ui.html')

@app.route('/ask', methods=['POST'])
def ask_question():
    if not chatbot:
        return jsonify({
            'error': 'PostgreSQL chatbot not initialized. Please check the database connection.'
        }), 500
    
    try:
        data = request.get_json()
        question = data.get('question', '').strip()
        
        if not question:
            return jsonify({'error': 'Please enter a valid question.'}), 400
        
        # Get answer from PostgreSQL chatbot
        answer = chatbot.ask(question)
        
        return jsonify({
            'question': question,
            'answer': answer
        })
        
    except Exception as e:
        return jsonify({
            'error': f'Error processing question: {str(e)}'
        }), 500

@app.route('/refresh', methods=['POST'])
def refresh_views():
    """Endpoint to refresh materialized views"""
    if not chatbot:
        return jsonify({'error': 'Chatbot not initialized.'}), 500
    
    try:
        chatbot.refresh_materialized_views()
        return jsonify({'message': 'Materialized views refreshed successfully!'})
    except Exception as e:
        return jsonify({'error': f'Error refreshing views: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)