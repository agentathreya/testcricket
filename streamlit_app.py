import streamlit as st
from ipl_chatbot_postgres import IPLStatsPostgresChatbot
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set page config
st.set_page_config(
    page_title="IPL Stats Chatbot",
    page_icon="🏏",
    layout="wide"
)

# Initialize session state
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

# Initialize chatbot
try:
    chatbot = IPLStatsPostgresChatbot(
        os.getenv('DATABASE_URL'),
        os.getenv('GROQ_API_KEY')
    )
    st.session_state.chatbot = chatbot
except Exception as e:
    st.error(f"Error initializing chatbot: {e}")
    st.stop()

# Sidebar with info
with st.sidebar:
    st.title("🏏 IPL Stats Chatbot")
    st.write("Ask me anything about IPL statistics!")
    
    # Example queries
    st.subheader("Example Queries:")
    example_queries = [
        "Who are the top 10 run scorers in IPL history?",
        "Best batters vs pace bowling in death overs",
        "Top wicket takers in IPL 2024",
        "Strike rate in death overs"
    ]
    
    for query in example_queries:
        if st.button(query, key=query, use_container_width=True):
            st.session_state.user_input = query

# Main chat interface
st.title("IPL Stats Chatbot")
st.write("Ask me anything about IPL statistics!")

# Display chat history
for message in st.session_state.chat_history:
    with st.chat_message(message["role"]):
        st.write(message["content"])

# User input
if prompt := st.chat_input("Ask me about IPL stats..."):
    # Add user message to chat history
    st.session_state.chat_history.append({"role": "user", "content": prompt})
    
    # Display user message
    with st.chat_message("user"):
        st.write(prompt)
    
    # Get bot response
    with st.spinner("Analyzing..."):
        try:
            response = chatbot.ask(prompt)
            # Add bot response to chat history
            st.session_state.chat_history.append({"role": "assistant", "content": response})
            # Display bot response
            with st.chat_message("assistant"):
                st.write(response)
        except Exception as e:
            error_msg = f"Sorry, I encountered an error: {str(e)}"
            st.session_state.chat_history.append({"role": "assistant", "content": error_msg})
            with st.chat_message("assistant"):
                st.error(error_msg)
