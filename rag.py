"""
RAG (Retrieval Augmented Generation) functionality for WhatsApp chat analysis
"""

import json
import os
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import streamlit as st
from db import connect, search


def retrieve_relevant_messages(query: str, limit: int = 10, chat_filter: Optional[str] = None) -> List[Dict]:
    """
    Retrieve relevant messages using FTS5 search for RAG context
    
    Args:
        query: User's question/query
        limit: Maximum number of messages to retrieve
        chat_filter: Optional chat name to filter results
    
    Returns:
        List of message dictionaries with context
    """
    with connect() as conn:
        # For broad questions like "what do they talk about", get recent messages
        if any(word in query.lower() for word in ['what', 'talk about', 'discuss', 'topics', 'most']):
            # Get recent messages from all chats
            results = list(conn.execute("""
                SELECT m.*, c.title as chat_title 
                FROM messages m
                JOIN chats c ON m.chat_id = c.id
                WHERE m.text IS NOT NULL AND m.text != ''
                ORDER BY m.ts DESC 
                LIMIT ?
            """, (limit * 3,)))  # Get more for better analysis
        else:
            # Use FTS5 search for specific queries
            try:
                results = search(conn, query, chat_id=None, sender=None, t1=None, t2=None, has_media=None, limit=limit)
                # Add chat titles
                for result in results:
                    chat_info = conn.execute("SELECT title FROM chats WHERE id = ?", (result['chat_id'],)).fetchone()
                    result = dict(result)  # Convert Row to dict
                    result['chat_title'] = chat_info['title'] if chat_info else 'Unknown'
            except Exception as e:
                st.error(f"Search error: {e}")
                # Fallback to recent messages
                results = list(conn.execute("""
                    SELECT m.*, c.title as chat_title 
                    FROM messages m
                    JOIN chats c ON m.chat_id = c.id
                    WHERE m.text IS NOT NULL AND m.text != ''
                    ORDER BY m.ts DESC 
                    LIMIT ?
                """, (limit,)))
        
        # Format results for RAG with additional context
        formatted_messages = []
        for result in results:
            # Convert timestamp to readable format
            timestamp = datetime.fromtimestamp(result['ts'] / 1000).isoformat()
            
            # Handle chat title from different query types
            chat_name = 'Unknown'
            if 'chat_title' in result.keys():
                chat_name = result['chat_title']
            else:
                # Get chat name for FTS search results
                chat_info = conn.execute("SELECT title FROM chats WHERE id = ?", (result['chat_id'],)).fetchone()
                chat_name = chat_info['title'] if chat_info else 'Unknown'
            
            formatted_message = {
                'content': result['text'],  # Use 'text' column from messages table
                'timestamp': timestamp,
                'chat_name': chat_name,
                'sender': result['sender'] if result['sender'] else 'System',
                'relevance_score': 0  # FTS5 doesn't provide rank by default
            }
            formatted_messages.append(formatted_message)
        
        return formatted_messages


def format_messages_for_llm(messages: List[Dict], query: str) -> str:
    """
    Format retrieved messages into a prompt for the LLM
    
    Args:
        messages: List of relevant messages
        query: Original user query
    
    Returns:
        Formatted prompt string
    """
    if not messages:
        return f"No relevant messages found for query: {query}"
    
    prompt = f"""Based on the following WhatsApp chat messages, please answer this question: {query}

CHAT MESSAGES:
"""
    
    for i, msg in enumerate(messages, 1):
        timestamp = datetime.fromisoformat(msg['timestamp']).strftime("%Y-%m-%d %H:%M")
        prompt += f"""
Message {i}:
- Chat: {msg['chat_name']}
- Sender: {msg['sender']}
- Time: {timestamp}
- Content: {msg['content']}
"""
    
    prompt += """

Please provide a helpful and accurate answer based on these messages. If the messages don't contain enough information to answer the question, please say so. Include specific details and quotes from the messages when relevant."""
    
    return prompt


def call_openai_llm(prompt: str, api_key: str, model: str = "gpt-3.5-turbo") -> str:
    """Call OpenAI API for chat completion"""
    try:
        import openai
        client = openai.OpenAI(api_key=api_key)
        
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a helpful assistant that analyzes WhatsApp chat messages to answer questions. Be accurate and cite specific messages when possible."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=1000,
            temperature=0.7
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        return f"Error calling OpenAI API: {str(e)}"


def call_anthropic_llm(prompt: str, api_key: str, model: str = "claude-3-haiku-20240307") -> str:
    """Call Anthropic Claude API for chat completion"""
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        
        response = client.messages.create(
            model=model,
            max_tokens=1000,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        return response.content[0].text
        
    except Exception as e:
        return f"Error calling Anthropic API: {str(e)}"


def call_grok_llm(prompt: str, api_key: str, model: str = "grok-3") -> str:
    """Call Grok (X.AI) API for chat completion"""
    try:
        import requests
        import json
        
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        data = {
            "messages": [
                {"role": "system", "content": "You are a helpful assistant that analyzes WhatsApp chat messages to answer questions. Be accurate and cite specific messages when possible."},
                {"role": "user", "content": prompt}
            ],
            "model": model,
            "stream": False,
            "temperature": 0.7,
            "max_tokens": 1000
        }
        
        response = requests.post(
            "https://api.x.ai/v1/chat/completions",
            headers=headers,
            json=data,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            return result["choices"][0]["message"]["content"]
        else:
            return f"Error calling Grok API: {response.status_code} - {response.text}"
            
    except Exception as e:
        return f"Error calling Grok API: {str(e)}"


def call_ollama_llm(prompt: str, model: str = "llama2", host: str = "http://localhost:11434") -> str:
    """Call local Ollama API for chat completion"""
    try:
        import ollama
        
        response = ollama.chat(
            model=model,
            messages=[
                {"role": "system", "content": "You are a helpful assistant that analyzes WhatsApp chat messages to answer questions. Be accurate and cite specific messages when possible."},
                {"role": "user", "content": prompt}
            ]
        )
        
        return response['message']['content']
        
    except Exception as e:
        return f"Error calling Ollama API: {str(e)}"


def rag_query(query: str, llm_provider: str, **llm_kwargs) -> Tuple[str, List[Dict]]:
    """
    Complete RAG query: retrieve relevant messages and generate response
    
    Args:
        query: User's question
        llm_provider: 'openai', 'anthropic', 'grok', or 'ollama'
        **llm_kwargs: Provider-specific arguments (api_key, model, etc.)
    
    Returns:
        Tuple of (LLM response, retrieved messages)
    """
    # Step 1: Retrieve relevant messages
    messages = retrieve_relevant_messages(query, limit=10)
    
    if not messages:
        return "No relevant messages found for your query.", []
    
    # Step 2: Format for LLM
    prompt = format_messages_for_llm(messages, query)
    
    # Step 3: Call appropriate LLM
    if llm_provider == "openai":
        response = call_openai_llm(prompt, **llm_kwargs)
    elif llm_provider == "anthropic":
        response = call_anthropic_llm(prompt, **llm_kwargs)
    elif llm_provider == "grok":
        response = call_grok_llm(prompt, **llm_kwargs)
    elif llm_provider == "ollama":
        response = call_ollama_llm(prompt, **llm_kwargs)
    else:
        response = f"Unsupported LLM provider: {llm_provider}"
    
    return response, messages


def get_chat_summary(chat_name: str, llm_provider: str, **llm_kwargs) -> str:
    """
    Generate a summary of a specific chat using LLM
    
    Args:
        chat_name: Name of the chat to summarize
        llm_provider: LLM provider to use
        **llm_kwargs: Provider-specific arguments
    
    Returns:
        Chat summary
    """
    with connect() as conn:
        # Get recent messages from the chat
        chat_result = conn.execute(
            "SELECT id FROM chats WHERE title = ?",
            (chat_name,)
        ).fetchone()
        
        if not chat_result:
            return f"Chat '{chat_name}' not found."
        
        chat_id = chat_result['id']
        
        # Get recent messages (last 50) using correct schema
        messages = conn.execute("""
            SELECT m.text, m.ts, m.sender
            FROM messages m
            WHERE m.chat_id = ?
            ORDER BY m.ts DESC
            LIMIT 50
        """, (chat_id,)).fetchall()
        
        if not messages:
            return f"No messages found in chat '{chat_name}'."
        
        # Format messages for summary
        prompt = f"Please provide a comprehensive summary of this WhatsApp chat '{chat_name}'. Include key topics discussed, main participants, and notable events or decisions. Here are the recent messages:\n\n"
        
        for msg in reversed(messages):  # Reverse to show chronological order
            timestamp = datetime.fromtimestamp(msg['ts'] / 1000).strftime("%Y-%m-%d %H:%M")
            sender = msg['sender'] or 'System'
            prompt += f"[{timestamp}] {sender}: {msg['text']}\n"
        
        prompt += "\n\nPlease provide a detailed summary covering the main themes, participants, and significant discussions in this chat."
        
        # Call LLM for summary
        if llm_provider == "openai":
            return call_openai_llm(prompt, **llm_kwargs)
        elif llm_provider == "anthropic":
            return call_anthropic_llm(prompt, **llm_kwargs)
        elif llm_provider == "grok":
            return call_grok_llm(prompt, **llm_kwargs)
        elif llm_provider == "ollama":
            return call_ollama_llm(prompt, **llm_kwargs)
        else:
            return f"Unsupported LLM provider: {llm_provider}"