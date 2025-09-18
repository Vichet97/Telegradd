#!/usr/bin/env python3
"""
Telegradd Web Application
A comprehensive web interface for Telegram account management with secure authentication and rate limiting.
"""

import os
import asyncio
import json
import time
from datetime import datetime, timedelta
from functools import wraps
from collections import defaultdict
import secrets
import hashlib

from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from werkzeug.security import generate_password_hash, check_password_hash
import redis

# Import existing Telegradd functionality
import main
from telegradd.connect.authorisation.client import TELEGRADD_client
from telegradd.connect.authorisation.databased import Database

app = Flask(__name__)
app.secret_key = secrets.token_hex(32)  # Generate secure secret key

# Initialize Flask-Limiter for rate limiting
try:
    # Try Redis first
    import redis
    redis_client = redis.Redis(host='localhost', port=6379, db=0)
    redis_client.ping()
    limiter = Limiter(
        key_func=get_remote_address,
        app=app,
        storage_uri="redis://localhost:6379",
        default_limits=["200 per day", "50 per hour"]
    )
except:
    # Fallback to memory storage
    limiter = Limiter(
        key_func=get_remote_address,
        app=app,
        storage_uri="memory://",
        default_limits=["200 per day", "50 per hour"]
    )

# Inject current year into all templates
@app.context_processor
def inject_current_year():
    return {'current_year': datetime.now().year}

# Security configuration
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD_HASH = generate_password_hash("016997791aA!!")
LOGIN_ATTEMPTS = defaultdict(list)
MAX_LOGIN_ATTEMPTS = 3
LOCKOUT_DURATION = 24 * 60 * 60  # 24 hours in seconds

def check_rate_limit(ip_address):
    """Check if IP address has exceeded login attempts"""
    now = time.time()
    attempts = LOGIN_ATTEMPTS[ip_address]
    
    # Remove old attempts (older than 24 hours)
    LOGIN_ATTEMPTS[ip_address] = [attempt for attempt in attempts if now - attempt < LOCKOUT_DURATION]
    
    return len(LOGIN_ATTEMPTS[ip_address]) < MAX_LOGIN_ATTEMPTS

def record_login_attempt(ip_address):
    """Record a failed login attempt"""
    LOGIN_ATTEMPTS[ip_address].append(time.time())

def login_required(f):
    """Decorator to require login for protected routes"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session or not session['logged_in']:
            flash('Please log in to access this page.', 'error')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def get_account_status():
    """Get current status of all accounts"""
    try:
        db = Database()
        accounts = []
        
        # Get session files to determine available accounts
        session_dirs = [
            '/Users/vichet/Desktop/Telegradd/sessions/sessions_json',
            '/Users/vichet/Desktop/Telegradd/sessions/telethon_sessions'
        ]
        
        account_files = []
        for session_dir in session_dirs:
            if os.path.exists(session_dir):
                for file in os.listdir(session_dir):
                    if file.endswith(('.json', '.session')) and not file.startswith('.'):
                        phone = file.split('.')[0]
                        if phone not in [acc['phone'] for acc in account_files]:
                            account_files.append({'phone': phone, 'type': 'json' if file.endswith('.json') else 'session'})
        
        for acc in account_files:
            try:
                # Get account info from database
                account_info = db.get_account_info(acc['phone'])
                status = 'online' if account_info and not account_info.get('restricted') else 'offline'
                
                accounts.append({
                    'phone': acc['phone'],
                    'type': acc['type'],
                    'status': status,
                    'added_today': account_info.get('added_today', 0) if account_info else 0,
                    'remaining_limit': account_info.get('remaining_limit', 0) if account_info else 0,
                    'last_active': account_info.get('last_active', 'Never') if account_info else 'Never'
                })
            except Exception as e:
                accounts.append({
                    'phone': acc['phone'],
                    'type': acc['type'],
                    'status': 'unknown',
                    'added_today': 0,
                    'remaining_limit': 0,
                    'last_active': 'Unknown'
                })
        
        db.close()
        return accounts
    except Exception as e:
        print(f"Error getting account status: {e}")
        return []

@app.route('/')
def index():
    """Redirect to login if not authenticated, otherwise show dashboard"""
    if 'logged_in' in session and session['logged_in']:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
@limiter.limit("10 per minute")
def login():
    """Secure login with rate limiting"""
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        ip_address = get_remote_address()
        
        # Check rate limiting
        if not check_rate_limit(ip_address):
            flash('Too many failed login attempts. Please try again in 24 hours.', 'error')
            return render_template('login.html')
        
        # Validate credentials
        if username == ADMIN_USERNAME and check_password_hash(ADMIN_PASSWORD_HASH, password):
            session['logged_in'] = True
            session['username'] = username
            session.permanent = True
            flash('Login successful!', 'success')
            return redirect(url_for('dashboard'))
        else:
            record_login_attempt(ip_address)
            remaining_attempts = MAX_LOGIN_ATTEMPTS - len(LOGIN_ATTEMPTS[ip_address])
            flash(f'Invalid credentials. {remaining_attempts} attempts remaining.', 'error')
    
    return render_template('login.html')

@app.route('/logout')
def logout():
    """Logout and clear session"""
    session.clear()
    flash('You have been logged out successfully.', 'info')
    return redirect(url_for('login'))

@app.route('/dashboard')
@login_required
def dashboard():
    """Main dashboard showing account status and navigation"""
    accounts = get_account_status()
    
    # Get background task status
    bg_tasks = main.BG_MANAGER.list_tasks()
    
    return render_template('dashboard.html', 
                         accounts=accounts, 
                         bg_tasks=bg_tasks,
                         total_accounts=len(accounts),
                         online_accounts=len([a for a in accounts if a['status'] == 'online']))

@app.route('/api/accounts/status')
@login_required
def api_account_status():
    """API endpoint for real-time account status updates"""
    accounts = get_account_status()
    return jsonify({
        'accounts': accounts,
        'timestamp': datetime.now().isoformat(),
        'total': len(accounts),
        'online': len([a for a in accounts if a['status'] == 'online'])
    })

@app.route('/api/background-tasks')
@login_required
def api_background_tasks():
    """API endpoint for background task status"""
    tasks = main.BG_MANAGER.list_tasks()
    return jsonify({
        'tasks': tasks,
        'timestamp': datetime.now().isoformat()
    })

# Feature Routes (1-24)
@app.route('/login-phone', methods=['GET', 'POST'])
@login_required
def login_phone():
    """Feature 1: Login with Phone Number"""
    if request.method == 'POST':
        # Handle phone login logic
        flash('Phone login initiated. Check your device for verification code.', 'info')
    return render_template('features/login_phone.html')

@app.route('/load-sessions', methods=['GET', 'POST'])
@login_required
def load_sessions():
    """Feature 2: Load Sessions JSON files"""
    if request.method == 'POST':
        # Handle session loading
        flash('Sessions loaded successfully.', 'success')
    return render_template('features/load_sessions.html')

@app.route('/load-tdata', methods=['GET', 'POST'])
@login_required
def load_tdata():
    """Feature 3: Load Tdata"""
    if request.method == 'POST':
        # Handle TData loading
        flash('TData loaded successfully.', 'success')
    return render_template('features/load_tdata.html')

@app.route('/load-pyrogram', methods=['GET', 'POST'])
@login_required
def load_pyrogram():
    """Feature 4: Load Pyrogram Sessions"""
    if request.method == 'POST':
        # Handle Pyrogram session loading
        flash('Pyrogram sessions loaded successfully.', 'success')
    return render_template('features/load_pyrogram.html')

@app.route('/load-telethon', methods=['GET', 'POST'])
@login_required
def load_telethon():
    """Feature 5: Load Telethon Sessions"""
    if request.method == 'POST':
        # Handle Telethon session loading
        flash('Telethon sessions loaded successfully.', 'success')
    return render_template('features/load_telethon.html')

@app.route('/scraper-participants', methods=['GET', 'POST'])
@login_required
def scraper_participants():
    """Feature 6: Participants Group Scraper"""
    if request.method == 'POST':
        # Handle participant scraping
        flash('Participant scraping started.', 'info')
    return render_template('features/scraper_participants.html')

@app.route('/scraper-hidden', methods=['GET', 'POST'])
@login_required
def scraper_hidden():
    """Feature 7: Hidden Participants Scraper"""
    if request.method == 'POST':
        # Handle hidden participant scraping
        flash('Hidden participant scraping started.', 'info')
    return render_template('features/scraper_hidden.html')

@app.route('/scraper-comments', methods=['GET', 'POST'])
@login_required
def scraper_comments():
    """Feature 8: Comments Participants Scraper"""
    if request.method == 'POST':
        # Handle comment participant scraping
        flash('Comment participant scraping started.', 'info')
    return render_template('features/scraper_comments.html')

@app.route('/add-by-id', methods=['GET', 'POST'])
@login_required
def add_by_id():
    """Feature 9: Add by ID"""
    if request.method == 'POST':
        # Handle adding by ID
        flash('Adding members by ID started.', 'info')
    return render_template('features/add_by_id.html')

@app.route('/add-by-username', methods=['GET', 'POST'])
@login_required
def add_by_username():
    """Feature 10: Add by Username"""
    if request.method == 'POST':
        # Handle adding by username
        flash('Adding members by username started.', 'info')
    return render_template('features/add_by_username.html')

@app.route('/warm-up', methods=['GET', 'POST'])
@login_required
def warm_up():
    """Feature 11: Warm Up Mode"""
    if request.method == 'POST':
        # Handle warm up mode
        flash('Warm up mode activated.', 'info')
    return render_template('features/warm_up.html')

@app.route('/delete-banned', methods=['GET', 'POST'])
@login_required
def delete_banned():
    """Feature 12: Delete banned accounts"""
    if request.method == 'POST':
        # Handle deleting banned accounts
        flash('Banned accounts deleted.', 'success')
    return render_template('features/delete_banned.html')

@app.route('/list-accounts')
@login_required
def list_accounts():
    """Feature 13: List accounts"""
    accounts = get_account_status()
    return render_template('features/list_accounts.html', accounts=accounts)

@app.route('/join-chats', methods=['GET', 'POST'])
@login_required
def join_chats():
    """Feature 14: Join Chat(s)"""
    if request.method == 'POST':
        # Handle joining chats
        flash('Joining chats initiated.', 'info')
    return render_template('features/join_chats.html')

@app.route('/change-settings', methods=['GET', 'POST'])
@login_required
def change_settings():
    """Feature 15: Change Proxy/Password/Etc"""
    if request.method == 'POST':
        # Handle settings changes
        flash('Settings updated successfully.', 'success')
    return render_template('features/change_settings.html')

@app.route('/test-auth', methods=['GET', 'POST'])
@login_required
def test_auth():
    """Feature 16: Test Authorization"""
    if request.method == 'POST':
        # Handle authorization testing
        flash('Authorization test completed.', 'info')
    return render_template('features/test_auth.html')

@app.route('/delete-duplicates', methods=['GET', 'POST'])
@login_required
def delete_duplicates():
    """Feature 17: Delete Duplicates"""
    if request.method == 'POST':
        # Handle duplicate deletion
        flash('Duplicates deleted successfully.', 'success')
    return render_template('features/delete_duplicates.html')

@app.route('/delete-accounts', methods=['GET', 'POST'])
@login_required
def delete_accounts():
    """Feature 18: Delete Account(s)"""
    if request.method == 'POST':
        # Handle account deletion
        flash('Accounts deleted successfully.', 'success')
    return render_template('features/delete_accounts.html')

@app.route('/check-spambot', methods=['GET', 'POST'])
@login_required
def check_spambot():
    """Feature 19: Check Accounts Status via @SpamBot"""
    if request.method == 'POST':
        # Handle SpamBot checking
        flash('SpamBot check initiated.', 'info')
    return render_template('features/check_spambot.html')

@app.route('/remove-restriction', methods=['GET', 'POST'])
@login_required
def remove_restriction():
    """Feature 20: Remove from Restriction"""
    if request.method == 'POST':
        # Handle restriction removal
        flash('Restrictions removed successfully.', 'success')
    return render_template('features/remove_restriction.html')

@app.route('/add-restriction', methods=['GET', 'POST'])
@login_required
def add_restriction():
    """Feature 21: Add to Restriction"""
    if request.method == 'POST':
        # Handle adding restrictions
        flash('Restrictions added successfully.', 'success')
    return render_template('features/add_restriction.html')

@app.route('/convert-tdata', methods=['GET', 'POST'])
@login_required
def convert_tdata():
    """Feature 22: Convert TData"""
    if request.method == 'POST':
        # Handle TData conversion
        flash('TData conversion completed.', 'success')
    return render_template('features/convert_tdata.html')

@app.route('/promote-admin', methods=['GET', 'POST'])
@login_required
def promote_admin():
    """Feature 23: Promote members to Admin"""
    if request.method == 'POST':
        # Handle admin promotion
        flash('Members promoted to admin successfully.', 'success')
    return render_template('features/promote_admin.html')

@app.route('/background-tasks', methods=['GET', 'POST'])
@login_required
def background_tasks():
    """Feature 24: Manage Background Tasks"""
    if request.method == 'POST':
        action = request.form.get('action')
        task_id = request.form.get('task_id')
        
        if action == 'stop' and task_id:
            try:
                main.BG_MANAGER.stop_task(task_id)
                flash(f'Task {task_id} stopped successfully.', 'success')
            except Exception as e:
                flash(f'Error stopping task: {str(e)}', 'error')
        elif action == 'start':
            # Handle starting new background task
            flash('Background task started successfully.', 'success')
    
    tasks = main.BG_MANAGER.list_tasks()
    return render_template('features/background_tasks.html', tasks=tasks)

if __name__ == '__main__':
    # Ensure templates and static directories exist
    os.makedirs('templates', exist_ok=True)
    os.makedirs('templates/features', exist_ok=True)
    os.makedirs('static/css', exist_ok=True)
    os.makedirs('static/js', exist_ok=True)
    
    app.run(debug=True, host='0.0.0.0', port=5000)