"""
Netflix–WBD Transaction Monitor v1.0
Enterprise-grade monitoring dashboard for small team use (~10 users)
"""

import streamlit as st
import requests
from datetime import datetime, timedelta
import google.generativeai as genai
import json
import gspread
from zoneinfo import ZoneInfo
import re
from difflib import SequenceMatcher
import html
from typing import List, Optional, Dict, Any, Tuple
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

# Pydantic for structured outputs
from pydantic import BaseModel, Field

# Robust date parsing
try:
    from dateutil import parser as dateutil_parser
    DATEUTIL_AVAILABLE = True
except ImportError:
    DATEUTIL_AVAILABLE = False

# Authentication (optional)
try:
    import streamlit_authenticator as stauth
    STAUTH_AVAILABLE = True
except ImportError:
    STAUTH_AVAILABLE = False


# ============================================================================
# SECRETS HELPER - supports both Streamlit secrets and env vars (for Render)
# ============================================================================

def get_secret(key: str, default: Any = None) -> Any:
    """Get secret from Streamlit secrets or environment variable."""
    # Try Streamlit secrets first
    try:
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    # Fall back to environment variable
    return os.environ.get(key, default)

def get_gcp_credentials() -> Optional[Dict[str, Any]]:
    """Get GCP service account credentials from secrets or env."""
    # Try Streamlit secrets first
    try:
        if "gcp_service_account" in st.secrets:
            return dict(st.secrets["gcp_service_account"])
    except Exception:
        pass
    
    # Try JSON string from environment variable
    gcp_json = os.environ.get("GCP_SERVICE_ACCOUNT")
    if gcp_json:
        try:
            return json.loads(gcp_json)
        except Exception:
            pass
    
    # Try individual environment variables (for Render/Docker)
    private_key = os.environ.get("GCP_PRIVATE_KEY")
    client_email = os.environ.get("GCP_CLIENT_EMAIL")
    
    if private_key and client_email:
        # Replace escaped newlines with actual newlines
        private_key = private_key.replace("\\n", "\n")
        return {
            "type": "service_account",
            "project_id": os.environ.get("GCP_PROJECT_ID", "transaction-monitor-483311"),
            "private_key_id": os.environ.get("GCP_PRIVATE_KEY_ID", ""),
            "private_key": private_key,
            "client_email": client_email,
            "client_id": os.environ.get("GCP_CLIENT_ID", ""),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{client_email.replace('@', '%40')}"
        }
    
    return None

# ============================================================================
# CONFIGURATION
# ============================================================================

APP_VERSION = "1.0"
DEBUG_MODE = False  # Set to True for verbose date parsing warnings

# Cache TTL settings (seconds)
CACHE_TTL_DATA = 3600      # 1 hour for main data
CACHE_TTL_SEC = 3600       # 1 hour for SEC filings
CACHE_TTL_SUMMARY = 1800   # 30 minutes for AI summary

# API timeout settings (seconds)
API_TIMEOUT_DEFAULT = 15
API_TIMEOUT_SEC = 10
API_TIMEOUT_SHEETS = 20

# Summary generation limits
MAX_ARTICLES_FOR_SUMMARY = 15
MAX_DESCRIPTION_LENGTH = 500

# ============================================================================
# AUTHENTICATION CONFIGURATION
# ============================================================================

def get_authenticator():
    """Initialize and return the authenticator object."""
    if not STAUTH_AVAILABLE:
        return None
    try:
        # Load credentials from secrets or env vars
        auth_creds = get_secret("auth_credentials")
        credentials = {
            'usernames': auth_creds if auth_creds else {
                'admin': {
                    'email': 'admin@example.com',
                    'name': 'Admin User',
                    'password': '$2b$12$PLACEHOLDER'  # bcrypt hashed
                }
            }
        }
        
        cookie_config = {
            'expiry_days': get_secret("auth_cookie_expiry", 7),
            'key': get_secret("auth_cookie_key", "netflix_wbd_monitor_key"),
            'name': get_secret("auth_cookie_name", "netflix_wbd_auth")
        }
        
        authenticator = stauth.Authenticate(
            credentials,
            cookie_config['name'],
            cookie_config['key'],
            cookie_config['expiry_days']
        )
        return authenticator
    except Exception:
        # If auth not configured, return None (bypass auth)
        return None

# ============================================================================
# PAGE CONFIGURATION (must be first Streamlit command)
# ============================================================================

st.set_page_config(
    page_title="Netflix–WBD Transaction Monitor",
    layout="wide",
    page_icon="⚖️",
    initial_sidebar_state="collapsed"
)

# ============================================================================
# AUTHENTICATION CHECK
# ============================================================================

# Check if authentication is enabled
AUTH_ENABLED = get_secret("auth_enabled", False)

if AUTH_ENABLED:
    authenticator = get_authenticator()
    if authenticator:
        try:
            name, authentication_status, username = authenticator.login('main')
            
            if authentication_status is False:
                st.error('Username/password is incorrect')
                st.stop()
            elif authentication_status is None:
                st.warning('Please enter your username and password')
                st.stop()
            # If authenticated, continue with the app
        except Exception as e:
            st.error(f"Authentication error: {html.escape(str(e))}")
            st.stop()

# ============================================================================
# SESSION STATE INITIALIZATION
# ============================================================================

if 'cache_invalidated' not in st.session_state:
    st.session_state.cache_invalidated = False
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = datetime.now()

# ============================================================================
# AUTO-REFRESH
# ============================================================================

st.markdown(
    '<meta http-equiv="refresh" content="3600">',
    unsafe_allow_html=True
)

# ============================================================================
# CSS STYLING - Enterprise Polish with Apple Design (Tightened)
# ============================================================================

st.markdown("""
<style>
    /* ===== CSS VARIABLES ===== */
    :root {
        color-scheme: light only !important;
        --bg-primary: #F5F5F7;
        --bg-card: #FFFFFF;
        --bg-card-hover: #FAFAFA;
        --bg-input: #F0F0F2;
        --text-primary: #1D1D1F;
        --text-secondary: #6E6E73;
        --text-tertiary: #86868B;
        --text-muted: #AEAEB2;
        --border-light: #E5E5EA;
        --border-medium: #D1D1D6;
        --border-focus: #0066CC;
        --accent-blue: #0066CC;
        --accent-blue-hover: #0055B3;
        --accent-green: #34C759;
        --accent-green-bg: rgba(52, 199, 89, 0.12);
        --accent-red: #FF3B30;
        --accent-red-bg: rgba(255, 59, 48, 0.12);
        --accent-orange: #FF9500;
        --accent-orange-bg: rgba(255, 149, 0, 0.12);
        --accent-purple: #AF52DE;
        --shadow-sm: 0 1px 2px rgba(0, 0, 0, 0.06);
        --shadow-md: 0 3px 10px rgba(0, 0, 0, 0.08);
        --shadow-lg: 0 6px 20px rgba(0, 0, 0, 0.1);
        --shadow-hover: 0 5px 16px rgba(0, 0, 0, 0.12);
        --radius-sm: 8px;
        --radius-md: 10px;
        --radius-lg: 12px;
        --transition-fast: 0.15s ease;
        --transition-normal: 0.25s ease;
    }
    
    /* ===== FONTS ===== */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    /* ===== GLOBAL RESET ===== */
    html, body, [data-testid="stAppViewContainer"], [data-testid="stApp"], 
    .stApp, .main, [data-testid="stMainBlockContainer"] {
        background-color: var(--bg-primary) !important;
        color: var(--text-primary) !important;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'SF Pro Text', 'Segoe UI', Roboto, sans-serif !important;
        font-weight: 400 !important;
        -webkit-font-smoothing: antialiased !important;
        -moz-osx-font-smoothing: grayscale !important;
    }
    
    /* ===== HIDE STREAMLIT CHROME ===== */
    #MainMenu, footer, header, 
    [data-testid="stHeader"], 
    [data-testid="stToolbar"],
    .stDeployButton { 
        display: none !important; 
        visibility: hidden !important;
    }
    
    /* ===== HIDE ALERTS IN SUMMARY ===== */
    [data-testid="stAlert"] {
        display: none !important;
    }
    
    /* ===== MAIN HEADER ===== */
    .main-header {
        background: linear-gradient(135deg, #1D1D1F 0%, #3A3A3C 100%) !important;
        color: #FFFFFF !important;
        padding: 1.25rem 1.5rem !important;
        border-radius: var(--radius-lg) !important;
        margin-bottom: 0.75rem !important;
        box-shadow: var(--shadow-lg) !important;
        position: relative !important;
        overflow: hidden !important;
    }
    
    .main-header::before {
        content: '' !important;
        position: absolute !important;
        top: 0 !important;
        left: 0 !important;
        right: 0 !important;
        height: 1px !important;
        background: linear-gradient(90deg, transparent, rgba(255,255,255,0.2), transparent) !important;
    }
    
    .main-header .main-title { 
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        font-size: 1.625rem !important; 
        font-weight: 600 !important; 
        letter-spacing: -0.025em !important;
        color: #FFFFFF !important;
        margin: 0 !important;
        padding: 0 !important;
    }
    
    .live-badge {
        display: inline-flex !important;
        align-items: center !important;
        gap: 0.35rem !important;
        background: var(--accent-green-bg) !important;
        color: var(--accent-green) !important;
        font-size: 0.6875rem !important;
        font-weight: 600 !important;
        padding: 0.25rem 0.625rem !important;
        border-radius: 20px !important;
        margin-left: 0.75rem !important;
        text-transform: uppercase !important;
        letter-spacing: 0.05em !important;
        vertical-align: middle !important;
    }
    
    .live-badge::before {
        content: '' !important;
        width: 6px !important;
        height: 6px !important;
        background: var(--accent-green) !important;
        border-radius: 50% !important;
        animation: pulse 2s ease-in-out infinite !important;
    }
    
    @keyframes pulse {
        0%, 100% { opacity: 1; transform: scale(1); }
        50% { opacity: 0.5; transform: scale(0.9); }
    }
    
    /* ===== CARDS ===== */
    .card {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-light) !important;
        border-radius: var(--radius-md) !important;
        padding: 1.25rem !important;
        margin-bottom: 0.75rem !important;
        box-shadow: var(--shadow-sm) !important;
        transition: box-shadow var(--transition-normal), border-color var(--transition-normal) !important;
    }
    
    .card:hover {
        box-shadow: var(--shadow-md) !important;
        border-color: var(--border-medium) !important;
    }
    
    /* ===== BANNER (Breaking News) ===== */
    .banner {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-light) !important;
        border-left: 4px solid var(--accent-red) !important;
        border-radius: var(--radius-sm) !important;
        padding: 0.625rem 1rem !important;
        margin: 0.5rem 0 !important;
        font-size: 0.9375rem !important;
        color: var(--text-primary) !important;
        box-shadow: var(--shadow-sm) !important;
        transition: box-shadow var(--transition-normal) !important;
    }
    
    .banner:hover {
        box-shadow: var(--shadow-md) !important;
    }
    
    .banner a {
        color: var(--text-primary) !important;
        text-decoration: none !important;
        font-weight: 500 !important;
        transition: color var(--transition-fast) !important;
    }
    
    .banner a:hover {
        color: var(--accent-blue) !important;
    }
    
    .banner-meta {
        color: var(--text-tertiary) !important;
        font-size: 0.8125rem !important;
        margin-left: 0.5rem !important;
    }
    
    /* ===== SECTION TITLES ===== */
    .section-title {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        font-size: 0.6875rem !important;
        font-weight: 600 !important;
        color: var(--text-tertiary) !important;
        text-transform: uppercase !important;
        letter-spacing: 0.1em !important;
        margin: 1rem 0 0.5rem 0 !important;
        padding: 0 !important;
    }
    
    /* ===== SUMMARY CONTAINER ===== */
    .summary-container {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-light) !important;
        border-radius: var(--radius-md) !important;
        padding: 1.25rem !important;
        margin-bottom: 0.5rem !important;
        box-shadow: var(--shadow-sm) !important;
    }
    
    .summary-paragraph {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        font-size: 0.9375rem !important;
        line-height: 1.6 !important;
        color: var(--text-primary) !important;
        margin-bottom: 0.75em !important;
        text-align: justify !important;
        hyphens: auto !important;
    }
    
    .summary-paragraph:last-child {
        margin-bottom: 0 !important;
    }
    
    .summary-paragraph a {
        color: var(--accent-blue) !important;
        text-decoration: none !important;
        transition: color var(--transition-fast) !important;
    }
    
    .summary-paragraph a:hover {
        color: var(--accent-blue-hover) !important;
        text-decoration: underline !important;
    }
    
    .summary-paragraph sup {
        font-size: 0.7em !important;
        vertical-align: super !important;
        line-height: 0 !important;
    }
    
    .summary-paragraph sup a {
        color: var(--text-tertiary) !important;
        font-weight: 500 !important;
    }
    
    .summary-paragraph sup a:hover {
        color: var(--accent-blue) !important;
    }
    
    /* ===== METRICS ===== */
    [data-testid="stMetricValue"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        font-size: 1.5rem !important;
        font-weight: 600 !important;
        color: var(--text-primary) !important;
        letter-spacing: -0.02em !important;
    }
    
    [data-testid="stMetricLabel"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        font-size: 0.75rem !important;
        color: var(--text-secondary) !important;
        text-transform: none !important;
        font-weight: 500 !important;
    }
    
    .row-widget.stMetric {
        margin-bottom: 0.5rem !important;
    }
    
    /* ===== TABS ===== */
    [data-testid="stTabs"] [data-baseweb="tab-list"] {
        background: var(--bg-input) !important;
        border-radius: 10px !important;
        padding: 4px !important;
        gap: 2px !important;
    }
    
    [data-testid="stTabs"] [data-baseweb="tab"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        font-size: 0.8125rem !important;
        font-weight: 500 !important;
        color: var(--text-secondary) !important;
        border-radius: 8px !important;
        padding: 0.375rem 0.75rem !important;
        background: transparent !important;
        border: none !important;
        transition: all var(--transition-fast) !important;
    }
    
    [data-testid="stTabs"] [data-baseweb="tab"]:hover {
        color: var(--text-primary) !important;
        background: rgba(0, 0, 0, 0.03) !important;
    }
    
    [data-testid="stTabs"] [aria-selected="true"] {
        background: var(--bg-card) !important;
        box-shadow: var(--shadow-sm) !important;
        color: var(--text-primary) !important;
    }
    
    [data-testid="stTabs"] [data-baseweb="tab-highlight"] {
        display: none !important;
    }
    
    /* ===== FEED ITEMS ===== */
    .feed-item {
        padding: 0.625rem 0 !important;
        border-bottom: 1px solid var(--border-light) !important;
        transition: background-color var(--transition-fast) !important;
    }
    
    .feed-item:hover {
        background-color: var(--bg-card-hover) !important;
        margin: 0 -0.5rem !important;
        padding-left: 0.5rem !important;
        padding-right: 0.5rem !important;
        border-radius: var(--radius-sm) !important;
    }
    
    .feed-item:last-child {
        border-bottom: none !important;
    }
    
    /* ===== EXPANDER ===== */
    [data-testid="stExpander"] {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-light) !important;
        border-radius: var(--radius-sm) !important;
        box-shadow: var(--shadow-sm) !important;
    }
    
    [data-testid="stExpander"] summary {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        font-size: 0.875rem !important;
        font-weight: 500 !important;
        color: var(--text-primary) !important;
    }
    
    [data-testid="stExpander"] summary:hover {
        color: var(--accent-blue) !important;
    }
    
    /* ===== SIDEBAR ===== */
    [data-testid="stSidebar"] {
        background: var(--bg-card) !important;
        border-right: 1px solid var(--border-light) !important;
    }
    
    [data-testid="stSidebar"] [data-testid="stMarkdown"] {
        color: var(--text-primary) !important;
    }
    
    /* ===== LINKS & TYPOGRAPHY ===== */
    [data-testid="stMarkdown"] p {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        line-height: 1.55 !important;
        color: var(--text-primary) !important;
    }
    
    [data-testid="stMarkdown"] a {
        color: var(--accent-blue) !important;
        text-decoration: none !important;
        transition: color var(--transition-fast) !important;
    }
    
    [data-testid="stMarkdown"] a:hover {
        color: var(--accent-blue-hover) !important;
        text-decoration: underline !important;
    }
    
    /* ===== DIVIDERS ===== */
    hr {
        border: none !important;
        border-top: 1px solid var(--border-light) !important;
        margin: 0.75rem 0 !important;
    }
    
    /* ===== SPINNER ===== */
    [data-testid="stSpinner"] {
        color: var(--text-secondary) !important;
    }
    
    /* ===== CAPTIONS ===== */
    [data-testid="stCaptionContainer"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        font-size: 0.8125rem !important;
        color: var(--text-tertiary) !important;
    }
    
    /* ===== SOURCE LIST ===== */
    .source-list-item {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        font-size: 0.8125rem !important;
        color: var(--text-primary) !important;
        padding: 0.25rem 0 !important;
        line-height: 1.4 !important;
        border-bottom: 1px solid var(--border-light) !important;
    }
    
    .source-list-item:last-child {
        border-bottom: none !important;
    }
    
    .source-list-item a {
        color: var(--accent-blue) !important;
    }
    
    /* ===== BUTTONS ===== */
    .stButton > button {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        font-weight: 500 !important;
        border-radius: var(--radius-sm) !important;
        transition: all var(--transition-fast) !important;
    }
    
    .stButton > button:hover {
        transform: translateY(-1px) !important;
        box-shadow: var(--shadow-md) !important;
    }
    
    .stButton > button:active {
        transform: translateY(0) !important;
    }
    
    /* ===== STATUS INDICATORS ===== */
    .status-success {
        color: var(--accent-green) !important;
        background: var(--accent-green-bg) !important;
        padding: 0.25rem 0.5rem !important;
        border-radius: var(--radius-sm) !important;
        font-size: 0.75rem !important;
        font-weight: 500 !important;
    }
    
    .status-error {
        color: var(--accent-red) !important;
        background: var(--accent-red-bg) !important;
        padding: 0.25rem 0.5rem !important;
        border-radius: var(--radius-sm) !important;
        font-size: 0.75rem !important;
        font-weight: 500 !important;
    }
    
    .status-warning {
        color: var(--accent-orange) !important;
        background: var(--accent-orange-bg) !important;
        padding: 0.25rem 0.5rem !important;
        border-radius: var(--radius-sm) !important;
        font-size: 0.75rem !important;
        font-weight: 500 !important;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# SOURCE CONFIGURATION
# ============================================================================

# Configurable blocked sources
BLOCKED_SOURCES = [
    'wikipedia',
    'reddit',
    'twitter',
    'x.com',
    'facebook',
    'tiktok',
    'pinterest',
    'quora',
]

# Irrelevant content patterns
IRRELEVANT_PATTERNS = [
    r'stranger things', r'harry potter', r'marvel movie', r'dc universe', r'dceu',
    r'premiere', r'trailer release', r'casting news',
    r'james gunn', r'zack snyder', r'chris nolan',
    r'best movies of', r'best shows of', r'most anticipated', r'\branking\b', r'\branked\b',
    r'lgbtq', r'coming to netflix', r'leaving netflix',
    r'what to watch', r'\bbinge\b', r'review:',
    r'demogorgon', r'upside down',
    r'top gun', r'speed racer', r'purple rain', r'babylon 5',
    r'dstv', r'canal\+', r'multichoice', r'social media ban',
    r'bundling deal', r'bundle deal', r'distribution deal', r'licensing deal',
    r'rtl\+', r'signs deal with', r'streaming bundle',
    r'grammy', r'emmy', r'oscar', r'golden globe',
    r'game of thrones', r'house of the dragon',
]

# TIER 1: Premium US/Canada sources
TIER1_SOURCES = [
    'reuters', 'bloomberg', 'wsj', 'apnews', 'nytimes', 'washingtonpost',
    'variety', 'deadline', 'hollywoodreporter', 'cnbc', 'cnn', 'bbc',
    'latimes', 'usatoday', 'nypost', 'foxbusiness', 'nbcnews', 'abcnews', 'cbsnews',
    'globeandmail', 'cbc', 'nationalpost', 'financialpost', 'bnnbloomberg', 'globalnews', 'ctv',
    'prnewswire', 'businesswire',
    'sec', 'doj', 'ftc', 'justice', 'europa', 'ec.europa', 'competitionbureau',
    'cma', 'gov.uk',
]

# TIER 2: Acceptable sources
TIER2_SOURCES = [
    'seekingalpha', 'benzinga', 'thestreet', 'marketwatch', 'barrons',
    'techcrunch', 'theverge', 'engadget', 'fastcompany', 'forbes', 'insider',
    'guardian', 'independent', 'telegraph', 'euronews',
    'indiewire', 'screendaily', 'vulture', 'rollingstone', 'avclub',
    'cision', 'gamespot', 'ign', 'insidermonkey', 'moneycontrol',
]

# Regulatory keywords and patterns
REGULATORY_PATTERNS = [
    r'(doj|department of justice|antitrust)',
    r'(ftc|federal trade commission)',
    r'(european commission|ec approval|eu regulator)',
    r'(cma|competition.{0,10}markets)',
    r'(competition bureau|canada regulator)',
    r'(hsr|hart-scott-rodino)',
    r'regulatory (review|approval|hurdle|scrutiny)',
    r'antitrust (review|concern|issue|scrutiny)',
    r'merger (review|approval|blocked)',
]

# Deal-specific terms
DEAL_PATTERNS = [
    r'(\$82|\$83|\$85).{0,5}billion',
    r'(\$108|\$110).{0,5}billion',
    r'\$30.{0,5}(per share|/share)',
    r'tender offer',
    r'hostile (bid|takeover)',
    r'poison pill',
    r'shareholder (vote|approval|meeting)',
    r'board (reject|accept|consider)',
]

# Must contain deal context patterns
MUST_CONTAIN_DEAL_CONTEXT = [
    r'netflix.{0,30}(acquire|acqui|buy|bid|offer|merge|deal).{0,30}(warner|wbd|discovery)',
    r'(warner|wbd|discovery).{0,30}(acquire|acqui|buy|bid|offer|merge|deal).{0,30}netflix',
    r'netflix.{0,50}warner',
    r'warner.{0,50}netflix',
    r'wbd.{0,30}netflix',
    r'netflix.{0,30}wbd',
    r'paramount.{0,30}(warner|wbd).{0,30}(bid|offer|hostile|tender)',
    r'(warner|wbd).{0,30}paramount.{0,30}(bid|offer|hostile|tender)',
    r'skydance.{0,30}(warner|wbd)',
    r'ellison.{0,30}(warner|wbd)',
    r'netflix.{0,50}zaslav',
    r'zaslav.{0,30}(netflix|merger|deal|acquisition)',
    r'sarandos.{0,30}(warner|wbd|acquisition|merger)',
    r'netflix.{0,50}sarandos.{0,30}warner',
    r'netflix buys wbd',
    r'netflix buys warner',
    r'netflix acquires warner',
    r'netflix acquires wbd',
    r'paramount.{0,50}(hostile|tender|takeover).{0,30}(warner|wbd)',
    r'skydance.{0,50}(bid|offer).{0,30}(warner|wbd)',
    r'ellison.{0,30}(warner|wbd|bid|offer)',
    r'(ftc|doj|antitrust).{0,50}(netflix.{0,30}warner|warner.{0,30}netflix)',
    r'(european commission|ec).{0,30}(netflix|warner).{0,30}(merger|review)',
    r'(cma|competition).{0,30}(netflix|warner).{0,30}(review|merger)',
    r'(competition bureau|canada).{0,30}(netflix|warner)',
    r'hsr.{0,20}(netflix|warner)',
    r'\$82.{0,20}billion',
    r'\$83.{0,20}billion',
    r'\$108.{0,20}billion',
    r'\$30.{0,10}(per share|share)',
    r'tender offer.{0,30}(warner|wbd|paramount)',
    r'bidding war.{0,30}(warner|wbd)',
    r'hostile.{0,30}(bid|takeover).{0,30}(warner|wbd)',
    r'(hbo|hbo max).{0,30}(netflix.{0,20}acquisition|sold to netflix)',
    r'max streaming.{0,30}(netflix|acquisition|merger)',
]

# ============================================================================
# SEC FILING CONFIGURATION
# ============================================================================

SEC_MA_FORMS = [
    'SC 13D', 'SC 13D/A', 'SC 13G', 'SC 13G/A',  # Beneficial ownership
    'SC TO-T', 'SC TO-T/A', 'SC TO-C', 'SC 14D9', 'SC 14D9/A',  # Tender offers
    'DEFM14A', 'DEFM14C', 'PREM14A', 'PREM14C',  # Proxy statements
    'S-4', 'S-4/A', 'S-4EF',  # Registration for M&A
    '425', 'DEFA14A',  # M&A communications
    '8-K', '8-K/A',  # Current reports (material events)
]

SEC_FORM_DESCRIPTIONS = {
    'SC 13D': 'Activist investor stake disclosure',
    'SC 13D/A': 'Amended activist stake disclosure',
    'SC 13G': 'Passive investor stake disclosure',
    'SC 13G/A': 'Amended passive stake disclosure',
    'SC TO-T': 'Third-party tender offer statement',
    'SC TO-T/A': 'Amended tender offer statement',
    'SC TO-C': 'Tender offer communication',
    'SC 14D9': 'Target company tender offer response',
    'SC 14D9/A': 'Amended tender offer response',
    'DEFM14A': 'Definitive merger proxy statement',
    'DEFM14C': 'Definitive merger information statement',
    'PREM14A': 'Preliminary merger proxy statement',
    'PREM14C': 'Preliminary merger information statement',
    'S-4': 'M&A registration statement',
    'S-4/A': 'Amended M&A registration',
    'S-4EF': 'Automatic M&A registration',
    '425': 'M&A prospectus communication',
    'DEFA14A': 'Additional proxy solicitation material',
    '8-K': 'Material event report',
    '8-K/A': 'Amended material event report',
}

SEC_COMPANIES = {
    'Netflix': '0001065280',
    'Warner Bros Discovery': '0001437107',
    'Paramount': '0000813828',
}

# ============================================================================
# NEWS API CONFIGURATION
# ============================================================================

BASE_URL = "https://newsdata.io/api/1/news"

# ============================================================================
# AI SUMMARY CONFIGURATION - Pydantic Schema
# ============================================================================

class SummaryParagraph(BaseModel):
    """Schema for a single summary paragraph."""
    content: str = Field(description="The paragraph content with citation numbers in [N] format")
    citations: List[int] = Field(description="List of source numbers cited in this paragraph")

class ExecutiveSummary(BaseModel):
    """Schema for the complete executive summary."""
    recent_developments: SummaryParagraph = Field(description="Paragraph about recent transaction developments in the last 48-72 hours")
    regulatory_status: SummaryParagraph = Field(description="Paragraph about regulatory and legal status from DOJ, FTC, EC, UK CMA, and Canadian Competition Bureau")
    deal_comparison: SummaryParagraph = Field(description="Paragraph comparing competing offers and deal structures")

# Configure Gemini with error handling
GEMINI_AVAILABLE = False
try:
    gemini_key = get_secret("gemini_key")
    if gemini_key:
        genai.configure(api_key=gemini_key)
        GEMINI_AVAILABLE = True
except Exception as e:
    GEMINI_AVAILABLE = False

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def safe_escape(text: Any) -> str:
    """Safely escape text for HTML display."""
    if text is None:
        return ""
    return html.escape(str(text))

def get_source_tier(source_id: str) -> int:
    """Return 1 for tier1, 2 for tier2, 3 for other sources."""
    if not source_id:
        return 3
    source_lower = source_id.lower()
    if any(t in source_lower for t in TIER1_SOURCES):
        return 1
    if any(t in source_lower for t in TIER2_SOURCES):
        return 2
    return 3

def get_url_path(url: str) -> str:
    """Extract the path from a URL for deduplication."""
    try:
        parsed = urlparse(url)
        return parsed.path.strip('/').lower()
    except Exception:
        return url

def url_path_similarity(url1: str, url2: str) -> float:
    """Calculate similarity between URL paths."""
    path1 = get_url_path(url1)
    path2 = get_url_path(url2)
    return SequenceMatcher(None, path1, path2).ratio()

def format_source(source_id: str) -> str:
    """Format source name for display with escaping."""
    if not source_id:
        return "Unknown"
    mapping = {
        'wsj': 'WSJ', 'nytimes': 'NYT', 'washingtonpost': 'WaPo',
        'bbc': 'BBC', 'cnn': 'CNN', 'cnbc': 'CNBC', 'cbc': 'CBC',
        'apnews': 'AP', 'reuters': 'Reuters', 'bloomberg': 'Bloomberg',
        'variety': 'Variety', 'deadline': 'Deadline', 'hollywoodreporter': 'THR',
        'theverge': 'Verge', 'techcrunch': 'TechCrunch', 'forbes': 'Forbes',
        'globeandmail': 'Globe & Mail', 'nationalpost': 'National Post',
        'seekingalpha': 'Seeking Alpha', 'marketwatch': 'MarketWatch',
    }
    source_lower = source_id.lower()
    for key, val in mapping.items():
        if key in source_lower:
            return safe_escape(val)
    return safe_escape(source_id.title().replace('_', ' ')[:20])

def format_date(date_str: Optional[str]) -> str:
    """Format date for display."""
    if not date_str:
        return "Unknown"
    try:
        dt = parse_pubdate(date_str)
        if dt == datetime.min:
            return "Unknown"
        return dt.strftime("%b %d")
    except Exception:
        return "Unknown"

def format_date_full(date_str: Optional[str]) -> str:
    """Format full date for prompts."""
    if not date_str:
        return "Unknown date"
    try:
        dt = parse_pubdate(date_str)
        if dt == datetime.min:
            return "Unknown date"
        return dt.strftime("%B %d, %Y")
    except Exception:
        return "Unknown date"

def parse_pubdate(date_str: Optional[str]) -> datetime:
    """
    Robust date parsing with multiple fallbacks.
    Returns datetime.min on failure (with debug warning if enabled).
    """
    if not date_str:
        return datetime.min
    
    # Primary: use dateutil.parser for flexible parsing (if available)
    if DATEUTIL_AVAILABLE:
        try:
            return dateutil_parser.parse(str(date_str), fuzzy=True)
        except Exception:
            pass
    
    # Fallback 1: ISO format variations
    try:
        clean_str = str(date_str).replace('Z', '+00:00')
        return datetime.fromisoformat(clean_str)
    except Exception:
        pass
    
    # Fallback 2: Common formats
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%B %d, %Y",
        "%b %d, %Y",
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(str(date_str)[:19], fmt)
        except Exception:
            continue
    
    # Debug warning (only in debug mode)
    if DEBUG_MODE:
        st.warning(f"Could not parse date: {safe_escape(str(date_str)[:50])}")
    
    return datetime.min

def extract_headline(title: str) -> str:
    """Clean and truncate headline with escaping."""
    if not title:
        return "Untitled"
    # Remove common suffixes
    for sep in [' | ', ' - ', ' – ', ' — ']:
        if sep in title:
            title = title.split(sep)[0]
    return safe_escape(title[:120] + ('...' if len(title) > 120 else ''))

def is_relevant_article(article: Dict[str, Any]) -> bool:
    """Filter for transaction-relevant articles."""
    title = (article.get('title') or '').lower()
    desc = (article.get('description') or '').lower()
    source = (article.get('source_id') or '').lower()
    text = title + ' ' + desc
    
    # Block sources
    if any(blocked in source for blocked in BLOCKED_SOURCES):
        return False
    
    # Block irrelevant content
    for pattern in IRRELEVANT_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return False
    
    # Must contain some deal context
    has_deal_context = any(
        re.search(p, text, re.IGNORECASE) 
        for p in MUST_CONTAIN_DEAL_CONTEXT
    )
    
    return has_deal_context

def categorize_article(article: Dict[str, Any]) -> str:
    """Categorize article by topic."""
    text = ((article.get('title') or '') + ' ' + (article.get('description') or '')).lower()
    
    for pattern in REGULATORY_PATTERNS:
        if re.search(pattern, text):
            return 'regulatory'
    
    for pattern in DEAL_PATTERNS:
        if re.search(pattern, text):
            return 'bids'
    
    if re.search(r'(analyst|opinion|outlook|prediction|expect|forecast)', text):
        return 'analysis'
    
    return 'deal'

def is_high_importance(article: Dict[str, Any]) -> bool:
    """Flag high-importance articles."""
    text = ((article.get('title') or '') + ' ' + (article.get('description') or '')).lower()
    
    high_importance_patterns = [
        r'tender offer',
        r'hostile (bid|takeover)',
        r'board (reject|accept)',
        r'shareholder vote',
        r'doj (block|sue|approve|clear)',
        r'ftc (block|sue|approve|clear)',
        r'regulatory (approve|block|clear)',
        r'deal (close|complete|terminate|collapse)',
        r'\$\d+.{0,5}billion.{0,20}(offer|bid)',
    ]
    
    return any(re.search(p, text) for p in high_importance_patterns)

def deduplicate_articles(articles: List[Dict[str, Any]], threshold: float = 0.75) -> List[Dict[str, Any]]:
    """
    Remove duplicate articles based on title similarity and URL path.
    Prefers Tier 1 sources.
    """
    if not articles:
        return []
    
    # Sort by tier (prefer tier 1)
    sorted_articles = sorted(articles, key=lambda a: get_source_tier(a.get('source_id', '')))
    
    unique = []
    seen_titles = []
    seen_urls = []
    
    for article in sorted_articles:
        title = (article.get('title') or '').lower()
        url = article.get('link', '')
        
        # Check title similarity
        is_title_duplicate = False
        for seen in seen_titles:
            if SequenceMatcher(None, title, seen).ratio() > threshold:
                is_title_duplicate = True
                break
        
        # Check URL path similarity
        is_url_duplicate = False
        for seen_url in seen_urls:
            if url_path_similarity(url, seen_url) > 0.85:
                is_url_duplicate = True
                break
        
        if not is_title_duplicate and not is_url_duplicate:
            unique.append(article)
            seen_titles.append(title)
            seen_urls.append(url)
    
    return unique

# ============================================================================
# PARALLEL DATA FETCHING (using ThreadPoolExecutor)
# ============================================================================

def fetch_news_query(query: str, api_key: str) -> List[Dict[str, Any]]:
    """Fetch news for a single query using requests."""
    params = {
        "apikey": api_key,
        "q": query,
        "language": "en",
        "size": 10
    }
    try:
        resp = requests.get(BASE_URL, params=params, timeout=API_TIMEOUT_DEFAULT)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "success":
                results = []
                for r in data.get("results", []):
                    results.append({
                        'title': r.get('title'),
                        'link': r.get('link'),
                        'pubDate': r.get('pubDate'),
                        'description': r.get('description'),
                        'source_id': r.get('source_id'),
                        'image_url': r.get('image_url')
                    })
                return results
    except requests.exceptions.Timeout:
        pass
    except requests.exceptions.RequestException:
        pass
    except Exception:
        pass
    return []

def fetch_all_news_parallel(api_key: str) -> List[Dict[str, Any]]:
    """Fetch news from all queries in parallel using ThreadPoolExecutor."""
    queries = [
        "Netflix Warner Bros acquisition",
        "Netflix Warner merger",
        "Netflix WBD deal",
        "Warner Bros Discovery Netflix",
        "Paramount Skydance Warner"
    ]
    
    all_articles = []
    seen_links = set()
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_news_query, q, api_key): q for q in queries}
        for future in as_completed(futures):
            try:
                results = future.result(timeout=API_TIMEOUT_DEFAULT + 5)
                for article in results:
                    link = article.get('link')
                    if link and link not in seen_links:
                        seen_links.add(link)
                        all_articles.append(article)
            except Exception:
                continue
    
    return all_articles

# ============================================================================
# SEC FILINGS FETCH
# ============================================================================

@st.cache_data(ttl=CACHE_TTL_SEC, show_spinner=False)
def fetch_sec_filings() -> List[Dict[str, Any]]:
    """Fetch SEC filings for tracked companies."""
    all_filings = []
    
    def fetch_company(company: str, cik: str) -> List[Dict[str, Any]]:
        filings = []
        try:
            url = f"https://data.sec.gov/submissions/CIK{cik}.json"
            headers = {"User-Agent": "ResearchBot/1.0 (research@example.com)"}
            resp = requests.get(url, headers=headers, timeout=API_TIMEOUT_SEC)
            
            if resp.status_code == 200:
                data = resp.json()
                recent = data.get('filings', {}).get('recent', {})
                forms = recent.get('form', [])
                dates = recent.get('filingDate', [])
                accessions = recent.get('accessionNumber', [])
                descriptions = recent.get('primaryDocDescription', [])
                
                cutoff = (datetime.utcnow() - timedelta(days=30)).strftime('%Y-%m-%d')
                
                for i, form in enumerate(forms[:50]):
                    if form in SEC_MA_FORMS and dates[i] >= cutoff:
                        acc = accessions[i].replace('-', '')
                        filings.append({
                            'company': company,
                            'form': form,
                            'form_description': SEC_FORM_DESCRIPTIONS.get(form, form),
                            'date': dates[i],
                            'title': descriptions[i] if i < len(descriptions) else form,
                            'url': f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{accessions[i]}-index.htm"
                        })
        except requests.exceptions.Timeout:
            pass
        except requests.exceptions.RequestException:
            pass
        except Exception:
            pass
        return filings
    
    # Use ThreadPoolExecutor for parallel SEC fetching
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_company, company, cik): company 
                   for company, cik in SEC_COMPANIES.items()}
        for future in as_completed(futures):
            try:
                filings = future.result(timeout=API_TIMEOUT_SEC + 5)
                all_filings.extend(filings)
            except Exception:
                continue
    
    return sorted(all_filings, key=lambda x: x['date'], reverse=True)

# ============================================================================
# DATA LOADING
# ============================================================================

@st.cache_data(ttl=CACHE_TTL_DATA, show_spinner=False)
def load_all_data(_cache_key: str) -> Dict[str, Any]:
    """Load and merge all data sources."""
    archived = []
    fresh = []
    worksheet = None
    archive_error = None
    api_error = None
    saved_count = 0
    
    # Load archived articles from Google Sheets
    try:
        gcp_creds = get_gcp_credentials()
        sheet_name = get_secret("sheet_name")
        sheet_id = get_secret("sheet_id", "1brXDHGcYUuduY8w9ql-7JDi9gCbWb29_ujkY7mYRu18")
        if gcp_creds and (sheet_name or sheet_id):
            gc = gspread.service_account_from_dict(gcp_creds)
            # Open by ID (more reliable) with name as fallback
            try:
                sh = gc.open_by_key(sheet_id)
            except Exception:
                sh = gc.open(sheet_name)
            worksheet = sh.sheet1
            archived = worksheet.get_all_records() or []
    except gspread.exceptions.APIError as e:
        archive_error = f"Sheets API error: {str(e)[:80]}"
    except gspread.exceptions.SpreadsheetNotFound:
        archive_error = "Spreadsheet not found"
    except Exception as e:
        archive_error = f"Archive error: {str(e)[:80]}"
    
    # Fetch fresh articles using parallel requests
    try:
        newsdata_key = get_secret("newsdata_key")
        if newsdata_key:
            fresh = fetch_all_news_parallel(newsdata_key)
    except Exception as e:
        api_error = f"News API error: {str(e)[:100]}"
    
    # Merge and save new articles
    existing_links = {a.get('link') for a in archived if a.get('link')}
    new_articles = [a for a in fresh if a.get('link') not in existing_links]
    
    if new_articles and worksheet:
        try:
            fields = ['title', 'link', 'pubDate', 'description', 'source_id', 'image_url']
            rows = [[a.get(field, '') for field in fields] for a in new_articles]
            worksheet.append_rows(rows)
            saved_count = len(new_articles)
        except Exception:
            pass
    
    # Combine all articles
    all_articles = archived + fresh
    
    # Filter, deduplicate, categorize
    relevant = [a for a in all_articles if is_relevant_article(a)]
    unique = deduplicate_articles(relevant)
    
    # Sort by date (newest first)
    unique.sort(key=lambda a: parse_pubdate(a.get('pubDate')), reverse=True)
    
    # Add category and importance
    for a in unique:
        a['category'] = categorize_article(a)
        a['important'] = is_high_importance(a)
    
    return {
        'articles': unique,
        'total_raw': len(all_articles),
        'filtered_out': len(all_articles) - len(relevant),
        'duplicates': len(relevant) - len(unique),
        'new_count': len([a for a in new_articles if is_relevant_article(a)]),
        'archived_count': len(archived),
        'api_fetched': len(fresh),
        'saved_count': saved_count,
        'archive_error': archive_error,
        'api_error': api_error,
    }

# ============================================================================
# UI RENDERING FUNCTIONS
# ============================================================================

def render_feed(articles_list: List[Dict[str, Any]], max_items: int = 20) -> None:
    """Render articles as a clean feed."""
    if not articles_list:
        st.caption("No articles found")
        return
    
    for i, article in enumerate(articles_list[:max_items]):
        title = extract_headline(article.get('title', 'Untitled'))
        link = safe_escape(article.get('link', '#'))
        source = format_source(article.get('source_id', ''))
        date = format_date(article.get('pubDate'))
        important = article.get('important', False)
        
        marker = "🔴 " if important else ""
        st.markdown(f"**{date}** · {marker}{source} [{title}]({link})")
        
        if i < min(max_items, len(articles_list)) - 1:
            st.markdown("<hr style='margin: 0.25rem 0; border: none; border-top: 1px solid #E5E5EA;'>", unsafe_allow_html=True)

def render_sec_filings(filings_list: List[Dict[str, Any]], max_items: int = 20) -> None:
    """Render SEC filings with plain language descriptions."""
    if not filings_list:
        st.caption("No SEC filings found")
        return
    
    company_emoji = {
        'Netflix': '🔴', 
        'Warner Bros Discovery': '🔵', 
        'Paramount': '🟣'
    }
    
    for i, filing in enumerate(filings_list[:max_items]):
        emoji = company_emoji.get(filing['company'], '📄')
        form_type = safe_escape(filing['form'])
        form_desc = safe_escape(filing.get('form_description', form_type))
        title = safe_escape(filing.get('title', form_desc))
        date = safe_escape(filing['date'])
        url = safe_escape(filing['url'])
        company = safe_escape(filing['company'])
        
        st.markdown(
            f"**{date}** · {emoji} {company} · **{form_type}** ({form_desc}) [{title}]({url})"
        )
        
        if i < min(max_items, len(filings_list)) - 1:
            st.markdown("<hr style='margin: 0.25rem 0; border: none; border-top: 1px solid #E5E5EA;'>", unsafe_allow_html=True)

# ============================================================================
# MAIN APPLICATION
# ============================================================================

# Sidebar
with st.sidebar:
    st.markdown("### ⚙️ Controls")
    
    if st.button("🔄 Refresh Data", use_container_width=True, type="primary"):
        # Manual cache invalidation
        st.session_state.cache_invalidated = True
        st.session_state.last_refresh = datetime.now()
        st.cache_data.clear()
        st.rerun()
    
    st.caption("Data refreshes hourly")
    
    st.divider()
    st.markdown("### 📊 Deal Terms")
    st.caption("**Netflix:** $82.7B")
    st.caption("WB Studios, HBO, Max")
    st.caption("**Paramount:** $108B (hostile)")
    st.caption("All WBD assets")
    
    st.divider()
    st.markdown("### 🏛️ Regulatory")
    st.caption("**DOJ:** Review initiated")
    st.caption("**FTC:** Pending")
    st.caption("**EC:** Pending")
    st.caption("**UK CMA:** Pending")
    st.caption("**HSR Filing:** Submitted")
    
    # Logout button if auth enabled
    if AUTH_ENABLED and 'authenticator' in dir():
        st.divider()
        try:
            authenticator.logout('Logout', 'sidebar')
        except Exception:
            pass

# Load data with cache key for manual invalidation
cache_key = str(st.session_state.get('last_refresh', 'initial'))

with st.spinner("Loading data..."):
    try:
        data = load_all_data(cache_key)
        sec_filings = fetch_sec_filings()
    except Exception as e:
        st.error(f"Error loading data: {safe_escape(str(e)[:200])}")
        data = {'articles': [], 'archived_count': 0, 'filtered_out': 0, 'saved_count': 0, 'archive_error': None, 'api_error': None}
        sec_filings = []

# Sidebar status
with st.sidebar:
    st.markdown("### 📊 Status")
    st.caption(f"📁 {data.get('archived_count', 0)} archived")
    st.caption(f"✅ {len(data.get('articles', []))} displayed")
    st.caption(f"🚫 {data.get('filtered_out', 0)} filtered")
    st.caption(f"📑 {len(sec_filings)} SEC filings")
    
    if data.get('saved_count', 0) > 0:
        st.markdown(f'<span class="status-success">✓ {data["saved_count"]} new saved</span>', unsafe_allow_html=True)
    
    st.divider()
    st.caption(f"v{APP_VERSION}")

# Main content
articles = data.get('articles', [])
now = datetime.now(ZoneInfo("America/Toronto"))
cutoff_7d = datetime.utcnow() - timedelta(days=7)
cutoff_48h = datetime.utcnow() - timedelta(hours=48)

recent = [a for a in articles if parse_pubdate(a.get('pubDate')) > cutoff_7d]
breaking = [a for a in articles if parse_pubdate(a.get('pubDate')) > cutoff_48h]

cat_counts = {'deal': 0, 'regulatory': 0, 'bids': 0, 'analysis': 0}
for a in recent:
    cat_counts[a.get('category', 'deal')] += 1

# Header
st.markdown(f"""
<div class="main-header">
    <div class="main-title">Netflix–Warner Transaction Monitor<span class="live-badge">Live</span></div>
</div>
""", unsafe_allow_html=True)

# Stats row
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Articles", len(articles))
col2.metric("Last 7 Days", len(recent))
col3.metric("Last 48 Hours", len(breaking))
col4.metric("Last Updated", now.strftime("%b %d, %I:%M %p"))

# Breaking news banner
if breaking:
    tier1_breaking = [a for a in breaking if get_source_tier(a.get('source_id', '')) == 1]
    latest = tier1_breaking[0] if tier1_breaking else breaking[0]
    
    latest_link = safe_escape(latest.get('link', '#'))
    latest_source = format_source(latest.get('source_id'))
    latest_title = extract_headline(latest.get('title', 'Breaking News'))
    
    st.markdown(f"""
    <div class="banner">
        ⚡ <a href="{latest_link}" target="_blank">{latest_title}</a>
        <span class="banner-meta">— {latest_source}</span>
    </div>
    """, unsafe_allow_html=True)

st.divider()

# Executive Summary
st.markdown('<p class="section-title">EXECUTIVE SUMMARY</p>', unsafe_allow_html=True)

if GEMINI_AVAILABLE and articles:
    # Prepare articles for summary
    tier1_articles = [a for a in articles if get_source_tier(a.get('source_id', '')) == 1]
    tier2_articles = [a for a in articles if get_source_tier(a.get('source_id', '')) == 2]
    regulatory_articles = [a for a in articles if a.get('category') == 'regulatory']
    
    priority_articles = regulatory_articles[:5] + tier1_articles[:15] + tier2_articles[:5]
    seen_links = set()
    unique_priority = []
    for a in priority_articles:
        if a.get('link') not in seen_links:
            seen_links.add(a.get('link'))
            unique_priority.append(a)
    priority_articles = unique_priority[:20]
    
    if not priority_articles:
        priority_articles = articles[:15]
    
    breaking_priority = [a for a in breaking if get_source_tier(a.get('source_id', '')) <= 2][:8]
    if not breaking_priority:
        breaking_priority = breaking[:5]
    
    articles_for_prompt = [{
        'title': extract_headline(a.get('title', '')),
        'description': (a.get('description') or '')[:MAX_DESCRIPTION_LENGTH],
        'url': a.get('link', ''),
        'date': format_date_full(a.get('pubDate')),
        'source': format_source(a.get('source_id', ''))
    } for a in priority_articles]
    
    breaking_for_prompt = [{
        'title': extract_headline(a.get('title', '')),
        'description': (a.get('description') or '')[:MAX_DESCRIPTION_LENGTH],
        'url': a.get('link', ''),
        'date': format_date_full(a.get('pubDate')),
        'source': format_source(a.get('source_id', ''))
    } for a in breaking_priority]
    
    # Limit total articles for summary (max 15)
    all_news_sources = (breaking_for_prompt + articles_for_prompt)[:MAX_ARTICLES_FOR_SUMMARY]
    
    @st.cache_data(ttl=CACHE_TTL_SUMMARY, show_spinner=False)
    def generate_structured_summary(articles_json: str) -> str:
        """Generate executive summary using Gemini with structured output."""
        limited_articles = json.loads(articles_json)
        
        # Build source list for prompt
        source_list = []
        for i, article in enumerate(limited_articles, 1):
            source_list.append(f"[{i}] {article['source']} ({article['date']}): {article['title']}\nDescription: {article['description']}\nURL: {article['url']}")
        
        prompt = f"""You are a senior M&A analyst preparing a detailed factual briefing on the Netflix–Warner Bros. Discovery transaction for institutional investors, legal counsel, and corporate executives.

SOURCE MATERIALS:
{chr(10).join(source_list)}

Generate a comprehensive executive summary with three detailed paragraphs based on the sources above. Cross-reference multiple sources for key facts to ensure accuracy; if conflicting information exists, note it objectively.

CRITICAL STYLE REQUIREMENTS:
- Write in neutral, objective, third-person voice appropriate for a legal or financial memorandum
- State facts without editorial commentary, speculation, or subjective characterization
- Avoid words like "significant," "substantial," "major," "notable," "interesting," or other qualitative descriptors unless directly quoting a source
- Do not characterize market reactions, sentiment, or implications beyond what sources explicitly state
- Use precise language and specific dollar amounts where available
- Present competing claims without favoring either party
- Keep sentences concise; aim for clarity and density without redundancy

PARAGRAPH 1 - RECENT DEVELOPMENTS (recent_developments):
Provide a detailed account of the last 48-72 hours of transaction activity. Include:
- Specific offers made with exact dollar amounts (total value and per-share price)
- Board actions and responses (acceptances, rejections, counteroffers)
- Public statements by named executives (David Zaslav, Ted Sarandos, Larry Ellison, Shari Redstone)
- Deadline extensions or amendments to existing offers
- Any changes to deal structure (e.g., shift from stock to all-cash)
This paragraph should be 5-7 sentences with specific facts and figures. If details are sparse, consolidate into fewer sentences without filler.

PARAGRAPH 2 - REGULATORY AND LEGAL STATUS (regulatory_status):
Provide detailed coverage of each regulatory body's position and any legal proceedings:
- DOJ (Department of Justice): Current review status, any statements on antitrust concerns, timeline indicators
- FTC (Federal Trade Commission): Involvement status, any public positions or concerns raised
- European Commission (EC): Notification status, Phase I/II review timeline, any conditions being discussed
- UK CMA (Competition and Markets Authority): Review status, any preliminary findings or concerns
- Canadian Competition Bureau (CCB): Filing status, review timeline
- Court proceedings: Any lawsuits filed (e.g., Paramount vs. WBD), specific legal arguments made, hearing dates
- Include specific quotes from regulators or legal filings where available
This paragraph should be 6-8 sentences covering each relevant regulatory development. Use "not disclosed in available reporting" only once per missing category to avoid repetition.

PARAGRAPH 3 - DEAL STRUCTURE COMPARISON (deal_comparison):
Provide a detailed comparison of all competing offers:
- Netflix offer: Total enterprise value, per-share price, payment structure (cash/stock mix), which assets are included (Warner Bros. studios, HBO, Max streaming platform, DC Entertainment), treatment of assets not included, financing arrangements
- Paramount/Skydance offer: Total value, per-share price, all-cash structure details, Larry Ellison/Oracle financing role, which WBD assets targeted
- Timeline comparison: Tender offer expiration dates, shareholder vote dates, regulatory approval deadlines, expected closing dates
- Any conditions or contingencies attached to each offer
This paragraph should be 5-7 sentences with specific financial terms. Highlight differences objectively.

FORMATTING RULES:
1. Include $ before ALL currency amounts: "$82.7 billion", "$30 per share", "$108 billion"
2. Use [N] format for citations where N is the source number (1-{len(limited_articles)})
3. Write flowing prose - absolutely no bullet points or lists
4. If specific information is not available in the sources, state "not disclosed in available reporting" - do not speculate or repeat unnecessarily
5. Include the citation numbers you use in the 'citations' list for each paragraph
6. Cite multiple sources for key claims where possible"""
        
        try:
            model = genai.GenerativeModel(
                model_name="gemini-2.0-flash",
                generation_config={
                    "temperature": 0.3,
                    "top_p": 0.95,
                    "response_mime_type": "application/json",
                    "response_schema": ExecutiveSummary.model_json_schema()
                }
            )
            
            response = model.generate_content(prompt)
            result = json.loads(response.text)
            
            # Build URL mapping for citations
            url_map = {i+1: article['url'] for i, article in enumerate(limited_articles)}
            
            def format_paragraph_with_links(content: str, citations: List[int]) -> str:
                """Convert [N] citations to clickable superscript links."""
                formatted = content
                for cite_num in sorted(set(citations), reverse=True):
                    if cite_num in url_map:
                        url = safe_escape(url_map[cite_num])
                        formatted = formatted.replace(
                            f"[{cite_num}]",
                            f'<sup><a href="{url}" target="_blank">[{cite_num}]</a></sup>'
                        )
                return formatted
            
            # Format each paragraph
            sections = []
            
            if 'recent_developments' in result:
                rd = result['recent_developments']
                content = format_paragraph_with_links(rd.get('content', ''), rd.get('citations', []))
                sections.append(f'<p class="summary-paragraph">{content}</p>')
            
            if 'regulatory_status' in result:
                rs = result['regulatory_status']
                content = format_paragraph_with_links(rs.get('content', ''), rs.get('citations', []))
                sections.append(f'<p class="summary-paragraph">{content}</p>')
            
            if 'deal_comparison' in result:
                dc = result['deal_comparison']
                content = format_paragraph_with_links(dc.get('content', ''), dc.get('citations', []))
                sections.append(f'<p class="summary-paragraph">{content}</p>')
            
            if sections:
                return f'<div class="summary-container">{chr(10).join(sections)}</div>'
            
        except Exception as e:
            pass
        
        return ""
    
    def generate_fallback_summary(articles_list: List[Dict[str, Any]]) -> str:
        """Generate a simple fallback summary when AI is unavailable."""
        if not articles_list:
            return '<div class="summary-container"><p class="summary-paragraph">No recent articles available for summary.</p></div>'
        
        top_articles = articles_list[:5]
        summary_parts = []
        for i, article in enumerate(top_articles, 1):
            title = article.get('title', 'Untitled')
            source = article.get('source', 'Unknown')
            url = safe_escape(article.get('url', '#'))
            summary_parts.append(f'{title} ({source})<sup><a href="{url}" target="_blank">[{i}]</a></sup>')
        
        content = '. '.join(summary_parts) + '.'
        return f'<div class="summary-container"><p class="summary-paragraph"><strong>Latest Coverage:</strong> {content}</p></div>'
    
    # Generate summary
    try:
        summary_html = generate_structured_summary(json.dumps(all_news_sources))
        if summary_html:
            st.markdown(summary_html, unsafe_allow_html=True)
        else:
            st.markdown(generate_fallback_summary(articles_for_prompt), unsafe_allow_html=True)
    except Exception as e:
        st.markdown(generate_fallback_summary(articles_for_prompt), unsafe_allow_html=True)
else:
    st.markdown("""
    <div class="summary-container">
        <p class="summary-paragraph">AI-powered executive summary is currently unavailable. Please check API configuration.</p>
    </div>
    """, unsafe_allow_html=True)

st.divider()

# Categorized News Tabs
st.markdown('<p class="section-title">NEWS BY CATEGORY</p>', unsafe_allow_html=True)

tab_deal, tab_reg, tab_bids, tab_sec, tab_analysis = st.tabs([
    f"📰 Deal News ({cat_counts['deal']})",
    f"🏛️ Regulatory ({cat_counts['regulatory']})",
    f"💰 Bids & Offers ({cat_counts['bids']})",
    f"📑 SEC Filings ({len(sec_filings)})",
    f"📊 Analysis ({cat_counts['analysis']})"
])

with tab_deal:
    deal_arts = [a for a in recent if a.get('category') == 'deal']
    render_feed(deal_arts, 20)

with tab_reg:
    reg_arts = [a for a in recent if a.get('category') == 'regulatory']
    render_feed(reg_arts, 20)

with tab_bids:
    bid_arts = [a for a in recent if a.get('category') == 'bids']
    render_feed(bid_arts, 20)

with tab_sec:
    if sec_filings:
        st.caption("Direct from SEC EDGAR · Netflix (NFLX) · Warner Bros Discovery (WBD) · Paramount (PARA)")
        render_sec_filings(sec_filings, 20)
    else:
        st.caption("No M&A-related SEC filings in the last 30 days.")

with tab_analysis:
    analysis_arts = [a for a in recent if a.get('category') == 'analysis']
    render_feed(analysis_arts, 20)

# Full Archive
with st.expander(f"Full Archive ({len(articles)} articles)"):
    render_feed(articles, 100)

# Footer
st.markdown("""<hr style='margin: 0.75rem 0;'><p style='text-align: center; color: var(--text-tertiary); font-size: 0.75rem; margin: 0;'>Built by Mohit Sethi (@_MohitSethi) – Toronto, January 2026 · v1.0</p>""", unsafe_allow_html=True)
