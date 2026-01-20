"""
Netflix–WBD Transaction Monitor v7.2
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

def get_gcp_credentials() -> Tuple[Optional[Dict[str, Any]], str]:
    """Get GCP service account credentials from secrets or env. Returns (creds, source)."""
    # Try Streamlit secrets first
    try:
        if "gcp_service_account" in st.secrets:
            return dict(st.secrets["gcp_service_account"]), "streamlit_secrets"
    except Exception:
        pass
    
    # Try JSON string from environment variable
    gcp_json = os.environ.get("GCP_SERVICE_ACCOUNT")
    if gcp_json:
        try:
            return json.loads(gcp_json), "json_env_var"
        except Exception as e:
            pass
    
    # Try individual environment variables (most reliable for Render)
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
        }, "individual_env_vars"
    
    return None, "none"

# ============================================================================
# CONFIGURATION
# ============================================================================

APP_VERSION = "7.2"
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

# Force clear cache on first load (temporary for debugging)
if 'cache_cleared' not in st.session_state:
    st.cache_data.clear()
    st.session_state.cache_cleared = True

# ============================================================================
# AUTO-REFRESH
# ============================================================================

st.markdown(
    '<meta http-equiv="refresh" content="3600">',
    unsafe_allow_html=True
)

# ============================================================================
# CSS STYLING - Enterprise Polish with Apple Design
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
        --shadow-sm: 0 1px 3px rgba(0, 0, 0, 0.08);
        --shadow-md: 0 4px 12px rgba(0, 0, 0, 0.1);
        --shadow-lg: 0 8px 24px rgba(0, 0, 0, 0.12);
        --shadow-hover: 0 6px 20px rgba(0, 0, 0, 0.15);
        --radius-sm: 8px;
        --radius-md: 12px;
        --radius-lg: 16px;
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
        padding: 1.75rem 2rem !important;
        border-radius: var(--radius-lg) !important;
        margin-bottom: 1.25rem !important;
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
        padding: 1.5rem !important;
        margin-bottom: 1rem !important;
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
        padding: 0.875rem 1.25rem !important;
        margin: 1rem 0 !important;
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
        margin: 1.75rem 0 1rem 0 !important;
        padding: 0 !important;
    }
    
    /* ===== SUMMARY CONTAINER ===== */
    .summary-container {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-light) !important;
        border-radius: var(--radius-md) !important;
        padding: 1.75rem !important;
        margin-bottom: 1rem !important;
        box-shadow: var(--shadow-sm) !important;
    }
    
    .summary-paragraph {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        font-size: 0.9375rem !important;
        line-height: 1.75 !important;
        color: var(--text-primary) !important;
        margin-bottom: 1.25em !important;
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
        font-size: 1.875rem !important;
        font-weight: 600 !important;
        color: var(--text-primary) !important;
        letter-spacing: -0.02em !important;
    }
    
    [data-testid="stMetricLabel"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        font-size: 0.8125rem !important;
        color: var(--text-secondary) !important;
        text-transform: none !important;
        font-weight: 500 !important;
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
        padding: 0.5rem 1rem !important;
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
        padding: 0.875rem 0 !important;
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
        line-height: 1.65 !important;
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
        margin: 1.25rem 0 !important;
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
        padding: 0.375rem 0 !important;
        line-height: 1.5 !important;
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

TRUSTED_SOURCE_PATTERNS = TIER1_SOURCES + TIER2_SOURCES + [
    'netflix', 'wbd', 'paramount', 'ftc', 'doj',
    'livemint', 'economictimes',
]

# Expanded deal context patterns for better relevance filtering
MUST_CONTAIN_DEAL_CONTEXT = [
    # Netflix-Warner acquisition patterns
    r'netflix.{0,50}(acqui|merg|buy|bid|purchase).{0,30}warner',
    r'warner.{0,50}(acqui|merg|bid|takeover|purchase).{0,30}netflix',
    r'wbd.{0,50}(netflix|merger|acquisition)',
    r'netflix.{0,30}wbd',
    r'netflix buys wbd',
    r'netflix buys warner',
    r'netflix acquires warner',
    r'netflix acquires wbd',
    
    # Executive-specific patterns
    r'netflix.{0,50}zaslav',
    r'zaslav.{0,30}(netflix|merger|deal|acquisition)',
    r'sarandos.{0,30}(warner|wbd|acquisition|merger)',
    r'netflix.{0,50}sarandos.{0,30}warner',
    
    # Paramount competing bid patterns
    r'paramount.{0,50}(hostile|tender|takeover).{0,30}(warner|wbd)',
    r'skydance.{0,50}(bid|offer).{0,30}(warner|wbd)',
    r'ellison.{0,30}(warner|wbd|bid|offer)',
    
    # Regulatory patterns
    r'(ftc|doj|antitrust).{0,50}(netflix.{0,30}warner|warner.{0,30}netflix)',
    r'(european commission|ec).{0,30}(netflix|warner).{0,30}(merger|review)',
    r'(cma|competition).{0,30}(netflix|warner).{0,30}(review|merger)',
    r'(competition bureau|canada).{0,30}(netflix|warner)',
    r'hsr.{0,20}(netflix|warner)',
    r'hart-scott-rodino.{0,30}(netflix|warner)',
    
    # Deal value patterns
    r'\$82.{0,20}billion',
    r'\$83.{0,20}billion',
    r'\$108.{0,20}billion',
    r'\$30.{0,10}(per share|share)',
    
    # Tender offer patterns
    r'tender offer.{0,30}(warner|wbd|paramount)',
    r'bidding war.{0,30}(warner|wbd)',
    r'hostile.{0,30}(bid|takeover).{0,30}(warner|wbd)',
    
    # HBO/Max specific
    r'(hbo|hbo max).{0,30}(netflix.{0,20}acquisition|sold to netflix)',
    r'max streaming.{0,30}(netflix|acquisition|merger)',
]

# ============================================================================
# API CONFIGURATION
# ============================================================================

# Configure Gemini with error handling
GEMINI_AVAILABLE = False
try:
    gemini_key = get_secret("gemini_key")
    if gemini_key:
        genai.configure(api_key=gemini_key)
        GEMINI_AVAILABLE = True
except Exception as e:
    GEMINI_AVAILABLE = False

BASE_URL = "https://newsdata.io/api/1/latest"

# SEC EDGAR Configuration
SEC_COMPANIES = {
    'Netflix': '0001065280',
    'Warner Bros Discovery': '0001437107',
    'Paramount': '0000813828',
}

RELEVANT_FORMS = ['8-K', '10-K', '10-Q', 'DEF 14A', 'DEFM14A', 'SC 13D', 'SC 13G', 
                  'S-4', '424B3', 'PREM14A', 'DEFA14A', 'SC TO-T', 'SC TO-C', 'SC 14D9']

SEC_FORM_DESCRIPTIONS = {
    '8-K': 'Material Event Report',
    '8-K/A': 'Material Event Report (Amended)',
    '10-K': 'Annual Report',
    '10-K/A': 'Annual Report (Amended)',
    '10-Q': 'Quarterly Report',
    '10-Q/A': 'Quarterly Report (Amended)',
    'DEF 14A': 'Definitive Proxy Statement',
    'DEFM14A': 'Definitive Merger Proxy Statement',
    'DEFA14A': 'Additional Proxy Materials',
    'PREM14A': 'Preliminary Proxy Statement',
    'SC 13D': 'Beneficial Ownership Report (Activist)',
    'SC 13D/A': 'Beneficial Ownership Report (Amended)',
    'SC 13G': 'Beneficial Ownership Report (Passive)',
    'SC 13G/A': 'Beneficial Ownership Report (Amended)',
    'S-4': 'Registration for M&A Securities',
    'S-4/A': 'Registration for M&A Securities (Amended)',
    '424B3': 'Prospectus Supplement',
    'SC TO-T': 'Tender Offer Statement (Third Party)',
    'SC TO-T/A': 'Tender Offer Statement (Amended)',
    'SC TO-C': 'Tender Offer Communication',
    'SC 14D9': 'Tender Offer Response',
    'SC 14D9/A': 'Tender Offer Response (Amended)',
}

# ============================================================================
# PYDANTIC SCHEMAS FOR STRUCTURED OUTPUT
# ============================================================================

class SummaryParagraph(BaseModel):
    """A single paragraph of the executive summary"""
    content: str = Field(
        description="The paragraph text. MUST include $ before all currency amounts. Use [N] for citations."
    )
    citations: List[int] = Field(
        description="List of citation numbers used in this paragraph"
    )

class DealTerms(BaseModel):
    """Structured deal information"""
    total_value: Optional[str] = Field(default=None)
    per_share_price: Optional[str] = Field(default=None)
    structure: Optional[str] = Field(default=None)
    assets_included: Optional[str] = Field(default=None)

class ExecutiveSummary(BaseModel):
    """Complete structured executive summary"""
    recent_developments: SummaryParagraph
    regulatory_status: SummaryParagraph
    deal_comparison: SummaryParagraph
    netflix_deal: Optional[DealTerms] = None
    paramount_deal: Optional[DealTerms] = None

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def safe_escape(text: Any) -> str:
    """Safely escape text for HTML display."""
    if text is None:
        return ""
    return html.escape(str(text))

def get_source_tier(source_id: Optional[str]) -> int:
    """Get the tier (1, 2, or 3) of a source. Lower is better."""
    if not source_id:
        return 3
    source_lower = source_id.lower()
    for pattern in TIER1_SOURCES:
        if pattern in source_lower:
            return 1
    for pattern in TIER2_SOURCES:
        if pattern in source_lower:
            return 2
    return 3

def is_trusted_source(source_id: Optional[str]) -> bool:
    """Check if source matches any trusted pattern."""
    if not source_id:
        return False
    source_lower = source_id.lower()
    return any(pattern in source_lower for pattern in TRUSTED_SOURCE_PATTERNS)

def extract_headline(title: Optional[str]) -> str:
    """Extract just the headline with aggressive truncation."""
    if not title:
        return "Untitled"
    title = str(title).strip()
    
    for i, char in enumerate(title):
        if i > 30 and char in '.!?':
            return safe_escape(title[:i+1])
        if i >= 80:
            last_space = title[:80].rfind(' ')
            if last_space > 40:
                return safe_escape(title[:last_space] + "...")
            return safe_escape(title[:77] + "...")
    
    result = title if len(title) <= 100 else title[:97] + "..."
    return safe_escape(result)

def categorize_article(article: Dict[str, Any]) -> Tuple[str, str, str]:
    """Categorize article by topic."""
    combined = f"{article.get('title', '')} {article.get('description', '')}".lower()
    if any(w in combined for w in ['ftc', 'doj', 'antitrust', 'regulatory', 'commission', 'review', 
                                    'approval', 'justice department', 'sec filing', 'hsr', 'hart-scott-rodino',
                                    'european commission', 'competition bureau', 'merger review', 'unlawful',
                                    'cma', 'competition and markets authority', 'uk regulator']):
        return 'regulatory', '⚖️', '#DC2626'
    elif any(w in combined for w in ['paramount', 'skydance', 'hostile', 'tender', 'competing', 'bidding war']):
        return 'bids', '🎯', '#D97706'
    elif any(w in combined for w in ['analyst', 'concern', 'impact', 'opinion', 'consolidation']):
        return 'analysis', '📊', '#7C3AED'
    return 'deal', '📋', '#2563EB'

def is_high_importance(article: Dict[str, Any]) -> bool:
    """Check if article is from a Tier 1 premium source or contains key keywords."""
    source = (article.get('source_id') or '').lower()
    title = (article.get('title') or '').lower()
    
    if get_source_tier(source) == 1:
        return True
    
    keywords = ['announce', 'official', 'reject', 'approve', 'board unanimously', 'confirms', 'closes']
    return any(k in title for k in keywords)

def is_relevant_article(article: Dict[str, Any]) -> bool:
    """Enhanced relevance check with configurable blocked sources."""
    source = (article.get('source_id') or '').lower()
    
    # Check blocked sources (configurable list)
    if any(blocked in source for blocked in BLOCKED_SOURCES):
        return False
    
    if not is_trusted_source(source):
        return False
    
    title = (article.get('title') or '').lower()
    description = (article.get('description') or '').lower()
    combined = f"{title} {description}"
    
    # Check irrelevant patterns
    for pattern in IRRELEVANT_PATTERNS:
        if re.search(pattern, combined, re.IGNORECASE):
            return False
    
    # Check deal context patterns (expanded)
    for pattern in MUST_CONTAIN_DEAL_CONTEXT:
        if re.search(pattern, combined, re.IGNORECASE):
            return True
    
    # Fallback: general acquisition context
    has_netflix = 'netflix' in combined
    has_warner = 'warner' in combined or 'wbd' in combined
    acquisition_words = ['acquire', 'acquisition', 'merger', 'takeover', 'bid', 'hostile', 
                         'tender offer', 'buyout', 'purchase agreement', 'deal']
    has_acquisition = any(word in combined for word in acquisition_words)
    
    return has_netflix and has_warner and has_acquisition

def get_url_path(url: Optional[str]) -> str:
    """Extract URL path for similarity comparison."""
    if not url:
        return ""
    try:
        parsed = urlparse(str(url))
        return parsed.path.lower().strip('/')
    except Exception:
        return ""

def title_similarity(t1: Optional[str], t2: Optional[str]) -> float:
    """Calculate title similarity."""
    if not t1 or not t2:
        return 0.0
    t1_clean = re.sub(r'[^\w\s]', '', str(t1).lower())
    t2_clean = re.sub(r'[^\w\s]', '', str(t2).lower())
    return SequenceMatcher(None, t1_clean, t2_clean).ratio()

def url_path_similarity(url1: Optional[str], url2: Optional[str]) -> float:
    """Calculate URL path similarity."""
    path1 = get_url_path(url1)
    path2 = get_url_path(url2)
    if not path1 or not path2:
        return 0.0
    return SequenceMatcher(None, path1, path2).ratio()

def deduplicate_articles(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Enhanced deduplication with higher threshold and URL path check.
    Uses 0.75 similarity threshold and checks both title and URL path.
    """
    if not articles:
        return []
    
    # Sort by source tier (best sources first)
    sorted_articles = sorted(articles, key=lambda a: get_source_tier(a.get('source_id', '')))
    
    unique: List[Dict[str, Any]] = []
    seen_titles: List[str] = []
    seen_urls: List[str] = []
    
    for article in sorted_articles:
        title = article.get('title', '')
        url = article.get('link', '')
        
        if not title:
            continue
            
        # Check title similarity (threshold: 0.75)
        is_title_duplicate = any(title_similarity(title, seen) > 0.75 for seen in seen_titles)
        
        # Check URL path similarity (threshold: 0.85)
        is_url_duplicate = any(url_path_similarity(url, seen) > 0.85 for seen in seen_urls)
        
        if not is_title_duplicate and not is_url_duplicate:
            unique.append(article)
            seen_titles.append(title)
            if url:
                seen_urls.append(url)
    
    return unique

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

def format_date(date_str: Optional[str]) -> str:
    """Format date for display."""
    dt = parse_pubdate(date_str)
    if dt == datetime.min:
        return "Unknown"
    diff = (datetime.utcnow() - dt).days
    if diff == 0:
        return "Today"
    if diff == 1:
        return "Yesterday"
    if diff < 7:
        return f"{diff} days ago"
    return dt.strftime("%b %d, %Y")

def format_date_full(date_str: Optional[str]) -> str:
    """Format date with full timestamp."""
    dt = parse_pubdate(date_str)
    if dt == datetime.min:
        return "Unknown"
    return dt.strftime("%B %d, %Y")

def format_source(source_id: Optional[str]) -> str:
    """Format source name for display."""
    names = {
        'netflix': 'Netflix', 'netflix_ir': 'Netflix IR', 'wbd_ir': 'WBD IR',
        'wbd': 'Warner Bros. Discovery', 'paramount': 'Paramount',
        'reuters': 'Reuters', 'bloomberg': 'Bloomberg', 'wsj': 'Wall Street Journal',
        'apnews': 'AP News', 'afp': 'AFP', 'bbc': 'BBC', 'cnn': 'CNN', 'cnbc': 'CNBC',
        'nytimes': 'New York Times', 'latimes': 'Los Angeles Times',
        'chicagotribune': 'Chicago Tribune', 'usatoday': 'USA Today',
        'nypost': 'New York Post', 'bostonglobo': 'Boston Globe',
        'washingtonpost': 'Washington Post',
        'variety': 'Variety', 'deadline': 'Deadline', 'hollywoodreporter': 'Hollywood Reporter',
        'indiewire': 'IndieWire', 'screendaily': 'Screen Daily', 'vulture': 'Vulture',
        'rollingstone': 'Rolling Stone', 'ew': 'Entertainment Weekly', 'avclub': 'AV Club',
        'thewrap': 'The Wrap',
        'globeandmail': 'Globe and Mail', 'theglobeandmail': 'Globe and Mail',
        'cbc': 'CBC News', 'cbcnews': 'CBC News', 
        'nationalpost': 'National Post', 'financialpost': 'Financial Post',
        'thestar': 'Toronto Star', 'bnnbloomberg': 'BNN Bloomberg',
        'bnnbloomberg_ca': 'BNN Bloomberg', 'globalnews': 'Global News',
        'winnipegfreepress': 'Winnipeg Free Press', 'halifaxtoday': 'Halifax Today',
        'ctv': 'CTV News', 'ctvnews': 'CTV News',
        'ft': 'Financial Times', 'marketwatch': 'MarketWatch', 'barrons': "Barron's",
        'seekingalpha': 'Seeking Alpha', 'benzinga': 'Benzinga', 'thestreet': 'The Street',
        'foxbusiness': 'Fox Business', 'moneycontrol': 'Moneycontrol',
        'livemint': 'Mint', 'economictimes': 'Economic Times',
        'economictimes_indiatimes': 'Economic Times',
        'techcrunch': 'TechCrunch', 'theverge': 'The Verge', 'engadget': 'Engadget',
        'arstechnica': 'Ars Technica', 'wired': 'Wired', 'fastcompany': 'Fast Company',
        'businessinsider': 'Business Insider', 'insider': 'Insider', 'forbes': 'Forbes',
        'nbcnews': 'NBC News', 'abcnews': 'ABC News', 'cbsnews': 'CBS News',
        'foxnews': 'Fox News', 'msnbc': 'MSNBC', 'abc7': 'ABC7',
        'guardian': 'The Guardian', 'theguardian': 'The Guardian',
        'independent': 'The Independent', 'independentuk': 'The Independent',
        'telegraph': 'The Telegraph', 'euronews': 'Euronews', 'dw': 'DW',
        'france24': 'France 24', 'aljazeera': 'Al Jazeera', 'scmp': 'SCMP',
        'prnewswire': 'PR Newswire', 'prnewswire_co_uk': 'PR Newswire',
        'businesswire': 'Business Wire', 'globenewswire': 'GlobeNewswire', 'cision': 'Cision',
        'ftc': 'FTC', 'doj': 'DOJ', 'justice': 'DOJ',
        'eu_commission': 'EU Commission', 'europa': 'EU Commission', 'ec': 'EU Commission',
        'sec': 'SEC', 'competitionbureau': 'Competition Bureau',
        'cma': 'UK CMA', 'gov_uk': 'UK Gov',
    }
    if not source_id:
        return 'Unknown'
    formatted = names.get(source_id.lower(), source_id.replace('_', ' ').title())
    if len(formatted) > 30:
        formatted = formatted[:27] + "..."
    return safe_escape(formatted)

# ============================================================================
# SUMMARY GENERATION
# ============================================================================

def format_summary_html(summary: ExecutiveSummary, source_urls: Dict[int, str]) -> str:
    """Convert structured summary to formatted HTML."""
    
    def format_paragraph_with_citations(para: SummaryParagraph) -> str:
        text = para.content
        for num in sorted(para.citations, reverse=True):
            url = source_urls.get(num, "#")
            text = text.replace(
                f"[{num}]",
                f'<sup><a href="{safe_escape(url)}" target="_blank">[{num}]</a></sup>'
            )
        return text
    
    paragraphs = ['<div class="summary-container">']
    
    p1 = format_paragraph_with_citations(summary.recent_developments)
    paragraphs.append(f'<p class="summary-paragraph">{p1}</p>')
    
    p2 = format_paragraph_with_citations(summary.regulatory_status)
    paragraphs.append(f'<p class="summary-paragraph">{p2}</p>')
    
    p3 = format_paragraph_with_citations(summary.deal_comparison)
    paragraphs.append(f'<p class="summary-paragraph">{p3}</p>')
    
    paragraphs.append('</div>')
    
    return "\n".join(paragraphs)

def generate_structured_summary(news_articles: List[Dict[str, Any]], source_urls: Dict[int, str]) -> str:
    """Generate executive summary using Gemini Structured Outputs."""
    
    # Limit articles for performance (max 15)
    limited_articles = news_articles[:MAX_ARTICLES_FOR_SUMMARY]
    
    source_list = []
    for i, a in enumerate(limited_articles, 1):
        desc = (a.get('description') or '').replace('\n', ' ').strip()[:MAX_DESCRIPTION_LENGTH]
        if desc:
            source_list.append(f"[{i}] {a['source']} ({a['date']}): {a['title']}\n    Content: {desc}")
        else:
            source_list.append(f"[{i}] {a['source']} ({a['date']}): {a['title']}")
    
    prompt = f"""You are a senior M&A analyst preparing a detailed factual briefing on the Netflix–Warner Bros. Discovery transaction for institutional investors, legal counsel, and corporate executives.

SOURCE MATERIALS:
{chr(10).join(source_list)}

Generate a comprehensive executive summary with three detailed paragraphs based on the sources above.

CRITICAL STYLE REQUIREMENTS:
- Write in neutral, objective, third-person voice appropriate for a legal or financial memorandum
- State facts without editorial commentary, speculation, or subjective characterization
- Avoid words like "significant," "substantial," "major," "notable," "interesting," or other qualitative descriptors unless directly quoting a source
- Do not characterize market reactions, sentiment, or implications beyond what sources explicitly state
- Use precise language and specific dollar amounts where available
- Present competing claims without favoring either party

PARAGRAPH 1 - RECENT DEVELOPMENTS (recent_developments):
Provide a detailed account of the last 48-72 hours of transaction activity. Include:
- Specific offers made with exact dollar amounts (total value and per-share price)
- Board actions and responses (acceptances, rejections, counteroffers)
- Public statements by named executives (David Zaslav, Ted Sarandos, Larry Ellison, Shari Redstone)
- Deadline extensions or amendments to existing offers
- Any changes to deal structure (e.g., shift from stock to all-cash)
This paragraph should be 5-7 sentences with specific facts and figures.

PARAGRAPH 2 - REGULATORY AND LEGAL STATUS (regulatory_status):
Provide detailed coverage of each regulatory body's position and any legal proceedings:
- DOJ (Department of Justice): Current review status, any statements on antitrust concerns, timeline indicators
- FTC (Federal Trade Commission): Involvement status, any public positions or concerns raised
- European Commission (EC): Notification status, Phase I/II review timeline, any conditions being discussed
- UK CMA (Competition and Markets Authority): Review status, any preliminary findings or concerns
- Canadian Competition Bureau (CCB): Filing status, review timeline
- Court proceedings: Any lawsuits filed (e.g., Paramount vs. WBD), specific legal arguments made, hearing dates
- Include specific quotes from regulators or legal filings where available
This paragraph should be 6-8 sentences covering each relevant regulatory development.

PARAGRAPH 3 - DEAL STRUCTURE COMPARISON (deal_comparison):
Provide a detailed comparison of all competing offers:
- Netflix offer: Total enterprise value, per-share price, payment structure (cash/stock mix), which assets are included (Warner Bros. studios, HBO, Max streaming platform, DC Entertainment), treatment of assets not included, financing arrangements
- Paramount/Skydance offer: Total value, per-share price, all-cash structure details, Larry Ellison/Oracle financing role, which WBD assets targeted
- Timeline comparison: Tender offer expiration dates, shareholder vote dates, regulatory approval deadlines, expected closing dates
- Any conditions or contingencies attached to each offer
This paragraph should be 5-7 sentences with specific financial terms.

FORMATTING RULES:
1. Include $ before ALL currency amounts: "$82.7 billion", "$30 per share", "$108 billion"
2. Use [N] format for citations where N is the source number (1-{len(limited_articles)})
3. Write flowing prose - absolutely no bullet points or lists
4. If specific information is not available in the sources, state "not disclosed in available reporting" - do not speculate
5. Include the citation numbers you use in the 'citations' list for each paragraph
6. Cite multiple sources for key claims where possible"""

    try:
        model = genai.GenerativeModel(
            "gemini-2.0-flash-exp",
            generation_config={
                "response_mime_type": "application/json",
                "response_schema": ExecutiveSummary.model_json_schema()
            }
        )
        
        response = model.generate_content(prompt)
        summary = ExecutiveSummary.model_validate_json(response.text)
        
        return format_summary_html(summary, source_urls)
    except Exception as e:
        raise RuntimeError(f"Structured summary generation failed: {str(e)}")

def generate_fallback_summary(news_articles: List[Dict[str, Any]], source_urls: Dict[int, str]) -> str:
    """Fallback summary generation if structured output fails."""
    
    limited_articles = news_articles[:MAX_ARTICLES_FOR_SUMMARY]
    
    source_list = []
    for i, a in enumerate(limited_articles, 1):
        desc = (a.get('description') or '').replace('\n', ' ').strip()[:300]
        if desc:
            source_list.append(f"[{i}] {a['source']}: {a['title']} - {desc}")
        else:
            source_list.append(f"[{i}] {a['source']}: {a['title']}")
    
    prompt = f"""Write a detailed 3-paragraph executive summary about the Netflix–Warner Bros. Discovery transaction for institutional investors.

SOURCES:
{chr(10).join(source_list)}

STYLE: Neutral, objective, third-person voice. State facts without editorial commentary. Avoid qualitative descriptors like "significant" or "major." Appropriate for a legal memorandum.

STRUCTURE:
- Paragraph 1 (5-7 sentences): Recent developments from last 48-72 hours - specific offers with dollar amounts, board actions, executive statements from Zaslav/Sarandos/Ellison
- Paragraph 2 (6-8 sentences): Regulatory status covering DOJ, FTC, European Commission, UK CMA, Canadian Competition Bureau - what each has said or done, any lawsuits filed
- Paragraph 3 (5-7 sentences): Deal comparison - Netflix offer terms vs Paramount/Skydance terms, timelines, conditions

RULES:
- Use $ before all dollar amounts ($82.7 billion, $30 per share)
- Cite sources as [1], [2], etc.
- No bullet points, flowing prose only
- If information not available, say "not disclosed in available reporting"
- Be specific with numbers and names"""

    try:
        model = genai.GenerativeModel("gemini-2.0-flash-exp")
        response = model.generate_content(prompt)
        
        text = response.text.strip()
        
        for num, url in source_urls.items():
            text = text.replace(f"[{num}]", f'<sup><a href="{safe_escape(url)}" target="_blank">[{num}]</a></sup>')
        
        paragraphs = text.split('\n\n')
        html_parts = ['<div class="summary-container">']
        html_parts.extend([f'<p class="summary-paragraph">{p.strip()}</p>' for p in paragraphs if p.strip()])
        html_parts.append('</div>')
        
        return "\n".join(html_parts)
    except Exception as e:
        raise RuntimeError(f"Fallback summary generation failed: {str(e)}")

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
# SEC EDGAR FETCHING
# ============================================================================

def fetch_sec_company(company_name: str, cik: str) -> List[Dict[str, Any]]:
    """Fetch SEC filings for a single company with error handling."""
    filings = []
    try:
        headers = {
            'User-Agent': 'Netflix-WBD-Monitor/1.0 (Enterprise Research)',
            'Accept-Encoding': 'gzip, deflate'
        }
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        resp = requests.get(url, headers=headers, timeout=API_TIMEOUT_SEC)
        
        if resp.status_code == 200:
            data = resp.json()
            recent = data.get('filings', {}).get('recent', {})
            
            if recent:
                forms = recent.get('form', [])
                dates = recent.get('filingDate', [])
                accessions = recent.get('accessionNumber', [])
                primary_docs = recent.get('primaryDocument', [])
                doc_descriptions = recent.get('primaryDocDescription', [])
                
                cutoff = (datetime.utcnow() - timedelta(days=30)).strftime('%Y-%m-%d')
                
                for i in range(min(30, len(forms))):
                    if dates[i] >= cutoff:
                        form_type = forms[i]
                        if any(f in form_type for f in RELEVANT_FORMS):
                            accession_clean = accessions[i].replace('-', '')
                            cik_clean = cik.lstrip('0')
                            filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik_clean}/{accession_clean}/{primary_docs[i]}"
                            
                            description = SEC_FORM_DESCRIPTIONS.get(form_type, form_type)
                            if doc_descriptions and i < len(doc_descriptions) and doc_descriptions[i]:
                                description = doc_descriptions[i]
                            
                            filings.append({
                                'company': company_name,
                                'form': form_type,
                                'form_description': SEC_FORM_DESCRIPTIONS.get(form_type, form_type),
                                'date': dates[i],
                                'url': filing_url,
                                'title': description,
                                'filename': primary_docs[i]
                            })
    except requests.exceptions.Timeout:
        pass  # Silently handle timeout
    except requests.exceptions.RequestException:
        pass  # Silently handle network errors
    except Exception:
        pass  # Silently handle other errors
    
    return filings

# ============================================================================
# CACHED DATA LOADING
# ============================================================================

def _hash_ignore_secrets(secrets):
    """Hash function that ignores secrets for caching."""
    return "static_hash"

@st.cache_data(ttl=CACHE_TTL_SEC, show_spinner=False)
def fetch_sec_filings() -> List[Dict[str, Any]]:
    """Fetch recent SEC filings for Netflix, WBD, and Paramount with caching."""
    all_filings = []
    
    # Use ThreadPoolExecutor for parallel SEC fetching
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(fetch_sec_company, name, cik): name 
            for name, cik in SEC_COMPANIES.items()
        }
        for future in futures:
            try:
                filings = future.result(timeout=API_TIMEOUT_SEC + 5)
                all_filings.extend(filings)
            except Exception:
                continue
    
    all_filings.sort(key=lambda x: x['date'], reverse=True)
    return all_filings[:20]

@st.cache_data(ttl=CACHE_TTL_DATA, show_spinner=False)
def load_all_data(_cache_key: str) -> Dict[str, Any]:
    """
    Load and process all article data with comprehensive error handling.
    _cache_key is used to allow manual invalidation via session_state.
    """
    archived = []
    fresh = []
    worksheet = None
    archive_error = None
    api_error = None
    saved_count = 0
    
    # Load archived articles from Google Sheets
    cred_source = "unknown"
    try:
        gcp_creds, cred_source = get_gcp_credentials()
        sheet_name = get_secret("sheet_name")
        if gcp_creds and sheet_name:
            gc = gspread.service_account_from_dict(gcp_creds)
            sh = gc.open(sheet_name)
            worksheet = sh.sheet1
            archived = worksheet.get_all_records() or []
        elif not gcp_creds:
            archive_error = "GCP credentials not found"
        elif not sheet_name:
            archive_error = "Sheet name not configured"
    except gspread.exceptions.APIError as e:
        archive_error = f"Sheets API error: {str(e)[:80]}"
    except gspread.exceptions.SpreadsheetNotFound:
        archive_error = f"Spreadsheet '{sheet_name}' not found"
    except Exception as e:
        archive_error = f"Archive: {type(e).__name__}: {str(e)[:60]}"
    
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
            worksheet.append_rows(rows, value_input_option='RAW')
            saved_count = len(new_articles)
        except Exception:
            pass  # Silently handle save errors
    
    all_articles = archived + new_articles
    
    # Filter, deduplicate, and sort
    relevant = [a for a in all_articles if is_relevant_article(a)]
    unique = deduplicate_articles(relevant)
    unique.sort(key=lambda x: parse_pubdate(x.get('pubDate')), reverse=True)
    
    # Categorize articles
    for a in unique:
        cat, icon, color = categorize_article(a)
        a['category'] = cat
        a['cat_icon'] = icon
        a['cat_color'] = color
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
        'cred_source': cred_source,
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
        st.markdown(f"**{date}** · {marker}{source}  \n[{title}]({link})")
        
        if i < min(max_items, len(articles_list)) - 1:
            st.markdown("<hr style='margin: 0.5rem 0; border: none; border-top: 1px solid #E5E5EA;'>", unsafe_allow_html=True)

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
            f"**{date}** · {emoji} {company} · **{form_type}** ({form_desc})  \n"
            f"[{title}]({url})"
        )
        
        if i < min(max_items, len(filings_list)) - 1:
            st.markdown("<hr style='margin: 0.5rem 0; border: none; border-top: 1px solid #E5E5EA;'>", unsafe_allow_html=True)

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
        data = {'articles': [], 'archived_count': 0, 'filtered_out': 0, 'saved_count': 0, 'archive_error': None, 'api_error': None, 'cred_source': 'error'}
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
    if data.get('archive_error'):
        st.markdown(f'<span class="status-error">⚠ Archive error</span>', unsafe_allow_html=True)
        st.caption(f"_{data['archive_error'][:80]}_")
    if data.get('api_error'):
        st.markdown(f'<span class="status-warning">API warning</span>', unsafe_allow_html=True)
    
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

# DEBUG: Show credential status
client_email_check = os.environ.get("GCP_CLIENT_EMAIL", "not set")
st.markdown(f"**Debug:** Cred source: `{data.get('cred_source', 'unknown')}` | Email: `{client_email_check[:30]}...` | Archived: `{data.get('archived_count', 0)}`")

# DEBUG: Show any data loading errors prominently
if data.get('archive_error'):
    st.markdown(f"**⚠️ Archive Error:** `{data['archive_error']}`")
if data.get('api_error'):
    st.markdown(f"**⚠️ API Error:** `{data['api_error']}`")

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
    source_urls = {i: a['url'] for i, a in enumerate(all_news_sources, 1)}

    try:
        with st.spinner("Generating summary..."):
            try:
                summary_html = generate_structured_summary(all_news_sources, source_urls)
            except Exception:
                summary_html = generate_fallback_summary(all_news_sources, source_urls)
            
            st.markdown(summary_html, unsafe_allow_html=True)
            
            with st.expander("View Sources"):
                for i, a in enumerate(all_news_sources, 1):
                    title_preview = a['title'][:55] + "..." if len(a['title']) > 55 else a['title']
                    st.markdown(
                        f'<p class="source-list-item"><strong>[{i}]</strong> '
                        f'<a href="{safe_escape(a["url"])}" target="_blank">{safe_escape(a["source"])}</a> '
                        f'— {safe_escape(title_preview)}</p>',
                        unsafe_allow_html=True
                    )
                    
    except Exception as e:
        st.markdown(
            '<div class="summary-container"><p class="summary-paragraph">'
            'Summary generation encountered an error. Please refresh to try again.</p></div>',
            unsafe_allow_html=True
        )
else:
    if not GEMINI_AVAILABLE:
        st.markdown(
            '<div class="summary-container"><p class="summary-paragraph">'
            'AI summary unavailable. Please configure Gemini API key.</p></div>',
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            '<div class="summary-container"><p class="summary-paragraph">'
            'No articles available for summary.</p></div>',
            unsafe_allow_html=True
        )

st.divider()

# Recent Coverage
st.markdown('<p class="section-title">RECENT COVERAGE</p>', unsafe_allow_html=True)

tab_all, tab_deal, tab_reg, tab_bids, tab_sec, tab_analysis = st.tabs([
    f"All ({len(recent)})",
    f"Deal ({cat_counts['deal']})",
    f"Regulatory ({cat_counts['regulatory']})",
    f"Bids ({cat_counts['bids']})",
    f"SEC Filings ({len(sec_filings)})",
    f"Analysis ({cat_counts['analysis']})"
])

with tab_all:
    render_feed(recent, 25)

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
