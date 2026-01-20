# Vedere Vision - Netflix–WBD Transaction Monitor

Enterprise-grade M&A monitoring dashboard.

## Deploy to Render (5 minutes)

### 1. Create GitHub Repo
```bash
gh repo create vedere-vision --public --clone
cd vedere-vision
# Copy app.py, requirements.txt, Dockerfile, render.yaml, .gitignore here
git add .
git commit -m "Initial commit"
git push origin main
```

### 2. Deploy on Render
1. Go to [render.com](https://render.com) → New → **Web Service**
2. Connect your `vedere-vision` repo
3. Settings will auto-detect from `render.yaml`
4. Click **Create Web Service**

### 3. Add Environment Variables
In Render dashboard → Environment → Add the following:

| Key | Value |
|-----|-------|
| `newsdata_key` | Your NewsData.io API key |
| `gemini_key` | Your Google Gemini API key |
| `sheet_name` | `Netflix-WBD-Monitor-Archive` |
| `GCP_SERVICE_ACCOUNT` | Your GCP service account JSON (as single line) |

**To convert GCP JSON to single line:**
```bash
cat service_account.json | jq -c .
```

### 4. Custom Domain (vision.vedere.ca)
1. Render dashboard → Settings → Custom Domains
2. Add `vision.vedere.ca`
3. Add DNS records at your registrar:
   ```
   Type: CNAME
   Host: vision
   Value: vedere-vision.onrender.com
   ```

## Local Development
```bash
# Create .env file (never commit this)
echo "newsdata_key=your_key" >> .env
echo "gemini_key=your_key" >> .env

# Run locally
streamlit run app.py
```

## Architecture
- **News**: NewsData.io API (50+ sources)
- **SEC Filings**: Direct EDGAR API
- **AI Summaries**: Google Gemini 2.0 Flash
- **Archive**: Google Sheets
- **Hosting**: Render ($7/mo, always on)
