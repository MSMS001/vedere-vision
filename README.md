# Vedere Vision

Streamlit dashboard for monitoring Netflix-Warner Bros Discovery transaction news and SEC filings.

## Features

- News aggregation from 50+ sources (NewsData.io API)
- - SEC EDGAR filings for Netflix, WBD, Paramount
  - - AI summaries via Google Gemini
    - - Google Sheets archiving
     
      - ## Requirements
     
      - - Python 3.11+
        - - NewsData.io API key
          - - Google Gemini API key
            - - GCP service account for Sheets
             
              - ## Usage
             
              - ```
                streamlit run app.py
                ```

                ## Deploy

                Uses Render. See render.yaml for config.
