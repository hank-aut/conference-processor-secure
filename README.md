# Conference Attendee Processor

A secure system for processing conference attendee lists with email discovery and Salesforce classification.

## Features

- **Email Discovery**: Find attendee email addresses using Apollo API and pattern analysis
- **Salesforce Integration**: Classify attendees based on existing Salesforce relationships
- **ROE Qualification**: Apply Rules of Engagement for lead qualification
- **Multi-format Output**: Generate Excel and CSV reports
- **Web Interface**: Streamlit-based UI for easy processing

## Security

✅ **Secure Credential Management**
- Environment variables for API keys
- No hardcoded credentials
- Comprehensive `.gitignore`

## Setup

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Environment Variables**
   ```bash
   cp .env.example .env
   # Edit .env with your actual credentials
   ```

3. **Required Credentials**
   - Apollo API key
   - Salesforce username, password, and security token

## Usage

### Command Line
```bash
python conference_processor.py attendees.csv
```

### Web Interface
```bash
streamlit run streamlit_app.py
```

## Deployment

### Streamlit Cloud
1. Connect this repository to Streamlit Cloud
2. Add secrets in Streamlit dashboard:
   - `APOLLO_API_KEY`
   - `SALESFORCE_USERNAME` 
   - `SALESFORCE_PASSWORD`
   - `SALESFORCE_TOKEN`

## File Structure

- `conference_processor.py` - Main processing logic
- `streamlit_app.py` - Web interface
- `requirements.txt` - Python dependencies
- `.env.example` - Environment variable template
- `.gitignore` - Security exclusions

## CSV Format

Required columns:
- First Name
- Last Name  
- Company
- Job Title

## Output Classifications

1. **Current Customers** - Active Salesforce customers
2. **Open Opportunities** - Companies with active sales opportunities
3. **Qualified Prospects** - ROE-qualified leads
4. **No SF Match** - No existing Salesforce relationship
5. **Disqualified** - Recent activity (ROE violations)

## Security Note

⚠️ **Never commit your `.env` file or expose API credentials in code**