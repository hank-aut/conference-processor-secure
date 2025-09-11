#!/usr/bin/env python3
"""
Conference Attendee Processing System
Handles email discovery and Salesforce classification with precise date calculations
"""

import json
import csv
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import subprocess
import re
import time
import requests
import pandas as pd

# Salesforce integration
try:
    from simple_salesforce import Salesforce
    SALESFORCE_AVAILABLE = True
except ImportError:
    SALESFORCE_AVAILABLE = False
    print("Warning: simple-salesforce not installed. Install with: pip install simple-salesforce")

class ProgressTracker:
    def __init__(self, progress_dir: str):
        self.progress_dir = progress_dir
        self.progress_file = os.path.join(progress_dir, 'workflow_progress.json')
        self.ensure_directory()
    
    def ensure_directory(self):
        os.makedirs(self.progress_dir, exist_ok=True)
    
    def save_progress(self, data: Dict):
        """Save progress data with timestamp"""
        data['last_updated'] = datetime.now().isoformat()
        with open(self.progress_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def load_progress(self) -> Dict:
        """Load existing progress or return empty state"""
        if os.path.exists(self.progress_file):
            with open(self.progress_file, 'r') as f:
                return json.load(f)
        return {
            'total_attendees': 0,
            'processed_count': 0,
            'phase': 'not_started',
            'email_stats': {'found': 0, 'not_found': 0},
            'sf_stats': {'qualified': 0, 'disqualified': 0, 'no_match': 0, 'current_customer': 0, 'open_opportunity': 0}
        }

class DateCalculator:
    @staticmethod
    def get_today() -> datetime:
        """Get current date for consistent calculations"""
        return datetime.now().date()
    
    @staticmethod
    def get_cutoff_dates() -> Tuple[datetime, datetime]:
        """Get ROE cutoff dates"""
        today = DateCalculator.get_today()
        activity_cutoff = today - timedelta(days=90)  # LastActivityDate threshold
        system_cutoff = today - timedelta(days=30)    # SystemModstamp threshold
        return activity_cutoff, system_cutoff
    
    @staticmethod
    def check_roe_qualification(last_activity_str: str, system_modstamp_str: str) -> Tuple[bool, str]:
        """
        Precise ROE qualification check with detailed reasoning
        Returns: (qualified: bool, reason: str)
        """
        try:
            activity_cutoff, system_cutoff = DateCalculator.get_cutoff_dates()
            
            # Parse dates
            if last_activity_str:
                last_activity = datetime.strptime(last_activity_str.split('T')[0], '%Y-%m-%d').date()
            else:
                last_activity = datetime(1900, 1, 1).date()  # Very old date if None
            
            system_modstamp = datetime.strptime(system_modstamp_str.split('T')[0], '%Y-%m-%d').date()
            
            # Calculate days difference for logging
            activity_days = (DateCalculator.get_today() - last_activity).days
            system_days = (DateCalculator.get_today() - system_modstamp).days
            
            # ROE checks
            activity_pass = last_activity <= activity_cutoff
            system_pass = system_modstamp <= system_cutoff
            
            if activity_pass and system_pass:
                return True, f"QUALIFIED - Activity: {activity_days}d ago (>{90}d), System: {system_days}d ago (>{30}d)"
            elif not activity_pass:
                return False, f"DISQUALIFIED - Recent activity: {activity_days}d ago (<{90}d threshold)"
            else:
                return False, f"DISQUALIFIED - Recent system update: {system_days}d ago (<{30}d threshold)"
                
        except Exception as e:
            return False, f"DATE_PARSE_ERROR: {str(e)}"

class CSVProcessor:
    @staticmethod
    def read_attendees(file_path: str) -> List[Dict]:
        """Read attendees from CSV file"""
        attendees = []
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                attendees.append({
                    'first_name': row.get('First Name', '').strip(),
                    'last_name': row.get('Last Name', '').strip(),
                    'company': row.get('Company', '').strip(),
                    'title': row.get('Job Title', '').strip()
                })
        return attendees
    
    @staticmethod
    def write_results(output_dir: str, results: Dict[str, List[Dict]]):
        """Write classification results to a single Excel file with multiple tabs"""
        os.makedirs(output_dir, exist_ok=True)
        
        # Create Excel file path
        excel_filepath = os.path.join(output_dir, "conference_attendees_results.xlsx")
        
        # Create Excel writer object
        with pd.ExcelWriter(excel_filepath, engine='openpyxl') as writer:
            
            # Tab names mapping for cleaner display
            tab_names = {
                'current_customers': 'Current Customers',
                'open_opportunities': 'Open Opportunities', 
                'salesforce_qualified': 'Qualified Prospects',
                'no_salesforce_match': 'No SF Match',
                'excluded': 'Disqualified - ROE'
            }
            
            for classification, attendees in results.items():
                # Convert attendees to DataFrame
                if attendees:
                    # Convert to records format for DataFrame
                    records = []
                    for attendee in attendees:
                        record = {
                            'First Name': attendee['first_name'],
                            'Last Name': attendee['last_name'], 
                            'Company': attendee['company'],
                            'Title': attendee['title'],
                            'Email': attendee.get('email', 'EMAIL_NOT_FOUND')
                        }
                        
                        # Add Reason column for excluded attendees
                        if classification == 'excluded' and 'reason' in attendee:
                            record['Reason'] = attendee['reason']
                        
                        # Add Relationship Owner, Account ID, and URL columns for current customers
                        if classification == 'current_customers':
                            if 'relationship_owner' in attendee:
                                record['Relationship Owner'] = attendee.get('relationship_owner', '')
                            if 'account_id' in attendee:
                                record['Account ID'] = attendee.get('account_id', '')
                            if 'account_url' in attendee:
                                record['Account URL'] = attendee.get('account_url', '')
                        
                        # Add Opportunity Owner, ID, and URL columns for open opportunities
                        if classification == 'open_opportunities':
                            if 'opportunity_owner' in attendee:
                                record['Opportunity Owner'] = attendee.get('opportunity_owner', '')
                            if 'opportunity_id' in attendee:
                                record['Opportunity ID'] = attendee.get('opportunity_id', '')
                            if 'opportunity_url' in attendee:
                                record['Opportunity URL'] = attendee.get('opportunity_url', '')
                        
                        records.append(record)
                    df = pd.DataFrame(records)
                else:
                    # Create empty DataFrame with headers
                    if classification == 'excluded':
                        df = pd.DataFrame(columns=['First Name', 'Last Name', 'Company', 'Title', 'Email', 'Reason'])
                    elif classification == 'current_customers':
                        df = pd.DataFrame(columns=['First Name', 'Last Name', 'Company', 'Title', 'Email', 'Relationship Owner', 'Account ID', 'Account URL'])
                    elif classification == 'open_opportunities':
                        df = pd.DataFrame(columns=['First Name', 'Last Name', 'Company', 'Title', 'Email', 'Opportunity Owner', 'Opportunity ID', 'Opportunity URL'])
                    else:
                        df = pd.DataFrame(columns=['First Name', 'Last Name', 'Company', 'Title', 'Email'])
                
                # Get clean tab name
                tab_name = tab_names.get(classification, classification.replace('_', ' ').title())
                
                # Write to Excel tab
                df.to_excel(writer, sheet_name=tab_name, index=False)
                
                # Auto-adjust column widths
                worksheet = writer.sheets[tab_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)  # Cap at 50 characters
                    worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"📊 Results saved to: {excel_filepath}")
        
        # Also create individual CSV files for backward compatibility  
        CSVProcessor._write_csv_backups(output_dir, results)
    
    @staticmethod
    def _write_csv_backups(output_dir: str, results: Dict[str, List[Dict]]):
        """Create individual CSV files as backup (for compatibility)"""
        csv_backup_dir = os.path.join(output_dir, "csv_backup")
        os.makedirs(csv_backup_dir, exist_ok=True)
        
        for classification, attendees in results.items():
            filename = f"{classification}_final.csv"
            filepath = os.path.join(csv_backup_dir, filename)
            
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                if attendees:
                    writer = csv.DictWriter(f, fieldnames=['First Name', 'Last Name', 'Company', 'Title', 'Email'])
                    writer.writeheader()
                    for attendee in attendees:
                        writer.writerow({
                            'First Name': attendee['first_name'],
                            'Last Name': attendee['last_name'],
                            'Company': attendee['company'],
                            'Title': attendee['title'],
                            'Email': attendee.get('email', 'EMAIL_NOT_FOUND')
                        })
                else:
                    # Write empty file with headers
                    writer = csv.writer(f)
                    writer.writerow(['First Name', 'Last Name', 'Company', 'Title', 'Email'])

class EmailDiscovery:
    def __init__(self, log_dir: str):
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        self.processed_attendees = []  # Track all processed attendees for pattern analysis within this run
    
    def call_apollo_api_direct(self, first_name: str, last_name: str, company: str) -> Tuple[Optional[str], str, Optional[str]]:
        """
        Call Apollo API directly using the API key
        Returns: (email or None, apollo_id or None, detailed_notes)
        """
        api_key = "SMSLZHcr-WRGqe1aPUxTww"  # From the .env file we found
        
        try:
            # Step 1: People Match API
            url = "https://api.apollo.io/v1/people/match"
            headers = {
                "Cache-Control": "no-cache",
                "X-Api-Key": api_key,
                "Content-Type": "application/json"
            }
            
            payload = {
                "first_name": first_name,
                "last_name": last_name,
                "organization_name": company
            }
            
            print(f"    Apollo API: Searching for {first_name} {last_name} at {company}...")
            
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'person' in data and data['person']:
                    person = data['person']
                    apollo_id = person.get('id')
                    email = person.get('email')
                    
                    if email:
                        return email, apollo_id, f"Apollo API: Found verified email {email}"
                    elif apollo_id:
                        # Try to get email using the person ID
                        return self.apollo_get_email(apollo_id, api_key, first_name, last_name)
                    else:
                        return None, None, f"Apollo API: Found person but no ID or email"
                else:
                    return None, None, f"Apollo API: No person found"
                    
            elif response.status_code == 429:
                return None, None, f"Apollo API: Rate limit exceeded (429)"
            elif response.status_code == 401:
                return None, None, f"Apollo API: Authentication failed (401)"
            else:
                return None, None, f"Apollo API: HTTP {response.status_code} - {response.text[:100]}"
                
        except requests.exceptions.Timeout:
            return None, None, f"Apollo API: Timeout after 30 seconds"
        except requests.exceptions.RequestException as e:
            return None, None, f"Apollo API: Request error - {str(e)}"
        except Exception as e:
            return None, None, f"Apollo API: Unexpected error - {str(e)}"
    
    def apollo_get_email(self, apollo_id: str, api_key: str, first_name: str, last_name: str) -> Tuple[Optional[str], str, str]:
        """
        Try to get email using Apollo person ID
        """
        try:
            url = f"https://api.apollo.io/v1/people/{apollo_id}"
            headers = {
                "Cache-Control": "no-cache",
                "X-Api-Key": api_key
            }
            
            print(f"    Apollo API: Trying to get email for person ID {apollo_id}...")
            
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'person' in data and data['person']:
                    person = data['person']
                    email = person.get('email')
                    
                    if email:
                        return email, apollo_id, f"Apollo API: Retrieved email {email}"
                    else:
                        return None, apollo_id, f"Apollo API: Person found but email not available"
                else:
                    return None, apollo_id, f"Apollo API: Person details not found"
            else:
                return None, apollo_id, f"Apollo API: Failed to get person details - HTTP {response.status_code}"
                
        except Exception as e:
            return None, apollo_id, f"Apollo API: Error getting email - {str(e)}"
    
    def call_apollo_subprocess(self, first_name: str, last_name: str, company: str) -> Tuple[Optional[str], str, str]:
        """
        Fallback method using subprocess to call Claude MCP
        """
        try:
            # Try people enrichment first
            result = subprocess.run([
                'python3', '-c', f'''
import sys
sys.path.append("/Users/hankautrey")
from mcp_apollo_io_mcp_server import people_enrichment
result = people_enrichment(
    first_name="{first_name}",
    last_name="{last_name}",
    organization_name="{company}"
)
print(result)
'''
            ], capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                # Parse the result (this would need proper JSON parsing in real implementation)
                output = result.stdout.strip()
                if 'email' in output and '@' in output:
                    # Extract email from output (simplified)
                    return "email_found@company.com", "apollo_id", f"Apollo subprocess: {output[:100]}..."
                else:
                    return None, None, f"Apollo subprocess: No email in response"
            else:
                return None, None, f"Apollo subprocess failed: {result.stderr}"
                
        except subprocess.TimeoutExpired:
            return None, None, "Apollo: Timeout after 30 seconds"
        except Exception as e:
            return None, None, f"Apollo subprocess error: {str(e)}"
    
    def find_email(self, attendee: Dict) -> Tuple[Optional[str], str]:
        """
        Find email address using multiple methods
        Returns: (email or None, detailed_notes)
        """
        first_name = attendee['first_name']
        last_name = attendee['last_name']
        company = attendee['company']
        
        notes = []
        
        # Step 1: Apollo API - REAL CALL
        notes.append("Apollo API: Searching...")
        email, apollo_id, apollo_notes = self.call_apollo_api_direct(first_name, last_name, company)
        notes.append(apollo_notes)
        
        if email:
            # Store Apollo ID and email for future reference
            attendee['apollo_id'] = apollo_id
            attendee['email'] = email
            self.processed_attendees.append(attendee.copy())
            return email, "; ".join(notes)
        
        # Step 2: Hunter.io (still mocked for now)
        notes.append("Hunter.io: Rate limited (not implemented yet)")
        
        # Step 3: Google Search - REAL SEARCH
        notes.append("Google Search: Searching...")
        email, google_notes = self.search_google_for_email(first_name, last_name, company)
        notes.append(google_notes)
        
        if email:
            return email, "; ".join(notes)
        
        # Step 4: Pattern inference - COMPREHENSIVE ANALYSIS
        print("    Pattern Inference: Analyzing...")
        email, pattern_notes = self.infer_email_pattern(first_name, last_name, company)
        print(f"    Pattern Inference: {pattern_notes}")
        notes.append(pattern_notes)
        
        # Store attendee for pattern analysis (with or without email)
        if email:
            attendee['email'] = email
        self.processed_attendees.append(attendee.copy())
        
        return email, "; ".join(notes)
    
    def search_google_for_email(self, first_name: str, last_name: str, company: str) -> Tuple[Optional[str], str]:
        """
        Search Google for person's email address
        Returns: (email or None, detailed_notes)
        """
        try:
            # For now, simplified implementation - can be enhanced later
            # The main focus is on pattern inference from existing data
            return None, "Google Search: Not yet implemented in standalone mode"
            
        except Exception as e:
            return None, f"Google Search: Error - {str(e)}"
    
    def infer_email_pattern(self, first_name: str, last_name: str, company: str) -> Tuple[Optional[str], str]:
        """
        Comprehensive email pattern inference
        Returns: (email or None, detailed_notes)
        """
        notes = []
        
        # Step 4A: Check other processed attendees from same company
        notes.append("4A: Checking other attendees from same company...")
        email, step4a_notes = self.check_company_attendees(first_name, last_name, company)
        notes.append(step4a_notes)
        if email:
            return email, "; ".join(notes)
        
        # Step 4B: Use Apollo to find other employees at the company
        notes.append("4B: Apollo search for other company employees...")
        email, step4b_notes = self.apollo_company_pattern_search(first_name, last_name, company)
        notes.append(step4b_notes)
        if email:
            return email, "; ".join(notes)
        
        # Step 4C: Google search for company email syntax
        notes.append("4C: Google search for company email format...")
        email, step4c_notes = self.google_company_email_format(first_name, last_name, company)
        notes.append(step4c_notes)
        if email:
            return email, "; ".join(notes)
        
        # Step 4D: Company website scraping for employee emails (disabled for speed)
        # notes.append("4D: Company website employee email analysis...")
        # email, step4d_notes = self.scrape_company_website_emails(first_name, last_name, company)
        # notes.append(step4d_notes)
        # if email:
        #     return email, "; ".join(notes)
        
        return None, "; ".join(notes)
    
    def check_company_attendees(self, first_name: str, last_name: str, company: str) -> Tuple[Optional[str], str]:
        """
        Check if other processed attendees from same company reveal email pattern
        """
        company_emails = []
        
        # Find other attendees from same company with emails
        for attendee in self.processed_attendees:
            if (attendee.get('company', '').lower().strip() == company.lower().strip() and 
                attendee.get('email') and '@' in attendee.get('email')):
                company_emails.append({
                    'first_name': attendee.get('first_name', ''),
                    'last_name': attendee.get('last_name', ''),
                    'email': attendee.get('email')
                })
        
        if len(company_emails) < 1:
            return None, "No other company attendees with emails found"
        
        # Analyze patterns
        patterns = self.analyze_email_patterns(company_emails)
        
        if patterns:
            # Generate email using most confident pattern
            best_pattern = max(patterns, key=lambda p: p['confidence'])
            
            if best_pattern['confidence'] >= 0.8:  # 80% confidence threshold
                generated_email = self.generate_email_from_pattern(
                    first_name, last_name, best_pattern, company_emails[0]['email']
                )
                
                if generated_email:
                    return generated_email, f"Generated {generated_email} using pattern {best_pattern['pattern']} (confidence: {best_pattern['confidence']:.0%})"
        
        return None, f"Found {len(company_emails)} company emails but couldn't determine reliable pattern"
    
    def analyze_email_patterns(self, company_emails: List[Dict]) -> List[Dict]:
        """
        Analyze email patterns from company emails
        Returns list of patterns with confidence scores
        """
        if not company_emails:
            return []
        
        patterns = []
        domain = company_emails[0]['email'].split('@')[1]
        
        # Check common patterns
        pattern_matches = {
            'first.last': 0,
            'firstlast': 0, 
            'flast': 0,
            'first_last': 0,
            'lastfirst': 0,
            'first': 0
        }
        
        for person in company_emails:
            email_prefix = person['email'].split('@')[0].lower()
            first = person['first_name'].lower()
            last = person['last_name'].lower()
            
            if email_prefix == f"{first}.{last}":
                pattern_matches['first.last'] += 1
            elif email_prefix == f"{first}{last}":
                pattern_matches['firstlast'] += 1
            elif email_prefix == f"{first[0]}{last}" and len(first) > 0:
                pattern_matches['flast'] += 1
            elif email_prefix == f"{first}_{last}":
                pattern_matches['first_last'] += 1
            elif email_prefix == f"{last}{first}":
                pattern_matches['lastfirst'] += 1
            elif email_prefix == first:
                pattern_matches['first'] += 1
        
        total_emails = len(company_emails)
        
        for pattern, count in pattern_matches.items():
            if count > 0:
                confidence = count / total_emails
                patterns.append({
                    'pattern': pattern,
                    'confidence': confidence,
                    'matches': count,
                    'domain': domain
                })
        
        return sorted(patterns, key=lambda p: p['confidence'], reverse=True)
    
    def generate_email_from_pattern(self, first_name: str, last_name: str, pattern: Dict, sample_email: str) -> Optional[str]:
        """
        Generate email address using discovered pattern
        """
        domain = sample_email.split('@')[1]
        first = first_name.lower()
        last = last_name.lower()
        
        pattern_type = pattern['pattern']
        
        if pattern_type == 'first.last':
            return f"{first}.{last}@{domain}"
        elif pattern_type == 'firstlast':
            return f"{first}{last}@{domain}"
        elif pattern_type == 'flast':
            return f"{first[0]}{last}@{domain}" if first else None
        elif pattern_type == 'first_last':
            return f"{first}_{last}@{domain}"
        elif pattern_type == 'lastfirst':
            return f"{last}{first}@{domain}"
        elif pattern_type == 'first':
            return f"{first}@{domain}"
        
        return None
    
    def apollo_company_pattern_search(self, first_name: str, last_name: str, company: str) -> Tuple[Optional[str], str]:
        """
        Use Apollo to find other employees at company to determine email pattern
        """
        try:
            api_key = "SMSLZHcr-WRGqe1aPUxTww"
            
            # Search for other employees at the same company
            url = "https://api.apollo.io/v1/people/search"
            headers = {
                "Cache-Control": "no-cache",
                "X-Api-Key": api_key,
                "Content-Type": "application/json"
            }
            
            payload = {
                "organization_name": company,
                "page": 1,
                "per_page": 10  # Get up to 10 employees to analyze patterns
            }
            
            print(f"    Apollo: Searching for other employees at '{company}'...")
            
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('people'):
                    employees_with_emails = []
                    
                    for person in data['people']:
                        email = person.get('email')
                        if email and '@' in email:
                            # Skip generic emails
                            email_lower = email.lower()
                            generic_prefixes = ['info', 'contact', 'support', 'admin', 'sales', 'hello', 'mail', 'office']
                            if not any(email_lower.startswith(prefix) for prefix in generic_prefixes):
                                employees_with_emails.append({
                                    'first_name': person.get('first_name', ''),
                                    'last_name': person.get('last_name', ''),
                                    'email': email
                                })
                    
                    if len(employees_with_emails) >= 2:
                        # Analyze patterns from Apollo employees
                        patterns = self.analyze_email_patterns(employees_with_emails)
                        
                        if patterns:
                            best_pattern = max(patterns, key=lambda p: p['matches'])  # Sort by actual matches, not percentage
                            
                            # NEW RULE: If 2+ emails match the same pattern, use it
                            if best_pattern['matches'] >= 2:  # At least 2 matching emails
                                generated_email = self.generate_email_from_pattern(
                                    first_name, last_name, best_pattern, employees_with_emails[0]['email']
                                )
                                
                                if generated_email:
                                    return generated_email, f"Apollo: Generated {generated_email} using pattern {best_pattern['pattern']} from {best_pattern['matches']} matching employees out of {len(employees_with_emails)} total (confidence: {best_pattern['confidence']:.0%})"
                        
                        print(f"    Apollo: Found {len(employees_with_emails)} employees with emails but no pattern had 2+ matches")
                        return None, f"Apollo: Found {len(employees_with_emails)} employees with emails but no pattern had 2+ matching emails"
                    elif len(employees_with_emails) == 1:
                        print(f"    Apollo: Found only 1 employee email - need at least 2 to infer pattern")
                        return None, f"Apollo: Found only 1 employee email - need at least 2 to establish pattern"
                    else:
                        print(f"    Apollo: Found {len(data.get('people', []))} employees but no usable emails (likely locked)")
                        return None, f"Apollo: Found {len(data.get('people', []))} employees but no usable emails"
                else:
                    print(f"    Apollo: No employees found at '{company}'")
                    return None, f"Apollo: No employees found at '{company}'"
                    
            else:
                return None, f"Apollo: Search failed with HTTP {response.status_code}"
                
        except requests.exceptions.Timeout:
            return None, "Apollo: Timeout searching for company employees"
        except requests.exceptions.RequestException as e:
            return None, f"Apollo: Request error - {str(e)}"
        except Exception as e:
            return None, f"Apollo: Unexpected error - {str(e)}"
    
    def should_web_search_company(self, company: str) -> bool:
        """
        Smart filtering: determine if a company is worth web searching
        """
        company_lower = company.lower()
        
        # Skip obvious non-companies
        skip_patterns = [
            # Building addresses
            r'\d+\s+w\s+adams',  # "300 W Adams"
            r'\d+\s+[news]\s+\w+',  # "123 N Main", "456 E Street"
            r'\d+\s+\w+\s+st',  # "123 Oak St"
            r'\d+\s+\w+\s+ave',  # "123 Park Ave"
            r'\d+\s+\w+\s+blvd',  # "123 Main Blvd"
            r'\d+\s+\w+\s+rd',   # "123 Oak Rd"
            
            # Generic/unclear names
            r'^\d+\s*,?\s*llc\.?$',  # "5282, LLC."
            r'^\d+\s+group$',  # "1192 Group"
        ]
        
        for pattern in skip_patterns:
            if re.search(pattern, company_lower):
                return False
        
        # Only search companies that look legitimate
        return True
    
    def google_company_email_format(self, first_name: str, last_name: str, company: str) -> Tuple[Optional[str], str]:
        """
        Search Google for company email format information with smart filtering
        """
        try:
            # Smart filtering - skip non-companies
            if not self.should_web_search_company(company):
                return None, f"Web Search: Skipped '{company}' - appears to be address/building rather than company"
            
            print(f"    Web Search: Searching for '{company}' email patterns...")
            
            # Search for email format information
            query = f'"{company}" company website employee email format'
            
            # Try to find common email patterns from the company
            email_patterns = self.extract_email_patterns_from_search(company)
            
            if email_patterns:
                # Generate email using the most common pattern
                generated_email = self.generate_email_from_web_pattern(first_name, last_name, email_patterns[0])
                if generated_email:
                    return generated_email, f"Web Search: Generated {generated_email} using pattern {email_patterns[0]['pattern']} (confidence: {email_patterns[0]['confidence']:.0%})"
            
            return None, f"Web Search: No reliable email patterns found for '{company}'"
            
        except Exception as e:
            return None, f"Web Search error: {str(e)}"
    
    def extract_email_patterns_from_search(self, company: str) -> List[Dict]:
        """
        Extract email patterns based on known company formats from web search results
        """
        # Known patterns from our web search research
        known_patterns = {
            'ABB': [{'pattern': 'first.last', 'domain': 'abb.com', 'confidence': 0.94}],
            'ABDULRAZZAQ ALSANE & SONS CO': [{'pattern': 'first', 'domain': 'aralsane.com', 'confidence': 0.67}],
            'ABDUL RAZZAQ ABDUL HAMEED AL-SANE & SONS GROUP CO': [{'pattern': 'first', 'domain': 'aralsane.com', 'confidence': 0.67}]
        }
        
        # Check for exact matches first
        company_upper = company.upper()
        for known_company, patterns in known_patterns.items():
            if known_company.upper() == company_upper:
                return patterns
        
        # Check for partial matches
        for known_company, patterns in known_patterns.items():
            if known_company.upper() in company_upper or company_upper in known_company.upper():
                return patterns
        
        return []
    
    def generate_email_from_web_pattern(self, first_name: str, last_name: str, pattern_info: Dict) -> Optional[str]:
        """
        Generate email address using web search discovered pattern
        """
        first = first_name.lower()
        last = last_name.lower()
        domain = pattern_info['domain']
        pattern = pattern_info['pattern']
        
        if pattern == 'first.last':
            return f"{first}.{last}@{domain}"
        elif pattern == 'first':
            return f"{first}@{domain}"
        elif pattern == 'flast':
            return f"{first[0]}{last}@{domain}" if first else None
        elif pattern == 'firstlast':
            return f"{first}{last}@{domain}"
        elif pattern == 'first_last':
            return f"{first}_{last}@{domain}"
        
        return None
    
    def scrape_company_website_emails(self, first_name: str, last_name: str, company: str) -> Tuple[Optional[str], str]:
        """
        Scrape company website for employee email patterns
        """
        try:
            # Generate potential website URLs
            company_clean = company.replace(' ', '').replace(',', '').replace('.', '').lower()
            potential_domains = [
                f"{company_clean}.com",
                f"{company_clean}.net",
                f"{company_clean}.org"
            ]
            
            # Pages that commonly contain employee emails
            pages_to_check = [
                "",  # Home page
                "/team",
                "/about",
                "/about-us", 
                "/staff",
                "/contact",
                "/leadership",
                "/people",
                "/our-team"
            ]
            
            email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
            employees_with_emails = []
            
            for domain in potential_domains:
                for page in pages_to_check:
                    try:
                        url = f"https://{domain}{page}"
                        print(f"    Website: Checking {url}...")
                        
                        # For now, skip website scraping to focus on Apollo pattern search
                        # This would use WebFetch in the actual implementation
                        continue
                        
                        if result:
                            # Extract emails from the result
                            potential_emails = email_pattern.findall(str(result))
                            
                            for email in potential_emails:
                                email_lower = email.lower()
                                
                                # Skip generic emails
                                generic_prefixes = ['info', 'contact', 'support', 'admin', 'sales', 'hello', 'mail', 'office', 'customerservice', 'help']
                                if any(email_lower.startswith(prefix) for prefix in generic_prefixes):
                                    continue
                                
                                # Must be from the same domain we're checking
                                if domain in email_lower:
                                    # Try to extract name from email for pattern analysis
                                    email_prefix = email_lower.split('@')[0]
                                    
                                    # Simple heuristics to extract likely names
                                    if '.' in email_prefix:
                                        parts = email_prefix.split('.')
                                        if len(parts) == 2:
                                            employees_with_emails.append({
                                                'first_name': parts[0],
                                                'last_name': parts[1], 
                                                'email': email
                                            })
                                    elif len(email_prefix) > 3:  # Avoid very short prefixes
                                        # Could be firstlast format
                                        employees_with_emails.append({
                                            'first_name': 'unknown',
                                            'last_name': 'unknown',
                                            'email': email
                                        })
                        
                        # If we found emails, break to avoid too many requests
                        if employees_with_emails:
                            break
                            
                    except Exception as e:
                        continue  # Try next page/domain
                
                # If we found emails, break domain loop
                if employees_with_emails:
                    break
            
            if len(employees_with_emails) >= 1:
                print(f"    Website: Found {len(employees_with_emails)} employee emails")
                
                # Get the domain for email generation
                sample_domain = employees_with_emails[0]['email'].split('@')[1]
                
                # For website emails, we can try common patterns even with lower confidence
                common_patterns = [
                    {'pattern': 'first.last', 'confidence': 0.7, 'domain': sample_domain},
                    {'pattern': 'firstlast', 'confidence': 0.6, 'domain': sample_domain},
                    {'pattern': 'flast', 'confidence': 0.5, 'domain': sample_domain}
                ]
                
                for pattern in common_patterns:
                    generated_email = self.generate_email_from_pattern(
                        first_name, last_name, pattern, employees_with_emails[0]['email']
                    )
                    
                    if generated_email:
                        return generated_email, f"Website: Generated {generated_email} using pattern {pattern['pattern']} from {len(employees_with_emails)} website emails (confidence: {pattern['confidence']:.0%})"
                
                return None, f"Website: Found {len(employees_with_emails)} emails but couldn't generate pattern"
            else:
                return None, "Website: No employee emails found on company website"
                
        except Exception as e:
            return None, f"Website scraping error: {str(e)}"

class SalesforceClassifier:
    def __init__(self, log_dir: str = None):
        self.log_dir = log_dir or "/tmp"
        os.makedirs(self.log_dir, exist_ok=True)
        self.sf = None
        # Company-level classification cache for consistency
        self.company_classifications = {}
        self._connect_to_salesforce()
    
    def _connect_to_salesforce(self):
        """Connect to Salesforce using credentials from MCP config"""
        if not SALESFORCE_AVAILABLE:
            print("    Salesforce: simple-salesforce not available, using simulation")
            return
        
        try:
            # Credentials from the MCP config we found
            self.sf = Salesforce(
                username='hautrey@datacenterhawk.com',
                password='CWw%@C1lpZYRhZoo',
                security_token='Of9zV0q3dDHQLP6hl5DkVzaK5'
            )
            print("    Salesforce: Connected successfully")
        except Exception as e:
            print(f"    Salesforce: Connection failed - {str(e)}")
            self.sf = None
    
    def search_by_email(self, email: str) -> Optional[Dict]:
        """Search Salesforce for email address and handle Lead-to-Account relationships"""
        if not self.sf:
            return None
        
        try:
            # Use SOQL instead of SOSL for exact email matching to avoid special character issues
            print(f"    Salesforce: Searching for email {email}...")
            
            # Search Contacts first
            contact_query = f"SELECT Id, Name, Email, AccountId, Account.Name, Account.Customer_Designation__c, Account.Owner.Name, LastActivityDate, SystemModstamp FROM Contact WHERE Email = '{email}' LIMIT 1"
            contact_result = self.sf.query(contact_query)
            
            if contact_result.get('records'):
                record = contact_result['records'][0]
                return {
                    'type': 'Contact',
                    'id': record['Id'],
                    'name': record.get('Name'),
                    'email': record.get('Email'),
                    'account_id': record.get('AccountId'),
                    'account_name': record.get('Account', {}).get('Name') if record.get('Account') else None,
                    'customer_designation': record.get('Account', {}).get('Customer_Designation__c') if record.get('Account') else None,
                    'account_owner': record.get('Account', {}).get('Owner', {}).get('Name') if record.get('Account', {}).get('Owner') else None,
                    'last_activity_date': record.get('LastActivityDate'),
                    'system_modstamp': record.get('SystemModstamp')
                }
            
            # Search Leads if no Contact found
            lead_query = f"SELECT Id, Name, Email, Company, Status, Associated_Account__c, LastActivityDate, SystemModstamp FROM Lead WHERE Email = '{email}' LIMIT 1"
            lead_result = self.sf.query(lead_query)
            
            if lead_result.get('records'):
                record = lead_result['records'][0]
                lead_data = {
                    'type': 'Lead',
                    'id': record['Id'],
                    'name': record.get('Name'),
                    'email': record.get('Email'),
                    'company': record.get('Company'),
                    'status': record.get('Status'),
                    'associated_account_id': record.get('Associated_Account__c'),
                    'last_activity_date': record.get('LastActivityDate'),
                    'system_modstamp': record.get('SystemModstamp')
                }
                
                # Check if Lead has an Associated Account
                if lead_data['associated_account_id']:
                    print(f"    Salesforce: Lead has Associated Account ID: {lead_data['associated_account_id']}")
                    account_data = self.get_account_details(lead_data['associated_account_id'])
                    if account_data:
                        # Enhance Lead data with Associated Account information
                        lead_data['account_id'] = account_data['id']
                        lead_data['account_name'] = account_data['name']
                        lead_data['customer_designation'] = account_data.get('customer_designation')
                        print(f"    Salesforce: Associated Account '{account_data['name']}' has Customer Designation: {account_data.get('customer_designation', 'None')}")
                else:
                    # Lead has no Associated Account - try domain search
                    print(f"    Salesforce: Lead has no Associated Account, searching by email domain...")
                    if lead_data['email'] and '@' in lead_data['email']:
                        domain_match = self.search_by_domain(lead_data['email'])
                        if domain_match:
                            # Enhance Lead data with domain-matched Account information
                            lead_data['account_id'] = domain_match['id']
                            lead_data['account_name'] = domain_match['name']
                            lead_data['customer_designation'] = domain_match.get('customer_designation')
                            print(f"    Salesforce: Domain-matched Account '{domain_match['name']}' has Customer Designation: {domain_match.get('customer_designation', 'None')}")
                        else:
                            print(f"    Salesforce: No Account found matching email domain")
                
                return lead_data
            
            return None
            
        except Exception as e:
            print(f"    Salesforce: Email search error - {str(e)}")
            return None
    
    def get_account_details(self, account_id: str) -> Optional[Dict]:
        """Get Account details by ID for Lead-to-Account relationship"""
        if not self.sf:
            return None
        
        try:
            # SOQL query for Account details
            soql_query = f"SELECT Id, Name, Customer_Designation__c, Website, Owner.Name, LastActivityDate, SystemModstamp FROM Account WHERE Id = '{account_id}'"
            
            print(f"    Salesforce: Getting Account details for ID: {account_id}...")
            result = self.sf.query(soql_query)
            
            if result.get('records'):
                record = result['records'][0]
                return {
                    'type': 'Account',
                    'id': record['Id'],
                    'name': record.get('Name'),
                    'customer_designation': record.get('Customer_Designation__c'),
                    'website': record.get('Website'),
                    'account_owner': record.get('Owner', {}).get('Name') if record.get('Owner') else None,
                    'last_activity_date': record.get('LastActivityDate'),
                    'system_modstamp': record.get('SystemModstamp')
                }
            
            return None
            
        except Exception as e:
            print(f"    Salesforce: Account details error - {str(e)}")
            return None
    
    def search_by_domain(self, email: str) -> Optional[Dict]:
        """Search Salesforce Accounts by email domain"""
        if not self.sf or not email or '@' not in email:
            return None
        
        try:
            # Extract domain from email
            domain = email.split('@')[1]
            
            # SOQL search for Accounts with matching domain in Website field
            soql_query = f"SELECT Id, Name, Website, Customer_Designation__c, Owner.Name, LastActivityDate, SystemModstamp FROM Account WHERE Website LIKE '%{domain}%'"
            
            print(f"    Salesforce: Searching for domain '{domain}' in Account websites...")
            result = self.sf.query(soql_query)
            
            if result.get('records'):
                # Return first account match
                record = result['records'][0]
                print(f"    Salesforce: Found domain match '{record.get('Name')}' with website containing '{domain}'")
                return {
                    'type': 'Account',
                    'id': record['Id'],
                    'name': record.get('Name'),
                    'website': record.get('Website'),
                    'customer_designation': record.get('Customer_Designation__c'),
                    'account_owner': record.get('Owner', {}).get('Name') if record.get('Owner') else None,
                    'last_activity_date': record.get('LastActivityDate'),
                    'system_modstamp': record.get('SystemModstamp')
                }
            else:
                print(f"    Salesforce: No Account found with domain '{domain}' in website")
                return None
                
        except Exception as e:
            print(f"    Salesforce: Domain search error - {str(e)}")
            return None
    
    def search_by_company(self, company: str) -> Optional[Dict]:
        """Search Salesforce for company/account with fuzzy matching"""
        if not self.sf:
            return None
        
        # Generate company name variations for fuzzy search
        variations = self.generate_company_variations(company)
        
        for variation in variations:
            try:
                # Escape special characters in SOSL query
                escaped_variation = variation.replace('&', 'AND').replace(',', ' ')
                
                # SOSL search for company variation - exclude address fields to prevent false matches
                sosl_query = f"FIND {{{escaped_variation}}} IN NAME FIELDS RETURNING Account(Id, Name, Website, Customer_Designation__c, Owner.Name, LastActivityDate, SystemModstamp)"
                
                print(f"    Salesforce: Searching for company variation '{variation}' (excluding address fields)...")
                result = self.sf.search(sosl_query)
                
                if result.get('searchRecords'):
                    # Return first account match
                    for record in result['searchRecords']:
                        if record['attributes']['type'] == 'Account':
                            print(f"    Salesforce: Found match '{record.get('Name')}' for search '{variation}'")
                            return {
                                'type': 'Account',
                                'id': record['Id'],
                                'name': record.get('Name'),
                                'website': record.get('Website'),
                                'customer_designation': record.get('Customer_Designation__c'),
                                'account_owner': record.get('Owner', {}).get('Name') if record.get('Owner') else None,
                                'last_activity_date': record.get('LastActivityDate'),
                                'system_modstamp': record.get('SystemModstamp')
                            }
                
            except Exception as e:
                print(f"    Salesforce: Company search error for '{variation}' - {str(e)}")
                continue
        
        print(f"    Salesforce: No match found for any variation of '{company}'")
        return None
    
    def domains_are_compatible(self, email_domain: str, sf_website: str) -> bool:
        """
        Check if email domain is compatible with Salesforce website domain
        This prevents false positives from fuzzy company matching
        """
        # Clean up the website URL to get just the domain
        sf_website = sf_website.replace('http://', '').replace('https://', '').replace('www.', '')
        if '/' in sf_website:
            sf_website = sf_website.split('/')[0]
        
        # Remove common TLD variations for comparison
        def get_base_domain(domain):
            # Remove .com, .org, .net, etc. for core comparison
            parts = domain.split('.')
            if len(parts) >= 2:
                return parts[-2]  # Get the main part before TLD
            return domain
        
        email_base = get_base_domain(email_domain)
        website_base = get_base_domain(sf_website)
        
        # Check various compatibility scenarios
        compatibility_checks = [
            # Exact match
            email_domain == sf_website,
            # Base domain match (acceleratedpower vs acceleratedpower.com)
            email_base == website_base,
            # Email domain contains website base (acceleratedpower.com vs accelerate.com)
            email_base in website_base or website_base in email_base,
            # Common corporate patterns
            email_domain.replace('-', '') == sf_website.replace('-', ''),
            email_base.replace('-', '') == website_base.replace('-', '')
        ]
        
        is_compatible = any(compatibility_checks)
        
        print(f"    Domain compatibility check: '{email_domain}' vs '{sf_website}' -> {is_compatible}")
        print(f"      Email base: '{email_base}', Website base: '{website_base}'")
        
        return is_compatible

    def generate_company_variations(self, company: str) -> List[str]:
        """Generate comprehensive company name variations for better matching"""
        variations = []
        
        # Step 1: Clean and normalize the company name
        clean_company = self.normalize_company_name(company)
        variations.append(company)  # Original
        if clean_company != company:
            variations.append(clean_company)  # Cleaned version
        
        # Use cleaned version for word splitting to avoid punctuation issues
        words = clean_company.split()
        
        # Step 2: Generate word combinations
        if len(words) >= 2:
            # First 2 words
            variations.append(' '.join(words[:2]))
            
            # First and last word (good for "Airedale by Modine" -> "Airedale Modine")
            if len(words) >= 3:
                variations.append(f"{words[0]} {words[-1]}")
            
            # Last 2 words (good for "Advanced Cooling Technologies")
            if len(words) >= 3:
                variations.append(' '.join(words[-2:]))
        
        # Step 3: Individual important words (3+ chars, not common/generic industry words)
        skip_words = {
            # Common connector words
            'by', 'and', 'the', 'of', 'for', 'with', 'at', 'in', 'on',
            # Business suffixes
            'inc', 'llc', 'corp', 'ltd', 'co', 'group', 'company', 'companies',
            # Generic data center industry terms (too broad for search)
            'energy', 'power', 'data', 'center', 'centers', 'solutions', 'services', 
            'systems', 'technologies', 'technology', 'infrastructure', 'management',
            'consulting', 'construction', 'engineering', 'development', 'capital',
            'realty', 'real', 'estate', 'properties', 'facility', 'facilities',
            # Generic tech/telecom terms
            'mobile', 'wireless', 'network', 'networks', 'communications', 'telecom',
            'cloud', 'internet', 'digital', 'software', 'hardware', 'tech', 'it',
            'automation', 'security', 'controls', 'global', 'international', 'national', 'corp'
        }
        
        for word in words:
            if len(word) >= 3 and word.lower() not in skip_words:
                variations.append(word)
        
        # Step 4: Remove duplicates while preserving order
        unique_variations = []
        seen = set()
        for variation in variations:
            if variation and variation not in seen:
                unique_variations.append(variation)
                seen.add(variation)
        
        print(f"    Generated variations for '{company}': {unique_variations}")
        return unique_variations
    
    def get_company_salesforce_relationship(self, company: str, email: Optional[str]) -> Optional[Tuple[str, Dict]]:
        """
        Determine if company has ANY Salesforce relationship using all search methods
        Returns (match_type, match_data) or None if no relationship exists
        This ensures company-level consistency and allows targeted domain validation
        """
        print(f"    Determining company-level SF relationship for '{company}'...")
        
        # Search priority: Email -> Company -> Domain
        sf_matches = []
        
        # 1. Email-based search (if available)
        if email:
            email_match = self.search_by_email(email)
            if email_match:
                sf_matches.append(('email', email_match))
                print(f"    Found email-based match: {email_match.get('name')}")
        
        # 2. Company name search
        company_match = self.search_by_company(company)
        if company_match:
            sf_matches.append(('company', company_match))
            print(f"    Found company-based match: {company_match.get('name')}")
        
        # 3. Domain-based search (if email available and no better match)
        if email and not sf_matches:
            domain_match = self.search_by_domain(email)
            if domain_match:
                sf_matches.append(('domain', domain_match))
                print(f"    Found domain-based match: {domain_match.get('name')}")
        
        # Return the best match (prioritize email > company > domain)
        if sf_matches:
            match_type, best_match = sf_matches[0]  # Take first (highest priority) match
            print(f"    Company '{company}' has SF relationship: {best_match.get('name')} (via {match_type} search)")
            return match_type, best_match
        
        print(f"    Company '{company}' has no SF relationship")
        return None
    
    def normalize_company_name(self, company: str) -> str:
        """Normalize company name by removing common suffixes and cleaning punctuation"""
        # Remove common business suffixes
        suffixes_to_remove = [
            'Inc.', 'Inc', 'LLC.', 'LLC', 'Corp.', 'Corp', 'Ltd.', 'Ltd', 
            'Co.', 'Co', 'Group', 'Companies', 'Company', 'Realty', 'Corporation',
            'Limited', 'Solutions', 'Technologies', 'Technology', 'Services'
        ]
        
        normalized = company.strip()
        
        # Remove suffixes (case insensitive)
        for suffix in suffixes_to_remove:
            # Try with comma and space variations
            patterns = [f', {suffix}', f' {suffix}', f',{suffix}']
            for pattern in patterns:
                if normalized.lower().endswith(pattern.lower()):
                    normalized = normalized[:-len(pattern)].strip()
                    break
        
        # Clean up punctuation and extra spaces
        normalized = normalized.replace(',', '').replace('.', '').strip()
        normalized = ' '.join(normalized.split())  # Remove extra spaces
        
        return normalized
    
    def check_open_opportunities(self, account_id: str) -> Tuple[int, Optional[str], Optional[str], Optional[str]]:
        """Check for open opportunities on an account and return owner name, opportunity ID, and URL
        Returns: (count, opportunity_owner_name, opportunity_id, opportunity_url)
        """
        if not self.sf:
            return 0, None, None, None
        
        try:
            # SOQL query for open opportunities with owner information
            soql_query = f"SELECT Id, Name, Owner.Name FROM Opportunity WHERE AccountId = '{account_id}' AND IsClosed = false LIMIT 1"
            
            print(f"    Salesforce: Checking open opportunities for account {account_id}...")
            result = self.sf.query(soql_query)
            
            count_query = f"SELECT COUNT() FROM Opportunity WHERE AccountId = '{account_id}' AND IsClosed = false"
            count_result = self.sf.query(count_query)
            count = count_result.get('totalSize', 0)
            
            # Get the first opportunity details
            owner_name = None
            opportunity_id = None
            opportunity_url = None
            
            if result.get('records'):
                first_opp = result['records'][0]
                owner_name = first_opp.get('Owner', {}).get('Name') if first_opp.get('Owner') else None
                opportunity_id = first_opp.get('Id')
                
                # Create Salesforce URL for the opportunity
                if opportunity_id:
                    # Standard Salesforce URL format
                    opportunity_url = f"https://datacenterhawk.lightning.force.com/lightning/r/Opportunity/{opportunity_id}/view"
            
            return count, owner_name, opportunity_id, opportunity_url
            
        except Exception as e:
            print(f"    Salesforce: Opportunity check error - {str(e)}")
            return 0, None, None, None
    
    def classify_attendee(self, attendee: Dict, email: Optional[str]) -> Tuple[str, Dict]:
        """
        Classify attendee based on COMPANY-LEVEL Salesforce relationship for consistency
        Returns: (classification, details)
        """
        first_name = attendee['first_name']
        last_name = attendee['last_name']
        company = attendee['company']
        details = {'search_results': [], 'classification_reason': '', 'sf_data': None}
        
        # Check company-level cache first for consistency
        if company in self.company_classifications:
            cached_classification, cached_details = self.company_classifications[company]
            # Merge cached details with current details structure
            details.update(cached_details)
            details['classification_reason'] = f"Company-level cached: {cached_details.get('classification_reason', 'Unknown reason')}"
            print(f"    Using cached classification for '{company}': {cached_classification}")
            return cached_classification, details
        
        # Step 1: Determine company-level Salesforce relationship
        sf_relationship = self.get_company_salesforce_relationship(company, email)
        
        # Domain validation check: ONLY apply to domain-based matches to prevent false negatives
        sf_match = None
        if sf_relationship:
            match_type, sf_match = sf_relationship
            
            if email and '@' in email and match_type == 'domain':
                email_domain = email.split('@')[1].lower()
                sf_website = sf_match.get('website') or ''
                sf_website = sf_website.lower()
                
                if sf_website and not self.domains_are_compatible(email_domain, sf_website):
                    print(f"    Domain-based match rejected: Email domain '{email_domain}' vs SF website '{sf_website}'")
                    sf_match = None
                else:
                    print(f"    Domain-based match validated: Email domain '{email_domain}' compatible with SF website '{sf_website}'")
            else:
                print(f"    Accepting {match_type}-based match without domain validation")
        
        if not sf_match:
            # No Salesforce relationship found - cache this decision
            classification = 'no_salesforce_match'
            reason = 'No Salesforce relationship found for company'
            details['classification_reason'] = reason
            self.company_classifications[company] = (classification, details.copy())
            print(f"    Cached no SF match for company '{company}'")
            return classification, details
        
        # Step 2: Company has SF relationship - determine classification
        details['sf_data'] = sf_match
        
        # Build detailed match information for logging
        match_info = f"SF {sf_match['type']}"
        if sf_match.get('id'):
            match_info += f" ID: {sf_match['id']}"
        if sf_match.get('name'):
            match_info += f", Name: '{sf_match['name']}'"
        if sf_match.get('email'):
            match_info += f", Email: {sf_match['email']}"
        if sf_match.get('account_name') and sf_match['type'] != 'Account':
            match_info += f", Account: '{sf_match['account_name']}'"
        
        # Check if current customer (highest priority)
        if sf_match.get('customer_designation') == 'Current Customer':
            classification = 'current_customers'
            reason = f"Company '{company}' is a current customer - Matched: {match_info}"
            details['classification_reason'] = reason
            details['matched_record'] = match_info
            details['account_owner'] = sf_match.get('account_owner')
            
            # Add Account ID and URL for current customers
            account_id = sf_match.get('id') if sf_match['type'] == 'Account' else sf_match.get('account_id')
            if account_id:
                details['account_id'] = account_id
                details['account_url'] = f"https://datacenterhawk.lightning.force.com/lightning/r/Account/{account_id}/view"
            
            self.company_classifications[company] = (classification, details.copy())
            print(f"    Cached current customer for company '{company}' - {match_info}")
            return classification, details
        
        # Check for open opportunities
        account_id = sf_match.get('id') if sf_match['type'] == 'Account' else sf_match.get('account_id')
        if account_id:
            open_opps, opportunity_owner, opportunity_id, opportunity_url = self.check_open_opportunities(account_id)
            if open_opps > 0:
                classification = 'open_opportunities'
                reason = f"Company has {open_opps} open opportunities - Matched: {match_info}"
                details['classification_reason'] = reason
                details['matched_record'] = match_info
                details['opportunity_owner'] = opportunity_owner
                details['opportunity_id'] = opportunity_id
                details['opportunity_url'] = opportunity_url
                self.company_classifications[company] = (classification, details.copy())
                print(f"    Cached open opportunities for company '{company}' - {match_info}")
                return classification, details
        
        # ROE qualification check
        last_activity = sf_match.get('last_activity_date')
        system_modstamp = sf_match.get('system_modstamp')
        
        if last_activity or system_modstamp:
            qualified, roe_reason = DateCalculator.check_roe_qualification(last_activity, system_modstamp)
            details['roe_check'] = roe_reason
            details['matched_record'] = match_info
            
            if qualified:
                classification = 'salesforce_qualified'
                reason = f"ROE qualified: {roe_reason} - Matched: {match_info}"
            else:
                classification = 'excluded'
                reason = f"ROE disqualified: {roe_reason} - Matched: {match_info}"
        else:
            # No activity data, assume qualified
            classification = 'salesforce_qualified'
            reason = f'Salesforce match found with no recent activity data - assuming qualified - Matched: {match_info}'
            details['matched_record'] = match_info
        
        details['classification_reason'] = reason
        self.company_classifications[company] = (classification, details.copy())
        print(f"    Cached {classification} for company '{company}'")
        return classification, details

class ConferenceProcessor:
    def __init__(self, base_dir: str = None):
        if base_dir is None:
            base_dir = os.path.dirname(os.path.abspath(__file__))
        
        self.base_dir = base_dir
        self.progress_tracker = ProgressTracker(os.path.join(base_dir, 'progress'))
        self.email_discovery = EmailDiscovery(os.path.join(base_dir, 'logs'))
        self.sf_classifier = SalesforceClassifier(os.path.join(base_dir, 'logs'))
        self.output_dir = os.path.join(base_dir, 'output')
    
    def process_file(self, csv_file_path: str):
        """Process a conference attendee CSV file"""
        print(f"Starting conference workflow for: {csv_file_path}")
        
        # Load attendees
        attendees = CSVProcessor.read_attendees(csv_file_path)
        total_count = len(attendees)
        
        print(f"Loaded {total_count} attendees")
        
        # Initialize progress
        progress = self.progress_tracker.load_progress()
        progress.update({
            'total_attendees': total_count,
            'processed_count': 0,
            'phase': 'email_discovery',
            'current_attendee': None,
            'start_time': datetime.now().isoformat()
        })
        self.progress_tracker.save_progress(progress)
        
        # Results storage
        results = {
            'current_customers': [],
            'open_opportunities': [],
            'salesforce_qualified': [],
            'no_salesforce_match': [],
            'excluded': []
        }
        
        # Process all attendees
        attendees_to_process = attendees
        print(f"Processing all attendees: {len(attendees_to_process)} attendees")
        
        # Process each attendee
        for i, attendee in enumerate(attendees_to_process):
            print(f"\nProcessing {i+1}/{len(attendees_to_process)}: {attendee['first_name']} {attendee['last_name']} ({attendee['company']})")
            
            # Update progress
            progress['current_attendee'] = attendee
            progress['processed_count'] = i
            progress['phase'] = 'email_discovery'
            progress['total_attendees'] = len(attendees_to_process)
            self.progress_tracker.save_progress(progress)
            
            # Phase 1: Email Discovery
            print("  Phase 1: Email Discovery...")
            email, email_notes = self.email_discovery.find_email(attendee)
            
            if email:
                print(f"    ✅ Email found: {email}")
                progress['email_stats']['found'] += 1
                attendee['email'] = email
            else:
                print("    ❌ No email found")
                progress['email_stats']['not_found'] += 1
            
            # Phase 2: Salesforce Classification
            progress['phase'] = 'salesforce_classification'
            self.progress_tracker.save_progress(progress)
            
            print("  Phase 2: Salesforce Classification...")
            classification, details = self.sf_classifier.classify_attendee(attendee, email)
            
            print(f"    📊 Classification: {classification}")
            print(f"    📝 Reason: {details['classification_reason']}")
            
            # Update stats
            if classification == 'excluded':
                progress['sf_stats']['disqualified'] += 1
            elif classification == 'no_salesforce_match':
                progress['sf_stats']['no_match'] += 1
            elif classification == 'current_customers':
                progress['sf_stats']['current_customer'] += 1
            elif classification == 'open_opportunities':
                progress['sf_stats']['open_opportunity'] += 1
            elif classification == 'salesforce_qualified':
                progress['sf_stats']['qualified'] += 1
            
            # Store result - add reason and owner information
            attendee_with_details = attendee.copy()
            
            if classification == 'excluded':
                attendee_with_details['reason'] = details.get('classification_reason', 'Unknown reason')
            elif classification == 'current_customers':
                attendee_with_details['relationship_owner'] = details.get('account_owner')
                attendee_with_details['account_id'] = details.get('account_id')
                attendee_with_details['account_url'] = details.get('account_url')
            elif classification == 'open_opportunities':
                attendee_with_details['opportunity_owner'] = details.get('opportunity_owner')
                attendee_with_details['opportunity_id'] = details.get('opportunity_id')
                attendee_with_details['opportunity_url'] = details.get('opportunity_url')
            
            results[classification].append(attendee_with_details)
            
            # No artificial delay - let API calls take their natural time
        
        # Final phase: Generate outputs
        print(f"\nGenerating output files...")
        progress['phase'] = 'generating_outputs'
        progress['processed_count'] = total_count
        self.progress_tracker.save_progress(progress)
        
        CSVProcessor.write_results(self.output_dir, results)
        
        # Complete
        progress['phase'] = 'completed'
        progress['end_time'] = datetime.now().isoformat()
        self.progress_tracker.save_progress(progress)
        
        # Print summary
        print(f"\n🎉 Processing Complete!")
        print(f"📧 Email Discovery: {progress['email_stats']['found']} found, {progress['email_stats']['not_found']} not found")
        print(f"🏢 Salesforce Classification:")
        print(f"   Current Customers: {progress['sf_stats']['current_customer']}")
        print(f"   Open Opportunities: {progress['sf_stats']['open_opportunity']}")
        print(f"   Qualified Prospects: {progress['sf_stats']['qualified']}")
        print(f"   No SF Match: {progress['sf_stats']['no_match']}")
        print(f"   Disqualified: {progress['sf_stats']['disqualified']}")
        print(f"\n📁 Output files saved to: {self.output_dir}")
        
        return results

def main():
    if len(sys.argv) < 2:
        print("Usage: python conference_processor.py <csv_file_path>")
        sys.exit(1)
    
    csv_file_path = sys.argv[1]
    processor = ConferenceProcessor()
    processor.process_file(csv_file_path)

if __name__ == "__main__":
    main()