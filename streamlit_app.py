#!/usr/bin/env python3
"""
Streamlit Web App for Conference Attendee Processing
Professional interface for team access to the conference workflow
"""

import streamlit as st
import pandas as pd
import os
import tempfile
import io
from datetime import datetime
import sys

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Fix SSL certificate issue for Salesforce
import certifi
os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()

# Import our existing processor
from conference_processor import ConferenceProcessor, CSVProcessor, EmailDiscovery, SalesforceClassifier

# Page configuration
st.set_page_config(
    page_title="Conference Attendee Processor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

def main():
    st.title("📊 Conference Attendee Processor")
    st.markdown("---")
    
    # Check credentials first
    apollo_key = os.getenv("APOLLO_API_KEY")
    sf_username = os.getenv("SALESFORCE_USERNAME")
    sf_password = os.getenv("SALESFORCE_PASSWORD")
    sf_token = os.getenv("SALESFORCE_TOKEN")
    
    # Show credential status
    if not apollo_key or not all([sf_username, sf_password, sf_token]):
        st.error("⚠️ Missing API credentials!")
        st.info("Please configure your environment variables or Streamlit secrets.")
        
        with st.expander("Required Environment Variables"):
            st.code("""
APOLLO_API_KEY=your_apollo_api_key_here
SALESFORCE_USERNAME=your_salesforce_username
SALESFORCE_PASSWORD=your_salesforce_password
SALESFORCE_TOKEN=your_salesforce_security_token
            """)
        return
    
    # Sidebar info
    with st.sidebar:
        st.header("ℹ️ About")
        st.info("""
        **What this tool does:**
        • Discovers attendee email addresses using Apollo
        • Classifies prospects using Salesforce data
        • Applies Rules of Engagement (ROE) qualification
        • Generates organized Excel output with multiple tabs
        
        **Output Categories:**
        • Current Customers
        • Open Opportunities  
        • Qualified Prospects
        • No Salesforce Match
        """)
        
        st.header("📋 Instructions")
        st.markdown("""
        1. Upload your conference attendee CSV file
        2. Ensure CSV has columns: First Name, Last Name, Company, Job Title
        3. Click 'Process Attendees' 
        4. Download the Excel results when complete
        """)
    
    # Main interface
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("📤 Upload Conference Attendees")
        uploaded_file = st.file_uploader(
            "Choose a CSV file",
            type="csv",
            help="Upload a CSV file with columns: First Name, Last Name, Company, Job Title"
        )
        
        if uploaded_file is not None:
            # Preview the uploaded data
            try:
                df = pd.read_csv(uploaded_file)
                st.success(f"✅ File uploaded successfully! Found {len(df)} attendees")
                
                # Show preview
                with st.expander("📋 Preview uploaded data", expanded=False):
                    st.dataframe(df.head(10))
                
                # Validate required columns
                required_columns = ['First Name', 'Last Name', 'Company', 'Job Title']
                missing_columns = [col for col in required_columns if col not in df.columns]
                
                if missing_columns:
                    st.error(f"❌ Missing required columns: {', '.join(missing_columns)}")
                    st.info("Required columns: First Name, Last Name, Company, Job Title")
                else:
                    st.success("✅ All required columns found!")
                    
                    # Processing options
                    st.header("⚙️ Processing Options")
                    
                    col_a, col_b = st.columns(2)
                    with col_a:
                        test_mode = st.checkbox(
                            "Test Mode (Process first 100 attendees only)", 
                            value=True,
                            help="Recommended for testing. Uncheck to process all attendees."
                        )
                    
                    with col_b:
                        if not test_mode:
                            st.warning(f"⚠️ Full processing mode will process all {len(df)} attendees")
                    
                    # Process button
                    if st.button("🚀 Process Attendees", type="primary", use_container_width=True):
                        process_attendees(uploaded_file, test_mode)
                        
            except Exception as e:
                st.error(f"❌ Error reading CSV file: {str(e)}")
                st.info("Please ensure your file is a valid CSV format.")
    
    # Display results if they exist in session state (persists across downloads)
    if 'processing_results' in st.session_state:
        st.markdown("---")
        display_results()
    
    with col2:
        st.header("📊 Status")
        
        # Processing stats placeholder
        if 'processing_stats' not in st.session_state:
            st.info("Upload a CSV file to begin processing")
        else:
            stats = st.session_state.processing_stats
            
            # Display metrics
            col_metric1, col_metric2 = st.columns(2)
            with col_metric1:
                st.metric("Total Processed", stats.get('total', 0))
            with col_metric2:
                st.metric("Emails Found", stats.get('emails_found', 0))
            
            # Progress bar
            if stats.get('total', 0) > 0:
                progress = stats.get('processed', 0) / stats.get('total', 1)
                st.progress(progress)

def process_attendees(uploaded_file, test_mode=True):
    """Process the uploaded CSV file using our existing workflow"""
    
    # Create progress indicators
    progress_placeholder = st.empty()
    status_placeholder = st.empty()
    metrics_placeholder = st.empty()
    
    try:
        with status_placeholder.container():
            st.info("🔄 Initializing processing...")
        
        # Save uploaded file to temporary location
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            temp_csv_path = tmp_file.name
        
        # Create temporary output directory
        temp_output_dir = tempfile.mkdtemp()
        
        # Read the CSV to get total count
        df = pd.read_csv(temp_csv_path)
        total_attendees = len(df)
        processing_limit = 100 if test_mode else total_attendees
        
        with status_placeholder.container():
            if test_mode:
                st.info(f"🧪 Test mode: Processing first {processing_limit} of {total_attendees} attendees")
            else:
                st.info(f"🚀 Full mode: Processing all {total_attendees} attendees")
        
        # Initialize progress tracking
        progress_bar = progress_placeholder.progress(0)
        current_processed = 0
        emails_found = 0
        
        # Create custom processor with progress callbacks
        class StreamlitConferenceProcessor(ConferenceProcessor):
            def __init__(self, output_dir, progress_callback=None, status_callback=None):
                super().__init__(output_dir)
                self.progress_callback = progress_callback
                self.status_callback = status_callback
                self.current_count = 0
                self.email_count = 0
                # Limit processing log to prevent memory issues
                self.processing_log = []
                self.batch_size = 100  # Process in batches
            
            def process_attendee_with_progress(self, attendee, index, total):
                """Process single attendee with progress updates"""
                from datetime import datetime
                
                self.current_count = index + 1
                log_entry = {
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'attendee_num': self.current_count,
                    'name': f"{attendee['first_name']} {attendee['last_name']}",
                    'company': attendee['company'],
                    'title': attendee['title'],
                    'email_discovery': {},
                    'salesforce_classification': {},
                    'final_result': {}
                }
                
                # Update status
                if self.status_callback:
                    self.status_callback(f"Processing {self.current_count}/{total}: {attendee['first_name']} {attendee['last_name']} ({attendee['company']})")
                
                # Email discovery phase with error handling
                log_entry['email_discovery']['started'] = True
                try:
                    email_discovery = EmailDiscovery(self.output_dir)
                    email, email_notes = email_discovery.find_email(attendee)
                except Exception as e:
                    # Handle API failures gracefully
                    email = None
                    email_notes = f"Error during email discovery: {str(e)}"
                
                if email:
                    attendee['email'] = email
                    self.email_count += 1
                    log_entry['email_discovery']['result'] = 'SUCCESS'
                    log_entry['email_discovery']['email_found'] = email
                    log_entry['email_discovery']['method'] = 'Found'  # email_notes is a string, not dict
                    log_entry['email_discovery']['notes'] = email_notes
                else:
                    log_entry['email_discovery']['result'] = 'FAILED'
                    log_entry['email_discovery']['email_found'] = None
                    log_entry['email_discovery']['notes'] = email_notes
                
                # Salesforce classification phase with robust error handling
                log_entry['salesforce_classification']['started'] = True
                try:
                    if hasattr(self, 'sf_classifier'):
                        classification, details = self.sf_classifier.classify_attendee(attendee, email)
                    else:
                        # Initialize SF classifier if needed
                        self.sf_classifier = SalesforceClassifier()
                        classification, details = self.sf_classifier.classify_attendee(attendee, email)
                except Exception as e:
                    classification = 'no_salesforce_match'
                    details = {'classification_reason': f'Salesforce error: {str(e)}'}
                
                # Log Salesforce results
                log_entry['salesforce_classification']['classification'] = classification
                log_entry['salesforce_classification']['details'] = details
                log_entry['salesforce_classification']['reason'] = details.get('classification_reason', 'No specific reason provided')
                
                # Add reason and owner information
                reason = details.get('classification_reason', 'Unknown reason') if classification == 'excluded' else None
                if classification == 'excluded':
                    attendee['reason'] = reason
                elif classification == 'current_customers':
                    attendee['relationship_owner'] = details.get('account_owner')
                elif classification == 'open_opportunities':
                    attendee['opportunity_owner'] = details.get('opportunity_owner')
                
                # Final result
                log_entry['final_result']['category'] = classification
                log_entry['final_result']['has_email'] = email is not None
                log_entry['final_result']['ready_for_outreach'] = classification in ['salesforce_qualified', 'no_salesforce_match'] and email is not None
                
                # Add to processing log (limit size to prevent memory issues)
                if len(self.processing_log) < 1000:  # Keep only last 1000 entries
                    self.processing_log.append(log_entry)
                elif len(self.processing_log) >= 1000:
                    # Keep only recent entries
                    self.processing_log = self.processing_log[-500:] + [log_entry]
                
                # Update progress
                if self.progress_callback:
                    self.progress_callback(self.current_count, total, self.email_count)
                
                # Force garbage collection every 50 attendees
                if self.current_count % 50 == 0:
                    import gc
                    gc.collect()
                
                return classification, email, reason
        
        # Progress callback functions
        def update_progress(current, total, emails_found):
            progress = current / total if total > 0 else 0
            progress_bar.progress(progress)
            
            # Update metrics
            with metrics_placeholder.container():
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Processed", f"{current}/{total}")
                with col2:
                    st.metric("Emails Found", emails_found)
                with col3:
                    completion = f"{progress:.1%}"
                    st.metric("Progress", completion)
        
        def update_status(message):
            with status_placeholder.container():
                st.info(f"🔄 {message}")
        
        # Initialize custom processor
        processor = StreamlitConferenceProcessor(
            temp_output_dir, 
            progress_callback=update_progress,
            status_callback=update_status
        )
        
        # Process attendees one by one with progress updates
        with status_placeholder.container():
            st.info("🔍 Starting email discovery and Salesforce classification...")
        
        # Read attendees and limit for test mode
        attendees = CSVProcessor.read_attendees(temp_csv_path)
        attendees_to_process = attendees[:processing_limit]
        
        # Initialize results structure
        results = {
            'current_customers': [],
            'open_opportunities': [],
            'salesforce_qualified': [],
            'no_salesforce_match': [],
            'excluded': []
        }
        
        # Process each attendee with batch processing to prevent memory issues
        batch_size = 50  # Process in smaller batches
        total_attendees = len(attendees_to_process)
        
        for batch_start in range(0, total_attendees, batch_size):
            batch_end = min(batch_start + batch_size, total_attendees)
            batch = attendees_to_process[batch_start:batch_end]
            
            # Process batch
            for i, attendee in enumerate(batch):
                global_index = batch_start + i
                classification, email, reason = processor.process_attendee_with_progress(
                    attendee, global_index, total_attendees
                )
                
                # Add to appropriate results category
                if classification in results:
                    results[classification].append(attendee)
            
            # Clear batch from memory and force garbage collection after each batch
            import gc
            del batch
            gc.collect()
            
            # Brief pause between batches to let Streamlit update
            import time
            time.sleep(0.01)  # Minimal pause
        
        # Generate Excel output using existing CSVProcessor
        CSVProcessor.write_results(temp_output_dir, results)
        
        # Final status update
        with status_placeholder.container():
            st.success("✅ Processing completed successfully!")
        
        progress_bar.progress(1.0)
        
        # Find the generated Excel file
        excel_file_path = os.path.join(temp_output_dir, "conference_attendees_results.xlsx")
        
        if os.path.exists(excel_file_path):
            # Read Excel file for download
            with open(excel_file_path, 'rb') as f:
                excel_data = f.read()
            
            # Generate processing log
            processing_log_text = generate_processing_log(processor.processing_log)
            
            # Store minimal results in session state to prevent memory issues
            # Only store summary data and file paths, not full datasets
            st.session_state.processing_results = {
                'results_summary': {k: len(v) for k, v in results.items()},  # Only counts
                'excel_data': excel_data,
                'processing_log_text': processing_log_text[:10000] + "... (truncated for memory)" if len(processing_log_text) > 10000 else processing_log_text,  # Truncate large logs
                'excel_file_path': excel_file_path,
                'processed_at': datetime.now().strftime('%Y%m%d_%H%M%S')
            }
            
            # Clear large objects from memory
            del results
            del processing_log_text
            import gc
            gc.collect()
            
            # Display results
            display_results()
            
        else:
            st.error("❌ Excel file was not generated. Please check the logs.")
        
        # Clean up temporary files
        try:
            os.unlink(temp_csv_path)
            # Note: temp_output_dir cleanup handled by OS
        except:
            pass
            
    except Exception as e:
        st.error(f"❌ Error during processing: {str(e)}")
        st.info("Please check your CSV format and try again.")

def display_results():
    """Display processing results from session state"""
    if 'processing_results' not in st.session_state:
        return
    
    stored_results = st.session_state.processing_results
    results_summary = stored_results.get('results_summary', {})
    excel_data = stored_results['excel_data']
    processing_log_text = stored_results['processing_log_text']
    excel_file_path = stored_results['excel_file_path']
    processed_at = stored_results['processed_at']
    
    st.header("📊 Processing Results")
    st.info(f"✅ Results generated at: {processed_at}")
    
    # Display results summary from counts only
    show_results_summary_from_counts(results_summary)
    
    # Download buttons in columns
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        st.download_button(
            label="📥 Download Excel Results",
            data=excel_data,
            file_name=f"conference_results_{processed_at}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
            use_container_width=True
        )
    
    with col2:
        st.download_button(
            label="📋 Download Processing Log",
            data=processing_log_text,
            file_name=f"processing_log_{processed_at}.txt",
            mime="text/plain",
            type="secondary",
            use_container_width=True
        )
    
    with col3:
        # Clear results button
        if st.button("🗑️ Clear Results", use_container_width=True):
            del st.session_state.processing_results
            st.rerun()
    
    # Show preview of results if Excel file still exists
    try:
        if os.path.exists(excel_file_path):
            show_results_preview(excel_file_path)
        else:
            st.warning("Excel file no longer available for preview")
    except Exception as e:
        st.warning(f"Preview unavailable: {str(e)}")

def generate_processing_log(processing_log):
    """Generate a human-readable processing log text file"""
    log_lines = []
    log_lines.append("=" * 80)
    log_lines.append("CONFERENCE ATTENDEE PROCESSING LOG")
    log_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_lines.append("=" * 80)
    log_lines.append("")
    
    for entry in processing_log:
        log_lines.append(f"ATTENDEE #{entry['attendee_num']}: {entry['name']}")
        log_lines.append("-" * 60)
        log_lines.append(f"Company: {entry['company']}")
        log_lines.append(f"Title: {entry['title']}")
        log_lines.append(f"Processed: {entry['timestamp']}")
        log_lines.append("")
        
        # Email Discovery Section
        log_lines.append("📧 EMAIL DISCOVERY:")
        email_disc = entry['email_discovery']
        if email_disc.get('result') == 'SUCCESS':
            log_lines.append(f"   ✅ SUCCESS - Email found: {email_disc['email_found']}")
            log_lines.append(f"   Details: {email_disc.get('notes', 'No details')}")
        else:
            log_lines.append(f"   ❌ FAILED - No email found")
            log_lines.append(f"   Details: {email_disc.get('notes', 'No details')}")
        log_lines.append("")
        
        # Salesforce Classification Section
        log_lines.append("🏢 SALESFORCE CLASSIFICATION:")
        sf_class = entry['salesforce_classification']
        log_lines.append(f"   Category: {sf_class['classification'].upper()}")
        log_lines.append(f"   Reason: {sf_class['reason']}")
        if sf_class.get('details'):
            details = sf_class['details']
            if details.get('matched_record'):
                log_lines.append(f"   Matched Record: {details['matched_record']}")
            if details.get('sf_account_id'):
                log_lines.append(f"   Salesforce Account ID: {details['sf_account_id']}")
            if details.get('customer_designation'):
                log_lines.append(f"   Customer Status: {details['customer_designation']}")
            if details.get('open_opportunities'):
                log_lines.append(f"   Open Opportunities: {details['open_opportunities']}")
            if details.get('last_activity_days'):
                log_lines.append(f"   Last Activity: {details['last_activity_days']} days ago")
            if details.get('roe_check'):
                log_lines.append(f"   ROE Check Details: {details['roe_check']}")
        log_lines.append("")
        
        # Final Result Section
        log_lines.append("📊 FINAL RESULT:")
        final = entry['final_result']
        log_lines.append(f"   Final Category: {final['category'].upper()}")
        log_lines.append(f"   Has Email: {'Yes' if final['has_email'] else 'No'}")
        log_lines.append(f"   Ready for Outreach: {'Yes' if final['ready_for_outreach'] else 'No'}")
        log_lines.append("")
        log_lines.append("=" * 80)
        log_lines.append("")
    
    # Summary statistics
    log_lines.append("PROCESSING SUMMARY:")
    log_lines.append("-" * 30)
    total_processed = len(processing_log)
    emails_found = sum(1 for entry in processing_log if entry['final_result']['has_email'])
    ready_for_outreach = sum(1 for entry in processing_log if entry['final_result']['ready_for_outreach'])
    
    categories = {}
    for entry in processing_log:
        cat = entry['final_result']['category']
        categories[cat] = categories.get(cat, 0) + 1
    
    log_lines.append(f"Total Attendees Processed: {total_processed}")
    log_lines.append(f"Emails Found: {emails_found} ({emails_found/total_processed*100:.1f}%)")
    log_lines.append(f"Ready for Outreach: {ready_for_outreach} ({ready_for_outreach/total_processed*100:.1f}%)")
    log_lines.append("")
    log_lines.append("Category Breakdown:")
    for category, count in categories.items():
        log_lines.append(f"  {category.title().replace('_', ' ')}: {count}")
    
    return "\n".join(log_lines)

def show_results_summary(results):
    """Display processing results summary"""
    # Calculate totals
    totals = {
        'Current Customers': len(results.get('current_customers', [])),
        'Open Opportunities': len(results.get('open_opportunities', [])),
        'Qualified Prospects': len(results.get('salesforce_qualified', [])),
        'No SF Match': len(results.get('no_salesforce_match', [])),
        'Disqualified - ROE': len(results.get('excluded', []))
    }
    
    # Display metrics in columns
    cols = st.columns(len(totals))
    
    for i, (category, count) in enumerate(totals.items()):
        with cols[i]:
            st.metric(category, count)
    
    # Total processed
    total_processed = sum(totals.values())
    st.metric("**Total Processed**", total_processed)

def show_results_summary_from_counts(results_summary):
    """Display processing results summary from count dictionary"""
    # Map internal keys to display names
    display_mapping = {
        'current_customers': 'Current Customers',
        'open_opportunities': 'Open Opportunities',
        'salesforce_qualified': 'Qualified Prospects', 
        'no_salesforce_match': 'No SF Match',
        'excluded': 'Disqualified - ROE'
    }
    
    # Build totals dictionary
    totals = {}
    for key, display_name in display_mapping.items():
        count = results_summary.get(key, 0)
        totals[display_name] = count
    
    # Display metrics in columns
    cols = st.columns(len(totals))
    
    for i, (category, count) in enumerate(totals.items()):
        with cols[i]:
            st.metric(category, count)
    
    # Total processed
    total_processed = sum(totals.values())
    st.metric("**Total Processed**", total_processed)

def show_results_preview_from_data(results):
    """Show preview of results from stored data"""
    st.header("👀 Results Preview")
    
    try:
        # Convert results to DataFrames for display
        tab_data = {}
        tab_names = []
        
        category_mapping = {
            'current_customers': 'Current Customers',
            'open_opportunities': 'Open Opportunities', 
            'salesforce_qualified': 'Qualified Prospects',
            'no_salesforce_match': 'No SF Match',
            'excluded': 'Disqualified - ROE'
        }
        
        for category, display_name in category_mapping.items():
            if category in results and results[category]:
                df = pd.DataFrame(results[category])
                
                # Map internal column names to display names
                column_mapping = {
                    'first_name': 'First Name',
                    'last_name': 'Last Name', 
                    'company': 'Company',
                    'title': 'Job Title',
                    'email': 'Email'
                }
                
                # Rename columns if they exist
                df_renamed = df.rename(columns=column_mapping)
                
                # Select display columns that exist
                display_columns = ['First Name', 'Last Name', 'Company', 'Job Title', 'Email']
                available_columns = [col for col in display_columns if col in df_renamed.columns]
                
                if available_columns:
                    tab_data[display_name] = df_renamed[available_columns]
                    tab_names.append(display_name)
        
        if tab_names:
            # Create tabs for each category
            tabs = st.tabs(tab_names)
            
            for i, tab_name in enumerate(tab_names):
                with tabs[i]:
                    df = tab_data[tab_name]
                    st.dataframe(df, use_container_width=True)
                    st.caption(f"Total: {len(df)} attendees")
        else:
            st.info("No results to display")
                    
    except Exception as e:
        st.error(f"Error previewing results: {str(e)}")

def show_results_preview(excel_file_path):
    """Show preview of the Excel results"""
    st.header("👀 Results Preview")
    
    try:
        # Read all sheets from Excel file
        excel_sheets = pd.read_excel(excel_file_path, sheet_name=None)
        
        # Create tabs for each sheet
        tab_names = list(excel_sheets.keys())
        tabs = st.tabs(tab_names)
        
        for i, (sheet_name, df) in enumerate(excel_sheets.items()):
            with tabs[i]:
                if len(df) > 0:
                    st.dataframe(df, use_container_width=True)
                    st.caption(f"Total: {len(df)} attendees")
                else:
                    st.info(f"No attendees in {sheet_name} category")
                    
    except Exception as e:
        st.error(f"Error previewing results: {str(e)}")

if __name__ == "__main__":
    main()