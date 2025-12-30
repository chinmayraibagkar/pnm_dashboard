"""
Email notification module for security alerts
"""
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import List
import streamlit as st


def get_email_config():
    """Get email configuration from secrets"""
    return {
        'smtp_server': st.secrets.get("email", {}).get("smtp_server", "smtp.gmail.com"),
        'smtp_port': st.secrets.get("email", {}).get("smtp_port", 587),
        'sender_email': st.secrets.get("email", {}).get("sender_email", ""),
        'sender_password': st.secrets.get("email", {}).get("sender_password", ""),
        'alert_emails': st.secrets.get("security", {}).get("alert_emails", [])
    }


def send_security_alert(
    user_email: str,
    attempt_count: int,
    max_attempts_reached: bool = False
):
    """
    Send security alert email for failed password attempts.
    
    Args:
        user_email: Email of user who attempted reset
        attempt_count: Number of failed attempts
        max_attempts_reached: Whether user exhausted all attempts
    """
    config = get_email_config()
    
    if not config['sender_email'] or not config['sender_password']:
        st.warning("Email configuration not set up. Alert not sent.")
        return False
    
    alert_emails = config['alert_emails']
    if isinstance(alert_emails, str):
        alert_emails = [e.strip() for e in alert_emails.split(",")]
    
    if not alert_emails:
        return False
    
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Create email content
    if max_attempts_reached:
        subject = "🚨 CRITICAL: BigQuery Reset - Maximum Failed Attempts Reached"
        alert_type = "CRITICAL SECURITY ALERT"
        alert_message = f"""
        <p style="color: #dc2626; font-weight: bold;">
            User has exhausted all {attempt_count} password attempts and has been locked out.
        </p>
        """
    else:
        subject = f"⚠️ Security Alert: Failed BigQuery Reset Attempt (Attempt #{attempt_count})"
        alert_type = "SECURITY ALERT"
        alert_message = f"""
        <p>
            Failed password attempt #{attempt_count} detected.
        </p>
        """
    
    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 10px 10px 0 0; }}
            .content {{ background: #f8f9fa; padding: 25px; border: 1px solid #e9ecef; }}
            .footer {{ background: #343a40; color: #adb5bd; padding: 15px; border-radius: 0 0 10px 10px; font-size: 12px; }}
            .alert-box {{ background: #fff3cd; border: 1px solid #ffc107; border-radius: 8px; padding: 15px; margin: 15px 0; }}
            .critical-box {{ background: #f8d7da; border: 1px solid #dc3545; border-radius: 8px; padding: 15px; margin: 15px 0; }}
            .info-row {{ display: flex; margin: 10px 0; }}
            .info-label {{ font-weight: bold; width: 150px; color: #495057; }}
            .info-value {{ color: #212529; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2 style="margin: 0;">📊 GA-SF Mapping Dashboard</h2>
                <p style="margin: 5px 0 0 0; opacity: 0.9;">{alert_type}</p>
            </div>
            <div class="content">
                <div class="{'critical-box' if max_attempts_reached else 'alert-box'}">
                    <h3 style="margin-top: 0;">BigQuery Reset Attempt Failed</h3>
                    {alert_message}
                </div>
                
                <h4>User Details:</h4>
                <div class="info-row">
                    <span class="info-label">User Email:</span>
                    <span class="info-value">{user_email}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Attempt Count:</span>
                    <span class="info-value">{attempt_count}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Timestamp:</span>
                    <span class="info-value">{current_time}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Application:</span>
                    <span class="info-value">GA-SF Mapping Dashboard</span>
                </div>
                
                <p style="margin-top: 20px; color: #6c757d; font-size: 14px;">
                    This is an automated security notification. Please investigate if this activity is unauthorized.
                </p>
            </div>
            <div class="footer">
                <p style="margin: 0;">
                    <strong>Chinmay Raibagkar</strong><br>
                    Analyst - Automation & Technology<br>
                    Aristok Technologies
                </p>
            </div>
        </div>
    </body>
    </html>
    """
    
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = config['sender_email']
        msg['To'] = ", ".join(alert_emails)
        
        html_part = MIMEText(html_body, 'html')
        msg.attach(html_part)
        
        with smtplib.SMTP(config['smtp_server'], config['smtp_port']) as server:
            server.starttls()
            server.login(config['sender_email'], config['sender_password'])
            server.sendmail(config['sender_email'], alert_emails, msg.as_string())
        
        return True
        
    except Exception as e:
        st.warning(f"Failed to send security alert: {str(e)}")
        return False
