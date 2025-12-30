"""
Authenticator module using Streamlit's built-in authentication
"""
import streamlit as st
from typing import List, Optional


class Authenticator:
    """Handle authentication with Streamlit's built-in auth and domain restrictions"""
    
    def __init__(self):
        """Initialize authenticator with secrets"""
        self.allowed_domains = st.secrets.get("auth", {}).get("allowed_domains", ["aristok.com"])
        self.allowed_emails = st.secrets.get("auth", {}).get("allowed_emails", [])
        self.blocked_emails = st.secrets.get("auth", {}).get("blocked_emails", [])
        
        # Convert string to list if needed
        if isinstance(self.allowed_domains, str):
            self.allowed_domains = [d.strip() for d in self.allowed_domains.split(",")]
        if isinstance(self.allowed_emails, str):
            self.allowed_emails = [e.strip() for e in self.allowed_emails.split(",")]
        if isinstance(self.blocked_emails, str):
            self.blocked_emails = [e.strip() for e in self.blocked_emails.split(",")]

    def is_authenticated(self) -> bool:
        """Check if user is authenticated using Streamlit's built-in auth"""
        return st.user.is_logged_in
    
    def get_user_email(self) -> Optional[str]:
        """Get authenticated user's email"""
        if self.is_authenticated():
            return st.user.get("email", None)
        return None
    
    def check_email_access(self, email: str) -> bool:
        """Check if email has access"""
        if not email: 
            return False
        
        # Check if blocked
        if email.lower() in [e.lower() for e in self.blocked_emails]:
            return False
            
        # Check if explicitly allowed
        if email.lower() in [e.lower() for e in self.allowed_emails]: 
            return True
        
        # Check domain
        try:
            domain = email.split('@')[-1].lower()
            return domain in [d.lower() for d in self.allowed_domains]
        except:
            return False

    def is_local_development(self) -> bool:
        """Check if running in local development mode"""
        try:
            redirect_uri = st.secrets.get("auth", {}).get("redirect_uri", "")
            return "localhost" in redirect_uri or "127.0.0.1" in redirect_uri
        except:
            return True  # Default to dev mode if secrets not loaded

    def show_login_page(self):
        """Display login page with Streamlit's built-in sign-in"""
        
        # Header
        st.markdown("""
            <div style="text-align: center; padding: 50px 20px;">
                <h1 style="font-size: 3rem; font-weight: 700; background: linear-gradient(90deg, #667eea, #764ba2, #10b981); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin-bottom: 20px;">
                    📊 GA-SF Data Mapping Dashboard
                </h1>
            </div>
        """, unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.markdown("""
                <div style="background: rgba(255, 255, 255, 0.05); border-radius: 12px; padding: 40px; border: 1px solid rgba(255, 255, 255, 0.1);">
                    <h2 style="text-align: center; margin-bottom: 30px;">Sign In</h2>
                    <p style="text-align: center; color: #888; font-size: 0.9em; margin-bottom: 20px;">
                        Sign in with your corporate Google credentials.
                    </p>
                </div>
            """, unsafe_allow_html=True)
            
            st.button("🔐 Sign in with Google", on_click=st.login, width="stretch", type="primary")

    def show_user_info(self):
        """Display logged-in user info in sidebar"""
        if self.is_authenticated():
            user_email = self.get_user_email()
            st.sidebar.markdown("---")
            st.sidebar.markdown(f"**👤 Logged in as:**")
            st.sidebar.caption(user_email)
            
            if st.sidebar.button("🚪 Logout", width="stretch"):
                self.logout()
    
    def logout(self):
        """Logout current user"""
        st.logout()
