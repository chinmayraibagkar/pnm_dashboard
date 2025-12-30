"""
Authenticator module for Google OAuth and domain restrictions
"""
import streamlit as st
from typing import List, Optional
import google_auth_oauthlib.flow
import requests


class Authenticator:
    """Handle authentication with Google OAuth and domain restrictions"""
    
    def __init__(self):
        """Initialize authenticator with secrets"""
        self.allowed_domains = st.secrets.get("auth", {}).get("allowed_domains", ["aristok.com"])
        self.allowed_emails = st.secrets.get("auth", {}).get("allowed_emails", [])
        self.blocked_emails = st.secrets.get("auth", {}).get("blocked_emails", [])
        
        # Use dedicated oauth section for authentication credentials
        self.client_id = st.secrets.get("oauth", {}).get("client_id")
        self.client_secret = st.secrets.get("oauth", {}).get("client_secret")
        self.redirect_uri = st.secrets.get("auth", {}).get("redirect_uri", "http://localhost:8501")
        
        # Convert string to list if needed
        if isinstance(self.allowed_domains, str):
            self.allowed_domains = [d.strip() for d in self.allowed_domains.split(",")]
        if isinstance(self.allowed_emails, str):
            self.allowed_emails = [e.strip() for e in self.allowed_emails.split(",")]
        if isinstance(self.blocked_emails, str):
            self.blocked_emails = [e.strip() for e in self.blocked_emails.split(",")]

    def is_authenticated(self) -> bool:
        """Check if user is authenticated"""
        return st.session_state.get('authenticated', False)
    
    def get_user_email(self) -> Optional[str]:
        """Get authenticated user's email"""
        return st.session_state.get('user_email', None)
    
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

    def get_flow(self):
        """Create OAuth flow"""
        client_config = {
            "web": {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": [self.redirect_uri]
            }
        }
        return google_auth_oauthlib.flow.Flow.from_client_config(
            client_config,
            scopes=["https://www.googleapis.com/auth/userinfo.email", "openid"],
            redirect_uri=self.redirect_uri
        )

    def show_login_page(self):
        """Display login page with REAL Google OAuth"""
        
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

            # Check for OAuth code in URL
            if "code" in st.query_params:
                try:
                    code = st.query_params["code"]
                    flow = self.get_flow()
                    flow.fetch_token(code=code)
                    credentials = flow.credentials
                    
                    # Get User Info
                    user_info = requests.get(
                        "https://www.googleapis.com/oauth2/v2/userinfo",
                        headers={"Authorization": f"Bearer {credentials.token}"}
                    ).json()
                    
                    email = user_info.get("email")
                    
                    # Check blocked list first
                    if email.lower() in [e.lower() for e in self.blocked_emails]:
                        st.error(f"❌ Access denied for {email}")
                        st.warning("Your account has been blocked. Contact administrator.")
                        st.query_params.clear()
                    elif self.check_email_access(email):
                        st.session_state.authenticated = True
                        st.session_state.user_email = email
                        # Clear params and rerun
                        st.query_params.clear()
                        st.rerun()
                    else:
                        st.error(f"❌ Access denied for {email}")
                        st.warning("Your domain is not authorized.")
                        st.query_params.clear()
                        
                except Exception as e:
                    st.error(f"Authentication failed: {str(e)}")
                    st.query_params.clear()
            
            else:
                # Show Login Button
                try:
                    flow = self.get_flow()
                    auth_url, _ = flow.authorization_url(prompt='consent')
                    
                    st.markdown(f"""
                        <a href="{auth_url}" target="_self" style="text-decoration: none;">
                            <button style="
                                width: 100%;
                                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                                color: white;
                                padding: 15px 30px;
                                border-radius: 10px;
                                border: none;
                                font-weight: 600;
                                font-size: 16px;
                                cursor: pointer;
                                text-align: center;
                                display: flex;
                                justify-content: center;
                                align-items: center;
                                gap: 10px;
                                margin-top: 20px;
                                transition: transform 0.2s;
                            ">
                                🔐 Sign in with Google
                            </button>
                        </a>
                    """, unsafe_allow_html=True)
                    
                except Exception as e:
                    st.error(f"OAuth Configuration Error: {str(e)}")
                    st.info("Check client_id/client_secret in secrets.toml")

    def show_user_info(self):
        """Display logged-in user info in sidebar"""
        if self.is_authenticated():
            user_email = self.get_user_email()
            st.sidebar.markdown("---")
            st.sidebar.markdown(f"**👤 Logged in as:**")
            st.sidebar.caption(user_email)
            
            if st.sidebar.button("🚪 Logout", use_container_width=True):
                self.logout()
    
    def logout(self):
        """Logout current user"""
        st.session_state.authenticated = False
        st.session_state.user_email = None
        st.rerun()
