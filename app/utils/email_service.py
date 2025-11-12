"""
Email service utility for sending emails via SMTP
"""
import os
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv
import logging
import ssl

load_dotenv()

logger = logging.getLogger("email_service")

# Email configuration from environment variables
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
SMTP_FROM_EMAIL = os.getenv("SMTP_FROM_EMAIL", SMTP_USER)
SMTP_USE_SSL = os.getenv("SMTP_USE_SSL", "false").lower() == "true"  # Use SSL instead of TLS


def send_otp_email(to_email: str, otp: str) -> bool:
    """
    Send password reset OTP email to the user.
    
    Args:
        to_email: Recipient email address
        otp: 6-digit OTP code
        
    Returns:
        bool: True if email sent successfully, False otherwise
    """
    try:
        # Validate configuration
        if not all([SMTP_HOST, SMTP_USER, SMTP_PASSWORD]):
            logger.error("Email configuration incomplete. Missing SMTP_HOST, SMTP_USER, or SMTP_PASSWORD")
            return False
        
        # Create message
        body = f"""Hello,

You have requested to reset your password for your QueryGuardAI account.

Your password reset OTP is: {otp}

This OTP will expire in 60 minutes.

If you did not request this password reset, please ignore this email.

Best regards,
QueryGuardAI Team"""
        
        msg = MIMEText(body)
        msg['Subject'] = "Password Reset OTP - QueryGuardAI"
        msg['From'] = SMTP_FROM_EMAIL
        msg['To'] = to_email
        
        # Send email
        try:
            if SMTP_USE_SSL:
                # Use SSL (typically port 465)
                context = ssl.create_default_context()
                with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context) as server:
                    server.login(SMTP_USER, SMTP_PASSWORD)
                    server.send_message(msg)
            else:
                # Use TLS (typically port 587)
                with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
                    server.starttls()  # Enable TLS encryption
                    server.login(SMTP_USER, SMTP_PASSWORD)
                    server.send_message(msg)
            
            logger.info(f"Password reset OTP email sent successfully to {to_email}")
            return True
        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"SMTP authentication failed for {to_email}: {str(e)}")
            logger.error("Please check your SMTP_USER and SMTP_PASSWORD")
            return False
        except smtplib.SMTPConnectError as e:
            logger.error(f"SMTP connection failed to {SMTP_HOST}:{SMTP_PORT}: {str(e)}")
            logger.error("Please check your SMTP_HOST and SMTP_PORT")
            return False
        
    except smtplib.SMTPException as e:
        logger.error(f"SMTP error while sending email to {to_email}: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error while sending email to {to_email}: {str(e)}", exc_info=True)
        return False


def send_welcome_email(to_email: str, username: str, otp: str) -> bool:
    """
    Send welcome email to new user with password setup OTP.
    
    Args:
        to_email: Recipient email address
        username: Username of the new user
        otp: 6-digit OTP code for password setup
        
    Returns:
        bool: True if email sent successfully, False otherwise
    """
    try:
        # Validate configuration
        if not all([SMTP_HOST, SMTP_USER, SMTP_PASSWORD]):
            logger.error("Email configuration incomplete. Missing SMTP_HOST, SMTP_USER, or SMTP_PASSWORD")
            return False
        
        # Create message
        body = f"""Hello {username},

Welcome to QueryGuardAI! Your account has been created.

To complete your account setup, please set your password using the OTP below:

Your password setup OTP is: {otp}

This OTP will expire in 60 minutes.

Please use this OTP along with your email address to set your password.

If you did not expect this email, please contact your administrator.

Best regards,
QueryGuardAI Team"""
        
        msg = MIMEText(body)
        msg['Subject'] = "Welcome to QueryGuardAI - Set Your Password"
        msg['From'] = SMTP_FROM_EMAIL
        msg['To'] = to_email
        
        # Send email
        try:
            if SMTP_USE_SSL:
                # Use SSL (typically port 465)
                context = ssl.create_default_context()
                with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context) as server:
                    server.login(SMTP_USER, SMTP_PASSWORD)
                    server.send_message(msg)
            else:
                # Use TLS (typically port 587)
                with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
                    server.starttls()  # Enable TLS encryption
                    server.login(SMTP_USER, SMTP_PASSWORD)
                    server.send_message(msg)
            
            logger.info(f"Welcome email sent successfully to {to_email}")
            return True
        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"SMTP authentication failed for {to_email}: {str(e)}")
            logger.error("Please check your SMTP_USER and SMTP_PASSWORD")
            return False
        except smtplib.SMTPConnectError as e:
            logger.error(f"SMTP connection failed to {SMTP_HOST}:{SMTP_PORT}: {str(e)}")
            logger.error("Please check your SMTP_HOST and SMTP_PORT")
            return False
        
    except smtplib.SMTPException as e:
        logger.error(f"SMTP error while sending welcome email to {to_email}: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error while sending welcome email to {to_email}: {str(e)}", exc_info=True)
        return False

