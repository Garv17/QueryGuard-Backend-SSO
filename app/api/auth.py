# POST /auth/signup → register user
# POST /auth/login → login & get JWT
# POST /auth/forgot-password → generate reset token
# POST /auth/reset-password → reset password with token
# POST /auth/logout → revoke JWT
# GET /auth/me → get current user info

from fastapi import APIRouter, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr
from jose import jwt, JWTError
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from app.database import get_db
from app.utils.models import User, UserToken, Organization
import hashlib
import uuid
import os
import random

router = APIRouter(prefix="/auth", tags=["Auth"])

# --- Config ---
SECRET_KEY = os.getenv("SECRET_KEY", "supersecretkey") 
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

security = HTTPBearer()


# --- Models ---
class UserSignup(BaseModel):
    username: str
    password: str
    email: EmailStr
    org_id: str

class UserLogin(BaseModel):
    username: str
    password: str

class ForgotPassword(BaseModel):
    email: EmailStr

class ResetPassword(BaseModel):
    email: str
    otp: str
    new_password: str

class UserResponse(BaseModel):
    id: str
    username: str
    email: str
    org_id: str

    class Config:
        from_attributes = True


# --- Helpers ---
def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def generate_otp() -> str:
    """Generate a 6-digit OTP"""
    return str(random.randint(100000, 999999))

def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def verify_token(raw_token: str):
    try:
        payload = jwt.decode(raw_token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    raw_token = credentials.credentials
    payload = verify_token(raw_token)
    user_id: str = payload.get("sub")
    if user_id is None:
        raise HTTPException(status_code=401, detail="Invalid token payload")

    # Check if token exists and is not revoked
    token_record = db.query(UserToken).filter(
        UserToken.token == raw_token,
        UserToken.is_revoked == False,
        UserToken.expires_at > datetime.utcnow()
    ).first()
    
    if not token_record:
        raise HTTPException(status_code=401, detail="Token revoked or expired")

    user = db.query(User).filter(User.id == user_id, User.is_active == True).first()
    if not user:
        raise HTTPException(status_code=401, detail="User not found or inactive")
    
    return user


# --- Endpoints ---
@router.post("/signup", status_code=status.HTTP_201_CREATED)
def signup(user: UserSignup, db: Session = Depends(get_db)):
    # Validate org_id format
    try:
        org_uuid = uuid.UUID(user.org_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid organization ID format")
    
    # Check if organization exists and is active
    organization = db.query(Organization).filter(
        Organization.id == org_uuid,
        Organization.is_active == True
    ).first()
    if not organization:
        raise HTTPException(status_code=400, detail="Invalid organization ID or organization is inactive")
    
    # Check if username already exists
    existing_user = db.query(User).filter(User.username == user.username).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="Username already exists")
    
    # Check if email already exists
    existing_email = db.query(User).filter(User.email == user.email).first()
    if existing_email:
        raise HTTPException(status_code=400, detail="Email already exists")

    new_user = User(
        username=user.username,
        email=user.email,
        password_hash=hash_password(user.password),
        org_id=org_uuid
    )
    
    try:
        db.add(new_user)
        db.commit()
        db.refresh(new_user)
        return {"message": "User registered successfully"}
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Registration failed")


@router.post("/login")
def login(user: UserLogin, db: Session = Depends(get_db)):
    db_user = db.query(User).filter(
        User.username == user.username,
        User.is_active == True
    ).first()
    
    if not db_user or db_user.password_hash != hash_password(user.password):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    token = create_access_token(
        data={"sub": str(db_user.id)}, 
        expires_delta=access_token_expires
    )
    
    # Store token in database
    token_record = UserToken(
        user_id=db_user.id,
        token=token,
        expires_at=datetime.utcnow() + access_token_expires
    )
    
    db.add(token_record)
    db.commit()
    
    return {"access_token": token, "token_type": "bearer"}


@router.post("/forgot-password")
def forgot_password(req: ForgotPassword, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == req.email, User.is_active == True).first()
    if not user:
        raise HTTPException(status_code=404, detail="Email not found")

    # Generate 6-digit OTP
    otp = generate_otp()
    user.password_reset_otp = otp
    user.reset_otp_expires = datetime.utcnow() + timedelta(minutes=60)  # 60 minute expiry
    
    db.commit()
    
    # TODO: Send email with OTP
    # In production, implement email sending here
    # For now, return OTP in response (remove this in production)
    return {
        "message": "Password reset OTP generated and sent to email",
        "otp": otp,  # Remove this in production
        "note": "OTP expires in 60 minutes"
    }


@router.post("/reset-password")
def reset_password(req: ResetPassword, db: Session = Depends(get_db)):
    user = db.query(User).filter(
        User.email == req.email,
        User.password_reset_otp == req.otp,
        User.reset_otp_expires > datetime.utcnow(),
        User.is_active == True
    ).first()
    
    if not user:
        raise HTTPException(status_code=400, detail="Invalid or expired OTP")

    user.password_hash = hash_password(req.new_password)
    user.password_reset_otp = None
    user.reset_otp_expires = None
    
    # Revoke all existing tokens for this user
    db.query(UserToken).filter(UserToken.user_id == user.id).update({"is_revoked": True})
    
    db.commit()
    return {"message": "Password reset successful"}


@router.post("/logout")
def logout(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    # Get the token from the request
    # Note: We need to get the raw token to revoke it
    # This is a simplified approach - in production you might want to pass the token explicitly
    
    # For now, we'll revoke all tokens for the user
    db.query(UserToken).filter(UserToken.user_id == current_user.id).update({"is_revoked": True})
    db.commit()
    
    return {"message": "Logged out successfully"}


@router.get("/me", response_model=UserResponse)
def me(current_user: User = Depends(get_current_user)):
    return current_user
