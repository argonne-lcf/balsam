from fastapi import Depends, APIRouter, status, HTTPException
from fastapi.security import OAuth2PasswordRequestForm

from balsam.server.models import get_session
from balsam.schemas import UserCreate, UserOut
from balsam.server.models.crud import users
from .password import authenticate_user_password
from .token import user_from_token, create_access_token

router = APIRouter()


@router.post("/login")
def login(form_data: OAuth2PasswordRequestForm = Depends(), db=Depends(get_session)):
    username = form_data.username
    password = form_data.password

    user = authenticate_user_password(db, username, password)
    token = create_access_token(user)
    return {"access_token": token, "token_type": "bearer"}


@router.get("/me", response_model=UserOut)
def profile(user=Depends(user_from_token)):
    return user


@router.post("/register", response_model=UserOut, status_code=status.HTTP_201_CREATED)
def register(user: UserCreate, db=Depends(get_session)):
    if users.user_exists(db, user.username):
        raise HTTPException(status_code=400, detail="Username already taken")

    new_user = users.create_user(db, user.username, user.password)
    db.commit()
    return new_user
