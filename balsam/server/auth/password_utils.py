from passlib.context import CryptContext  # type: ignore

ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")


def verify_password(plain: str, hashed: str) -> bool:
    return bool(ctx.verify(plain, hashed))


def get_hash(password: str) -> str:
    return str(ctx.hash(password))
