from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    MONGODB_URI: str = "mongodb+srv://yiscarose:Z754IlLTUkiqfpIZ@by-staging.dxg0wdb.mongodb.net/"

    class Config:
        env_file = ".env"

settings = Settings()