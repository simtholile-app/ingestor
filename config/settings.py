import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    STORAGE_MODE = (os.getenv("STORAGE_MODE") or "local").strip().lower()  # 'local' or 'remote'
    LOCAL_OUTPUT_DIR = os.getenv("LOCAL_OUTPUT_DIR") or "local_data"

    # Max cases to ingest per adapter per run.
    # Dev default: 50  |  Launch: 500  |  Full: 0 (no limit)
    # Override at runtime with --limit flag or INGEST_LIMIT env var.
    _raw_limit = os.getenv("INGEST_LIMIT")
    INGEST_LIMIT = int(_raw_limit) if _raw_limit is not None and _raw_limit.strip() != "" else 50

    FIREBASE_CREDENTIALS = os.getenv("FIREBASE_CREDENTIALS_JSON")
    FIREBASE_DATABASE_URL = os.getenv("FIREBASE_DATABASE_URL")
    CLOUDINARY_CLOUD_NAME = os.getenv("CLOUDINARY_CLOUD_NAME")
    CLOUDINARY_API_KEY = os.getenv("CLOUDINARY_API_KEY")
    CLOUDINARY_API_SECRET = os.getenv("CLOUDINARY_API_SECRET")

    # The Simtholile user ID used as the author for all ingested cases.
    AUTHOR_USER_ID = os.getenv("AUTHOR_USER_ID") or "0rC9Xbe62uQUf69uRNciXYaeR0O2"

    @classmethod
    def validate(cls):
        if cls.STORAGE_MODE not in ["local", "remote"]:
            raise ValueError("STORAGE_MODE must be set to either 'local' or 'remote'.")

        # Only check Firebase & Cloudinary keys if running in production remote mode
        if cls.STORAGE_MODE == "remote":
            missing = [key for key, val in {
                "FIREBASE_CREDENTIALS_JSON": cls.FIREBASE_CREDENTIALS,
                "FIREBASE_DATABASE_URL": cls.FIREBASE_DATABASE_URL,
                "CLOUDINARY_CLOUD_NAME": cls.CLOUDINARY_CLOUD_NAME,
                "CLOUDINARY_API_KEY": cls.CLOUDINARY_API_KEY,
                "CLOUDINARY_API_SECRET": cls.CLOUDINARY_API_SECRET,
            }.items() if not val]

            if missing:
                raise ValueError(f"Missing required production environment variables: {', '.join(missing)}")