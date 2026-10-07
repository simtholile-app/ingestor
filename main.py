import os
import json
import argparse
from config.settings import Settings
import firebase_admin
from firebase_admin import credentials, firestore, db as rtdb
from engine import GlobalIngestionEngine

from adapters.saps_adapter import SAPSAdapter
from adapters.interpol_adapter import InterpolAdapter


def init_firebase():
    """Initializes Firebase Admin SDK only if running in remote production mode.

    Returns a (firestore_client, rtdb_reference_root) tuple.
    In local mode both are None.
    """
    if Settings.STORAGE_MODE == "local":
        return None, None

    creds_json = Settings.FIREBASE_CREDENTIALS
    if creds_json and creds_json.startswith("{"):
        cred_dict = json.loads(creds_json)
        cred = credentials.Certificate(cred_dict)
    elif creds_json and os.path.exists(creds_json):
        cred = credentials.Certificate(creds_json)
    else:
        cred = credentials.ApplicationDefault()

    firebase_admin.initialize_app(cred, {
        "databaseURL": Settings.FIREBASE_DATABASE_URL,
    })
    return firestore.client(), rtdb


def parse_args():
    parser = argparse.ArgumentParser(description="Simtholile ingestor")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Max cases to ingest per adapter. "
            "Overrides INGEST_LIMIT env var. "
            "Pass 0 for no limit (full run)."
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    Settings.validate()

    # CLI --limit takes precedence over env var
    ingest_limit = args.limit if args.limit is not None else Settings.INGEST_LIMIT

    db, realtime_db = init_firebase()

    global_adapters = [
        SAPSAdapter(),
        InterpolAdapter(),
    ]

    engine = GlobalIngestionEngine(db, global_adapters, ingest_limit=ingest_limit, realtime_db=realtime_db)
    engine.run_sync()
