# Simtholile Global Missing Persons Ingestion Engine

An automated, cross-border ingestion pipeline designed to continuously scrape, normalize, and sync missing persons data across global police databases, non-governmental registries, and international law enforcement APIs (e.g., SAPS, INTERPOL).

The pipeline writes normalized case records directly into **Firebase Firestore** (matching the Simtholile Android app models) and tracks state in **Firebase Realtime Database** and **Cloudinary** for image processing.

---

## Folder Structure

```text
simtholile-ingestor/
├── .env.example                # Template for required environment variables
├── requirements.txt            # Python dependencies
├── main.py                     # Entrypoint & Firebase initialization
├── engine.py                   # Core sync engine & Firestore/RTDB/Cloudinary sync
├── config/
│   └── settings.py             # Environment configuration & validation
├── adapters/
│   ├── __init__.py
│   ├── base_adapter.py         # Abstract base class contract for all authorities
│   ├── saps_adapter.py         # South African Police Service scraper module
│   └── interpol_adapter.py     # INTERPOL Yellow Notices API module
└── .github/
    └── workflows/
        └── extract.yml         # Production CI/CD cron workflow (GitHub Actions)
```

---

## Environment & Configuration

Copy `.env.example` to create your local `.env`:

```bash
cp .env.example .env
```

### Environment Variables

| Variable | Description | Example / Default |
|---|---|---|
| `STORAGE_MODE` | Ingestion mode (`local` or `remote`) | `local` (or `remote` for Firestore) |
| `LOCAL_OUTPUT_DIR` | Local storage folder when `STORAGE_MODE=local` | `local_data` |
| `INGEST_LIMIT` | Max cases per adapter per run (0 = unlimited) | `50` |
| `FIREBASE_CREDENTIALS_JSON` | Path to service account JSON file OR raw JSON string | `path/to/serviceAccountKey.json` |
| `FIREBASE_DATABASE_URL` | Firebase Realtime Database URL | `https://your-project-id-default-rtdb.firebaseio.com` |
| `CLOUDINARY_CLOUD_NAME` | Cloudinary Cloud Name | `your_cloud_name` |
| `CLOUDINARY_API_KEY` | Cloudinary API Key | `your_api_key` |
| `CLOUDINARY_API_SECRET` | Cloudinary API Secret | `your_api_secret` |
| `AUTHOR_USER_ID` | Simtholile User UUID set as author for ingested cases | `0rC9Xbe62uQUf69uRNciXYaeR0O2` |

---

## Running on Firestore & Firebase

To run the ingestor in **remote mode** against Firebase Firestore and Realtime Database:

### Step 1: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 2: Configure Remote Credentials in `.env`

Set `STORAGE_MODE=remote` and supply your Firebase + Cloudinary credentials:

```ini
STORAGE_MODE=remote

# Firebase Credentials (path to JSON file OR raw JSON string)
FIREBASE_CREDENTIALS_JSON=./serviceAccountKey.json

# Firebase Realtime Database URL (required for state tracking)
FIREBASE_DATABASE_URL=https://your-project-id-default-rtdb.firebaseio.com

# Cloudinary Configuration (for hosting case photos)
CLOUDINARY_CLOUD_NAME=your_cloud_name
CLOUDINARY_API_KEY=your_api_key
CLOUDINARY_API_SECRET=your_api_secret

# Author UUID
AUTHOR_USER_ID=0rC9Xbe62uQUf69uRNciXYaeR0O2
```

### Step 3: Run the Ingestor

Run a limited run (e.g., 5 cases per adapter) to test:

```bash
python main.py --limit 5
```

Run a full ingestion without limits:

```bash
python main.py --limit 0
```

---

## How Data & Case IDs are Saved on Firebase

When running in **remote mode** (`STORAGE_MODE=remote`), the ingestor writes to both **Firestore** and **Firebase Realtime Database**:

### 1. Firestore Documents Created

For each missing person case ingested, 5 distinct documents are created across collections matching the Android app models:

* **`/people/{personId}`** (`FirestorePerson`): Person demographics, split `firstName` / `lastName` / `displayName`, `searchTokens` (lowercased for search), `identifyingDetails`, `photoUrl`, `createdByUserId`.
* **`/cases/{caseId}`** (`FirestoreCase`): Private case details, `personId`, `sapsCaseNumber` / `circulationNumber`, `externalReferenceId`, `source`, `contact` information, `posterName` ("Simtholile Ingestor"), `posterType` ("ORGANISATION"), `status` ("OPEN"), `createdByUserId`.
* **`/public_cases/{caseId}`** (`FirestorePublicCase`): Safe public feed projection containing `displayName`, `photoUrl`, `summary`, `approximateLocation`, `verificationStatus` ("OFFICIAL"). Uses the exact same document ID as `/cases/{caseId}`.
* **`/cases/{caseId}/authorized_users/{userId}`** (`FirestoreCaseAuthorizedUser`): Subcollection granting `VIEW`, `UPDATE`, `VERIFY`, `MANAGE` permissions to the author (`AUTHOR_USER_ID`).
* **`/reports/{reportId}`** (`FirestoreReport`): Initial report document of type `MISSING_DECLARATION` with `sourceType: OFFICIAL` and `ingestionChannel: OFFICIAL_FEED`.

### 2. Case ID & Ingestion State Tracking in Realtime Database

To prevent duplicate processing on subsequent runs (especially in stateless environments like GitHub Actions), case IDs are recorded in **Firebase Realtime Database**:

* **RTDB Path:** `/ingestion_state/{source_name}/{external_id}`
* **Saved Structure:**
  ```json
  {
    "caseId": "FIRESTORE_CASE_DOC_ID",
    "ingestedAt": { ".sv": "timestamp" }
  }
  ```
* **Deduplication Workflow:**
  1. Before parsing details, the engine fetches active case IDs from the source (e.g., SAPS or INTERPOL).
  2. The engine reads `/ingestion_state/{source_name}` from Realtime Database to load already-ingested IDs.
  3. Any ID already present in RTDB is skipped immediately.
  4. When a new case is successfully written to Firestore, its `external_id` and Firestore `caseId` are set in RTDB.

### 3. Soft Deletes on Case Removal

If a case disappears from the official authority source:
- `/cases/{caseId}` is updated to `status: "CLOSED"`, `resolutionType: "REMOVED_FROM_OFFICIAL_SOURCE"`, and `resolvedAt: SERVER_TIMESTAMP`.
- `/public_cases/{caseId}` is updated to `status: "CLOSED"` and `softDeletedAt: SERVER_TIMESTAMP` (retaining record history while removing it from active public feeds).

---

## Production Deployment (GitHub Actions)

The repository includes a GitHub Actions workflow at `.github/workflows/extract.yml` scheduled to run once a day (`0 0 * * *`).

To enable GitHub Actions execution, configure the following secrets in **Repository Settings → Secrets and variables → Actions**:

* `STORAGE_MODE`: `remote`
* `FIREBASE_CREDENTIALS_JSON`: Entire JSON string of your Firebase Service Account key.
* `FIREBASE_DATABASE_URL`: `https://your-project-id-default-rtdb.firebaseio.com`
* `CLOUDINARY_CLOUD_NAME`: Your Cloudinary cloud name.
* `CLOUDINARY_API_KEY`: Your Cloudinary API key.
* `CLOUDINARY_API_SECRET`: Your Cloudinary API secret.
* `AUTHOR_USER_ID`: `0rC9Xbe62uQUf69uRNciXYaeR0O2`
* `INGEST_LIMIT`: `0` (or leave empty for default)
