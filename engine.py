import os
import json
import time
import requests
import cloudinary
import cloudinary.uploader
from firebase_admin import firestore, db as rtdb
from typing import List, Optional, Set
from config.settings import Settings
from adapters.base_adapter import BaseAuthorityAdapter


def _split_name(full_name: str):
    """Splits a full name into (firstName, lastName, displayName).

    First word becomes firstName, the rest becomes lastName.
    displayName is always the original full_name.
    """
    parts = full_name.strip().split(None, 1)
    first_name = parts[0] if parts else ""
    last_name = parts[1] if len(parts) > 1 else ""
    return first_name, last_name, full_name.strip()


def _build_search_tokens(first_name: str, last_name: str, display_name: str) -> List[str]:
    """Creates lowercased search tokens matching FirestorePerson.searchTokens."""
    raw = set()
    for part in [first_name, last_name]:
        token = part.strip().lower()
        if token:
            raw.add(token)
    # Also add each word of display_name in case middle name was folded in
    for word in display_name.lower().split():
        if word:
            raw.add(word)
    return sorted(raw)


def _parse_height_meters(raw_height: Optional[str]) -> Optional[float]:
    """Attempts to parse a height string like '1.65m' or '165cm' into metres."""
    if not raw_height:
        return None
    h = raw_height.strip().lower().replace(",", ".")
    try:
        if "cm" in h:
            return round(float(h.replace("cm", "").strip()) / 100.0, 2)
        elif "m" in h:
            return round(float(h.replace("m", "").strip()), 2)
        else:
            val = float(h)
            # If value > 3 it's probably cm
            return round(val / 100.0, 2) if val > 3 else round(val, 2)
    except (ValueError, TypeError):
        return None


def _parse_weight_kg(raw_weight: Optional[str]) -> Optional[float]:
    """Attempts to parse a weight string like '65kg' into kg."""
    if not raw_weight:
        return None
    w = raw_weight.strip().lower().replace(",", ".")
    try:
        if "kg" in w:
            return round(float(w.replace("kg", "").strip()), 1)
        elif "lbs" in w or "lb" in w:
            return round(float(w.replace("lbs", "").replace("lb", "").strip()) * 0.453592, 1)
        else:
            return round(float(w), 1)
    except (ValueError, TypeError):
        return None


class GlobalIngestionEngine:

    def __init__(self, db: Optional[firestore.Client], adapters: List[BaseAuthorityAdapter],
                 ingest_limit: int = 0, realtime_db=None):
        self.db = db
        self.adapters = adapters
        self.mode = Settings.STORAGE_MODE
        self.local_dir = Settings.LOCAL_OUTPUT_DIR
        self.author_user_id = Settings.AUTHOR_USER_ID
        # 0 = no limit (full run), any positive int caps per-adapter ingestion
        self.ingest_limit = ingest_limit
        # firebase_admin.db module reference — used for Realtime Database state tracking
        self.rtdb = realtime_db

        if self.mode == "remote":
            cloudinary.config(
                cloud_name=Settings.CLOUDINARY_CLOUD_NAME,
                api_key=Settings.CLOUDINARY_API_KEY,
                api_secret=Settings.CLOUDINARY_API_SECRET,
                secure=True
            )

    # ------------------------------------------------------------------
    # Incremental state helpers
    # ------------------------------------------------------------------

    def _state_path(self, source_name: str) -> str:
        """Returns the path to the local state file that tracks ingested IDs."""
        state_dir = os.path.join(self.local_dir, "state")
        os.makedirs(state_dir, exist_ok=True)
        return os.path.join(state_dir, f"{source_name.lower()}_ingested.json")

    def _load_ingested_ids(self, source_name: str) -> Set[str]:
        """Loads the set of already-ingested external IDs.

        In remote mode reads from Realtime Database (/ingestion_state/<source>/).
        In local mode reads from the local state file on disk.
        """
        if self.mode == "remote" and self.rtdb:
            ref = self.rtdb.reference(f"ingestion_state/{source_name.lower()}")
            data = ref.get() or {}
            return set(data.keys())

        # --- local mode ---
        path = self._state_path(source_name)
        if not os.path.exists(path):
            return set()
        try:
            with open(path, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except (json.JSONDecodeError, OSError):
            # Empty or corrupt state file — rebuild from saved JSON
            print(f"  [WARN] State file {path} is unreadable, rebuilding from saved JSON.")
            cases_path = os.path.join(self.local_dir, "json", f"{source_name.lower()}_cases.json")
            try:
                with open(cases_path, "r", encoding="utf-8") as f:
                    return {str(r["external_reference_id"]) for r in json.load(f)}
            except (json.JSONDecodeError, OSError, KeyError):
                return set()

    def _save_ingested_id(self, source_name: str, ext_id: str, ingested_ids: Set[str],
                          firestore_case_id: str = None):
        """Persists a single newly-ingested ID.

        In remote mode writes to Realtime Database (/ingestion_state/<source>/<ext_id>).
        In local mode writes to the local state file.
        """
        ingested_ids.add(ext_id)

        if self.mode == "remote" and self.rtdb:
            ref = self.rtdb.reference(f"ingestion_state/{source_name.lower()}/{ext_id}")
            ref.set({
                "caseId": firestore_case_id or "",
                "ingestedAt": {".sv": "timestamp"},
            })
            return

        # --- local mode ---
        path = self._state_path(source_name)
        tmp_path = path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(list(ingested_ids), f)
        os.replace(tmp_path, path)

    def _load_known_circulations(self, source_name: str) -> Set[str]:
        """Circulation numbers already saved (uppercased), for de-duplication."""
        if self.mode == "remote":
            # In remote mode we query Firestore directly for dups
            return set()

        filepath = os.path.join(self.local_dir, "json", f"{source_name.lower()}_cases.json")
        if not os.path.exists(filepath):
            return set()
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return {
                    r["circulation_number"].strip().upper()
                    for r in json.load(f)
                    if r.get("circulation_number")
                }
        except (json.JSONDecodeError, OSError):
            return set()

    # ------------------------------------------------------------------
    # Photo processing
    # ------------------------------------------------------------------

    def process_photo(self, image_url: str, case_id: str, is_minor: bool, country_code: str) -> Optional[str]:
        """Handles local file downloads or Cloudinary remote uploads depending on STORAGE_MODE."""
        if not image_url:
            return None

        if self.mode == "local":
            media_dir = os.path.join(self.local_dir, "media", country_code.lower())
            os.makedirs(media_dir, exist_ok=True)
            local_image_path = os.path.join(media_dir, f"{case_id}.jpg")

            try:
                try:
                    res = requests.get(
                        image_url,
                        timeout=15,
                        headers={
                            "User-Agent": "SimtholileDev/1.0",
                            "Referer": "https://www.saps.gov.za/crimestop/missing/",
                        },
                    )
                except requests.exceptions.SSLError:
                    res = requests.get(
                        image_url,
                        timeout=15,
                        verify=False,
                        headers={
                            "User-Agent": "SimtholileDev/1.0",
                            "Referer": "https://www.saps.gov.za/crimestop/missing/",
                        },
                    )
                res.raise_for_status()
                with open(local_image_path, "wb") as f:
                    f.write(res.content)
                print(f"    Saved photo: {local_image_path}")
                return os.path.abspath(local_image_path)
            except Exception as e:
                print(f"    [WARN] Failed to download photo for {case_id}: {e}")
                return None

        else:
            transformation = [
                {'width': 600, 'height': 600, 'crop': 'fill', 'gravity': 'face'},
                {'quality': 'auto', 'fetch_format': 'auto'}
            ]
            if is_minor:
                transformation.append({'effect': 'blur:200'})

            result = cloudinary.uploader.upload(
                image_url,
                folder=f"simtholile/{country_code.lower()}/cases/{case_id}",
                public_id="primary_photo",
                transformation=transformation,
                overwrite=True
            )
            return result.get("secure_url")

    # ------------------------------------------------------------------
    # Local JSON persistence
    # ------------------------------------------------------------------

    def save_local_json(self, source_name: str, records: List[dict]):
        """Merges new records into the existing local JSON file."""
        json_dir = os.path.join(self.local_dir, "json")
        os.makedirs(json_dir, exist_ok=True)
        filepath = os.path.join(json_dir, f"{source_name.lower()}_cases.json")

        # Load existing records so we append rather than overwrite on re-runs
        existing = []
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                try:
                    existing = json.load(f)
                except json.JSONDecodeError:
                    existing = []

        merged = existing + records
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(merged, f, indent=2, ensure_ascii=False)

        print(f"\n  Saved {len(records)} new record(s) to: {filepath}  (total on disk: {len(merged)})")

    # ------------------------------------------------------------------
    # Soft-delete: close cases removed from official source
    # ------------------------------------------------------------------

    def _soft_delete_removed_cases(self, adapter: BaseAuthorityAdapter, active_ids: List[str]):
        """Marks Firestore cases as CLOSED when they vanish from the official source.

        Also soft-deletes the matching public_cases document (sets status=CLOSED
        and adds a softDeletedAt timestamp) instead of hard-deleting it.
        """
        active_set = set(active_ids)

        from google.cloud.firestore_v1.base_query import FieldFilter
        existing_docs = self.db.collection("cases") \
            .where(filter=FieldFilter("source", "==", adapter.source_name)) \
            .where(filter=FieldFilter("status", "==", "OPEN")) \
            .stream()

        for doc in existing_docs:
            case_data = doc.to_dict()
            ext_ref = case_data.get("externalReferenceId")
            if ext_ref and ext_ref not in active_set:
                print(f"  Soft-deleting resolved case [{ext_ref}]...")

                # Update the private case document
                doc.reference.update({
                    "status": "CLOSED",
                    "resolutionType": "REMOVED_FROM_OFFICIAL_SOURCE",
                    "updatedAt": firestore.SERVER_TIMESTAMP,
                    "resolvedAt": firestore.SERVER_TIMESTAMP,
                })

                # Soft-delete the public_cases projection
                pub_ref = self.db.collection("public_cases").document(doc.id)
                pub_ref.update({
                    "status": "CLOSED",
                    "softDeletedAt": firestore.SERVER_TIMESTAMP,
                })

    # ------------------------------------------------------------------
    # Firestore write: aligned to Android Kotlin models
    # ------------------------------------------------------------------

    def _write_to_firestore(self, adapter: BaseAuthorityAdapter, data: dict,
                            case_id: str, photo_url: Optional[str], ext_id: str,
                            circ: Optional[str], ingested_ids: Set[str]):
        """Writes a person, case, public_case, authorized_user, and initial report
        to Firestore, matching the Android app's data model exactly.
        """
        full_name = data.get("full_name", "Unknown")
        first_name, last_name, display_name = _split_name(full_name)
        search_tokens = _build_search_tokens(first_name, last_name, display_name)

        height_meters = _parse_height_meters(data.get("height"))
        weight_kg = _parse_weight_kg(data.get("weight"))

        # --- Identifying details (free text) ---
        id_parts = []
        if data.get("build"):
            id_parts.append(f"Build: {data['build']}")
        if data.get("eye_color"):
            id_parts.append(f"Eye colour: {data['eye_color']}")
        if data.get("hair_color"):
            id_parts.append(f"Hair colour: {data['hair_color']}")
        if data.get("height"):
            id_parts.append(f"Height: {data['height']}")
        if data.get("weight"):
            id_parts.append(f"Weight: {data['weight']}")
        identifying_details = "; ".join(id_parts) if id_parts else None

        # --- /people/{personId} — matches FirestorePerson.kt ---
        person_ref = self.db.collection("people").document()
        person_doc = {
            "id": person_ref.id,
            "firstName": first_name,
            "middleName": None,
            "lastName": last_name,
            "displayName": display_name,
            "searchTokens": search_tokens,
            "dateOfBirth": None,
            "approximateAge": None,
            "gender": data.get("gender"),
            "photoUrl": photo_url,
            "photoStoragePath": None,
            "phoneNumber": None,
            "identifyingDetails": identifying_details,
            "createdByUserId": self.author_user_id,
            "country": adapter.country_code,
            "town": data.get("station"),
            "createdAt": firestore.SERVER_TIMESTAMP,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }
        person_ref.set(person_doc)

        # --- /cases/{caseId} — matches FirestoreCase.kt ---
        case_ref = self.db.collection("cases").document()
        summary = data.get("circumstances")

        case_doc = {
            "id": case_ref.id,
            "personId": person_ref.id,
            "personDisplayName": display_name,
            "personPhotoUrl": photo_url,
            "caseType": "MISSING",
            "status": "OPEN",
            "createdByUserId": self.author_user_id,
            "posterName": "Simtholile Ingestor",
            "posterPhotoUrl": None,
            "posterType": "ORGANISATION",
            "lastKnownLocation": None,
            "lastKnownLocationPrecision": "AREA",
            "lastKnownLocationDescription": data.get("station"),
            "country": adapter.country_code,
            "summary": summary,
            "lastKnownTime": None,
            "visibility": "PUBLIC",
            "sightingCount": 0,
            "disasterEventId": None,
            "displacementId": None,
            "approximateAge": None,
            "ageGroup": None,
            "heightLevel": None,
            "buildType": data.get("build"),
            "complexion": None,
            "topGarment": None,
            "topColor": None,
            "bottomGarment": None,
            "bottomColor": None,
            "timeOfDay": None,
            "gender": data.get("gender"),
            "eyeColour": data.get("eye_color"),
            "hairColour": data.get("hair_color"),
            "heightMeters": height_meters,
            "weightKg": weight_kg,
            "sapsCaseNumber": circ,
            "missingCircumstances": summary,
            "createdAt": firestore.SERVER_TIMESTAMP,
            "updatedAt": firestore.SERVER_TIMESTAMP,
            "lastActivityAt": firestore.SERVER_TIMESTAMP,
            "resolvedAt": None,
            "resolvedByUserId": None,
            "resolutionType": None,
            # Ingestor-specific fields (not in Kotlin model, but kept for sync/queries)
            "source": adapter.source_name,
            "externalReferenceId": ext_id,
            "circulationNumber": circ,
            "sourceUrl": data.get("source_url"),
            "isMinor": data.get("is_minor", False),
            "missingDate": data.get("missing_date"),
        }
        case_ref.set(case_doc)

        # --- /public_cases/{caseId} — matches FirestorePublicCase.kt ---
        public_doc = {
            "id": case_ref.id,
            "caseId": case_ref.id,
            "displayName": display_name,
            "photoUrl": photo_url,
            "posterName": "Simtholile Ingestor",
            "posterPhotoUrl": None,
            "posterType": "ORGANISATION",
            "caseType": "MISSING",
            "status": "OPEN",
            "createdByUserId": self.author_user_id,
            "approximateLocation": data.get("station"),
            "country": adapter.country_code,
            "sightingCount": 0,
            "lastActivityAt": firestore.SERVER_TIMESTAMP,
            "verificationStatus": "OFFICIAL",
            "summary": summary,
            "disasterEventId": None,
            "approximateAge": None,
            "ageGroup": None,
            "heightLevel": None,
            "buildType": data.get("build"),
            "complexion": None,
            "topGarment": None,
            "topColor": None,
            "bottomGarment": None,
            "bottomColor": None,
            "timeOfDay": None,
            "gender": data.get("gender"),
            "eyeColour": data.get("eye_color"),
            "hairColour": data.get("hair_color"),
            "heightMeters": height_meters,
            "weightKg": weight_kg,
            "sapsCaseNumber": circ,
            "missingCircumstances": summary,
        }
        self.db.collection("public_cases").document(case_ref.id).set(public_doc)

        # --- /cases/{caseId}/authorized_users/{userId} — matches FirestoreCaseAuthorizedUser.kt ---
        auth_ref = case_ref.collection("authorized_users").document(self.author_user_id)
        auth_ref.set({
            "userId": self.author_user_id,
            "relationship": None,
            "permissions": ["VIEW", "UPDATE", "VERIFY", "MANAGE"],
            "grantedByUserId": self.author_user_id,
            "grantedAt": firestore.SERVER_TIMESTAMP,
        })

        # --- /reports/{reportId} — initial MISSING_DECLARATION report, matches FirestoreReport.kt ---
        report_ref = self.db.collection("reports").document()
        report_ref.set({
            "id": report_ref.id,
            "caseId": case_ref.id,
            "subjectPersonId": person_ref.id,
            "reporterUserId": self.author_user_id,
            "reporterRelationship": None,
            "reportType": "MISSING_DECLARATION",
            "description": summary,
            "location": None,
            "locationPrecision": "AREA",
            "locationDescription": data.get("station"),
            "observedAt": None,
            "createdAt": firestore.SERVER_TIMESTAMP,
            "sourceType": "OFFICIAL",
            "ingestionChannel": "OFFICIAL_FEED",
            "verificationStatus": "OFFICIAL",
            "visibility": "PUBLIC",
            "photoUrl": photo_url,
            "photoStoragePath": None,
            "clientGeneratedId": report_ref.id,
            "status": "ACTIVE",
            "needs": [],
        })

        print(f"    Written: person/{person_ref.id} + case/{case_ref.id} + public_case + report/{report_ref.id}")

        # Track in Realtime Database
        self._save_ingested_id(adapter.source_name, ext_id, ingested_ids, firestore_case_id=case_ref.id)

    # ------------------------------------------------------------------
    # Main sync loop
    # ------------------------------------------------------------------

    def run_sync(self):
        limit_label = str(self.ingest_limit) if self.ingest_limit > 0 else "unlimited"
        print(f"=== Running Engine in [{self.mode.upper()}] Mode | Limit: {limit_label} per adapter ===")

        for adapter in self.adapters:
            print(f"\n--- Processing [{adapter.source_name}] ---")
            try:
                active_ids = adapter.fetch_active_external_ids()
            except Exception as e:
                print(f"  [ERROR] Failed to fetch active IDs for [{adapter.source_name}]: {e}")
                print(f"  [WARN] Skipping adapter [{adapter.source_name}] for this run.")
                continue

            print(f"Active cases found: {len(active_ids)}")

            # Adapters that can order IDs newest-first do so here, before the limit is applied.
            active_ids = adapter.sort_ids_by_recency(active_ids)

            new_records = []
            # SAPS republishes the same case under several bids, so de-dupe on circulation number
            seen_circulations = self._load_known_circulations(adapter.source_name)

            # ---- Load already-ingested IDs ----
            ingested_ids = self._load_ingested_ids(adapter.source_name)
            pending_ids = [eid for eid in active_ids if eid not in ingested_ids]
            print(f"Already ingested:   {len(ingested_ids)}")
            print(f"Remaining new:      {len(pending_ids)}")

            # Apply limit (0 = no limit)
            if self.ingest_limit > 0:
                pending_ids = pending_ids[: self.ingest_limit]
                print(f"Capped to:          {len(pending_ids)}")

            # Soft-delete cases that vanished from the official source (remote only)
            if self.mode == "remote":
                try:
                    self._soft_delete_removed_cases(adapter, active_ids)
                except Exception as e:
                    print(f"  [ERROR] Failed to run soft-delete check for [{adapter.source_name}]: {e}")

            total = len(pending_ids)
            if total == 0:
                print("  Nothing new to ingest.")
                continue

            start_time = time.time()

            for idx, ext_id in enumerate(pending_ids, start=1):
                # Progress indicator with elapsed time and ETA
                elapsed = time.time() - start_time
                avg_per_record = elapsed / idx if idx > 1 else 0
                eta_seconds = int(avg_per_record * (total - idx))
                eta_str = f"  ETA ~{eta_seconds // 60}m{eta_seconds % 60:02d}s" if idx > 1 else ""
                print(f"  [{idx}/{total}] Ingesting record [{ext_id}]...{eta_str}")

                try:
                    data = adapter.parse_case_details(ext_id)
                except Exception as e:
                    print(f"    [ERROR] Skipping record [{ext_id}]: {e}")
                    continue

                # ---- De-dupe: same circulation number = same case under a different bid ----
                circ = (data.get("circulation_number") or "").strip().upper()
                if circ:
                    is_dup = circ in seen_circulations
                    if not is_dup and self.mode == "remote":
                        from google.cloud.firestore_v1.base_query import FieldFilter
                        is_dup = len(
                            self.db.collection("cases")
                            .where(filter=FieldFilter("source", "==", adapter.source_name))
                            .where(filter=FieldFilter("circulationNumber", "==", circ))
                            .limit(1).get()
                        ) > 0
                    if is_dup:
                        print(f"    [DUP] {circ} already ingested, skipping [{ext_id}]")
                        # Mark as handled so it isn't re-fetched next run
                        self._save_ingested_id(adapter.source_name, ext_id, ingested_ids)
                        continue
                    seen_circulations.add(circ)

                case_id = f"{adapter.country_code}_{ext_id.replace('/', '_')}"

                photo_url = self.process_photo(
                    data.get("raw_photo_url"),
                    case_id,
                    data.get("is_minor", False),
                    adapter.country_code
                )

                if self.mode == "local":
                    payload = {
                        "case_id": case_id,
                        "external_reference_id": ext_id,
                        "source": adapter.source_name,
                        "country_code": adapter.country_code,
                        "full_name": data.get("full_name"),
                        "gender": data.get("gender"),
                        "eye_color": data.get("eye_color"),
                        "hair_color": data.get("hair_color"),
                        "build": data.get("build"),
                        "height": data.get("height"),
                        "weight": data.get("weight"),
                        "circumstances": data.get("circumstances"),
                        "missing_date": data.get("missing_date"),
                        "station": data.get("station"),
                        "circulation_number": circ or None,
                        "station_phone": data.get("station_phone"),
                        "investigating_officer": data.get("investigating_officer"),
                        "contact_number": data.get("contact_number"),
                        "contact_email": data.get("contact_email"),
                        "is_minor": data.get("is_minor", False),
                        "photo_url": photo_url,
                        "source_url": data.get("source_url"),
                    }
                    new_records.append(payload)
                    # Persist state immediately so a crash mid-run doesn't lose progress
                    self._save_ingested_id(adapter.source_name, ext_id, ingested_ids)

                else:
                    self._write_to_firestore(
                        adapter, data, case_id, photo_url, ext_id,
                        circ or None, ingested_ids
                    )

            if self.mode == "local":
                self.save_local_json(adapter.source_name, new_records)