import time
from datetime import date, datetime
import requests
from typing import List, Dict, Any, Optional
from adapters.base_adapter import BaseAuthorityAdapter

# Same polite delay as the SAPS adapter
REQUEST_DELAY_SECONDS = 1.0


class InterpolAdapter(BaseAuthorityAdapter):

    API_BASE = "https://ws-public.interpol.int/notices/v1/yellow"
    PAGE_SIZE = 100  # maximum the API supports per page

    @property
    def source_name(self) -> str:
        return "INTERPOL"

    @property
    def country_code(self) -> str:
        return "INT"

    # ------------------------------------------------------------------
    # HTTP helper
    # ------------------------------------------------------------------

    def _get_with_retry(self, url: str, params: dict = None, retries: int = 3, backoff: float = 5.0) -> requests.Response:
        """GET with retry/backoff on timeouts and 5xx errors."""
        for attempt in range(1, retries + 1):
            try:
                res = requests.get(
                    url,
                    params=params,
                    headers={"User-Agent": "SimtholileGlobal/1.0"},
                    timeout=30,
                )
                res.raise_for_status()
                return res
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                if attempt == retries:
                    raise
                wait = backoff * attempt
                print(f"  [WARN] Attempt {attempt} failed ({e}). Retrying in {wait}s...")
                time.sleep(wait)
            except requests.exceptions.HTTPError:
                if attempt == retries or res.status_code < 500:
                    raise
                wait = backoff * attempt
                print(f"  [WARN] HTTP {res.status_code} on attempt {attempt}. Retrying in {wait}s...")
                time.sleep(wait)

    # ------------------------------------------------------------------
    # ID fetching — paginated, stops early once we have enough
    # ------------------------------------------------------------------

    def fetch_active_external_ids(self, limit: int = 0) -> List[str]:
        """
        Fetches Yellow Notice entity IDs, paginating until exhausted or limit reached.
        limit=0 means fetch all.
        """
        ids = []
        page = 1

        while True:
            res = self._get_with_retry(
                self.API_BASE,
                params={"resultPerPage": self.PAGE_SIZE, "page": page},
            )
            data = res.json()
            notices = data.get("_embedded", {}).get("notices", [])

            if not notices:
                break

            for notice in notices:
                if "entity_id" in notice:
                    ids.append(notice["entity_id"].replace("/", "-"))

            total_available = data.get("total", 0)
            if len(ids) >= total_available:
                break

            if limit > 0 and len(ids) >= limit:
                break

            page += 1
            time.sleep(REQUEST_DELAY_SECONDS)

        return ids

    # ------------------------------------------------------------------
    # Detail parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _calculate_is_minor(dob_str: Optional[str]) -> bool:
        """Returns True if the person is under 18 based on their date of birth."""
        if not dob_str:
            return False
        for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                dob = datetime.strptime(dob_str, fmt).date()
                today = date.today()
                age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
                return age < 18
            except ValueError:
                continue
        return False

    def parse_case_details(self, external_id: str) -> Dict[str, Any]:
        formatted_id = external_id.replace("-", "/")
        url = f"{self.API_BASE}/{formatted_id}"

        # Polite delay before every detail request
        time.sleep(REQUEST_DELAY_SECONDS)

        res = self._get_with_retry(url)
        details = res.json()

        # Photo: the images link returns a list endpoint; grab the first thumbnail
        raw_photo_url = None
        images_href = details.get("_links", {}).get("images", {}).get("href")
        if images_href:
            try:
                img_res = self._get_with_retry(images_href)
                img_data = img_res.json()
                embedded_images = img_data.get("_embedded", {}).get("images", [])
                if embedded_images:
                    raw_photo_url = embedded_images[0].get("_links", {}).get("self", {}).get("href")
            except Exception:
                pass  # photo is best-effort

        forename = details.get("forename", "")
        name = details.get("name", "")
        full_name = f"{forename} {name}".strip() or "INTERPOL Yellow Notice"

        dob = details.get("date_of_birth")
        is_minor = self._calculate_is_minor(dob)

        nationalities = details.get("nationalities", [])
        country = nationalities[0] if nationalities else "INT"

        return {
            "external_id": external_id,
            "full_name": full_name,
            "gender": details.get("sex_id"),
            "eye_color": (details.get("eyes_colors_id") or [None])[0],
            "hair_color": (details.get("hairs_id") or [None])[0],
            "country": country,
            "date_of_birth": dob,
            "circumstances": details.get("summary") or "Missing person listed under INTERPOL Yellow Notice.",
            "is_minor": is_minor,
            "raw_photo_url": raw_photo_url,
            "source_url": (
                f"https://www.interpol.int/en/How-we-work/Notices/Yellow-Notices"
                f"/View-Yellow-Notices#{formatted_id}"
            ),
        }
