import re
import time
import urllib.parse
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from adapters.base_adapter import BaseAuthorityAdapter

# Polite delay between requests to avoid hammering SAPS server
REQUEST_DELAY_SECONDS = 1.0


class SAPSAdapter(BaseAuthorityAdapter):

    LIST_URL = "https://www.saps.gov.za/crimestop/missing/list.php"
    DETAIL_URL = "https://www.saps.gov.za/crimestop/missing/detail.php"
    BASE_URL = "https://www.saps.gov.za/crimestop/missing/"

    # label on the SAPS page (lowercased, no colon) -> key in our payload
    FIELD_MAP = {
        "missing circumstances": "circumstances",
        "missing date": "missing_date",
        "gender": "gender",
        "eye colour": "eye_color",
        "hair colour": "hair_color",
        "height": "height",
        "weight": "weight",
        "build": "build",
        "station": "station",
        "circulation number": "circulation_number",
        "station telephone": "station_phone",
        "investigating officer": "investigating_officer",
        "contact nr": "contact_number",
        "e-mail": "contact_email",
    }

    @property
    def source_name(self) -> str:
        return "SAPS_ZA"

    @property
    def country_code(self) -> str:
        return "ZA"

    def fetch_active_external_ids(self) -> List[str]:
        res = requests.get(self.LIST_URL, headers={"User-Agent": "SimtholileGlobal/1.0"}, timeout=30)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")

        bids = []
        for anchor in soup.find_all("a", href=re.compile(r"detail\.php\?bid=\d+")):
            href = anchor.get("href")
            parsed = urllib.parse.urlparse(href)
            params = urllib.parse.parse_qs(parsed.query)
            if "bid" in params:
                bids.append(params["bid"][0])
        return list(set(bids))

    def sort_ids_by_recency(self, ids: List[str]) -> List[str]:
        """Sort SAPS IDs descending — higher numeric ID = more recently listed."""
        try:
            return sorted(ids, key=lambda x: int(x), reverse=True)
        except ValueError:
            return ids

    def _get_with_retry(self, url: str, retries: int = 3, backoff: float = 5.0) -> requests.Response:
        """GET with retry/backoff on timeout, connection errors or 5xx."""
        for attempt in range(1, retries + 1):
            try:
                res = requests.get(url, headers={"User-Agent": "SimtholileGlobal/1.0"}, timeout=30)
                res.raise_for_status()
                return res
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                if attempt == retries:
                    raise
                wait = backoff * attempt
                print(f"  [WARN] Attempt {attempt} failed ({e}). Retrying in {wait}s...")
                time.sleep(wait)
            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response is not None else 0
                if attempt == retries or status < 500:
                    raise
                wait = backoff * attempt
                print(f"  [WARN] HTTP {status} on attempt {attempt}. Retrying in {wait}s...")
                time.sleep(wait)

    def parse_case_details(self, external_id: str) -> Dict[str, Any]:
        url = f"{self.DETAIL_URL}?bid={external_id}"

        # Polite delay before every detail request
        time.sleep(REQUEST_DELAY_SECONDS)

        res = self._get_with_retry(url)
        soup = BeautifulSoup(res.text, "html.parser")

        data: Dict[str, Any] = {
            "external_id": external_id,
            "source_url": url,
            "country": self.country_code,
        }

        # --- Name + category (Adult / Minor) live in the heading block ---
        h2 = soup.find("h2")
        if h2:
            data["full_name"] = h2.get_text(strip=True)
            cat_tag = h2.find_next_sibling("p")
            data["age_cat"] = cat_tag.get_text(strip=True) if cat_tag else ""

        # --- Label/value table rows ---
        for tr in soup.find_all("tr"):
            tds = tr.find_all("td", recursive=False)
            if len(tds) != 2:
                continue
            label_tag = tds[0].find("b")
            if not label_tag:
                continue
            label = label_tag.get_text(strip=True).rstrip(":").strip().lower()
            key = self.FIELD_MAP.get(label)
            if not key:
                continue

            if key == "contact_email":
                mail = tds[1].find("a", href=re.compile(r"^mailto:", re.I))
                value = mail["href"][7:].strip() if mail else tds[1].get_text(strip=True)
            else:
                value = tds[1].get_text(" ", strip=True)

            if value:
                data[key] = value

        # Normalise 2026/05/10 -> 2026-05-10
        if data.get("missing_date"):
            data["missing_date"] = data["missing_date"].replace("/", "-")

        # --- Photo ---
        img_tag = soup.find("img", src=re.compile(r"thumbnail\.php|image\.php|photo", re.I))
        if img_tag and img_tag.get("src"):
            data["raw_photo_url"] = urllib.parse.urljoin(self.BASE_URL, img_tag["src"])

        # --- Minor detection ---
        # Page shows a red "Adult" label under the name. Anything else is treated as a minor
        # so we blur by default rather than expose a child's photo.
        age_cat = (data.get("age_cat") or "").strip().lower()
        data["is_minor"] = bool(age_cat) and age_cat != "adult"

        data.setdefault("full_name", data.get("circulation_number", "SAPS Missing Person"))
        return data