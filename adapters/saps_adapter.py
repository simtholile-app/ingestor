import re
import time
import random
import urllib.parse
import requests
import urllib3
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from adapters.base_adapter import BaseAuthorityAdapter

# Suppress insecure HTTPS warnings if fallback is triggered due to missing CA root certificates on Linux runners
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Polite delay between requests to avoid hammering SAPS server
REQUEST_DELAY_SECONDS = 1.0


def _safe_get(url: str, headers: dict = None, timeout: int = 30) -> requests.Response:
    """GET request that falls back gracefully with verify=False if SSL certificate verification fails."""
    req_headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
    }
    if headers:
        req_headers.update(headers)

    session = requests.Session()
    session.cookies.clear()
    try:
        return session.get(url, headers=req_headers, timeout=timeout)
    except Exception as e:
        err_str = str(e)
        if "SSL" in err_str or "CERTIFICATE_VERIFY_FAILED" in err_str or isinstance(e, requests.exceptions.SSLError):
            print(f"  [WARN] SSL certificate verification failed for {url}: {e}")
            print("  [WARN] Retrying request with SSL verification disabled...")
            return session.get(url, headers=req_headers, timeout=timeout, verify=False)
        raise


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
        res = _safe_get(self.LIST_URL, timeout=30)
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
                res = _safe_get(url, timeout=30)
                res.raise_for_status()
                return res
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                # If connection error is SSL related, _safe_get already handled or will raise
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

        # Polite delay with jitter before every detail request to avoid SAPS server throttling & session overlap
        time.sleep(random.uniform(1.0, 1.5))

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
        # SAPS missing person profile pictures specifically use 'thumbnail.php?id=<digits>'
        img_tag = soup.find("img", src=re.compile(r"thumbnail\.php\?id=\d+", re.I))

        if img_tag and img_tag.get("src"):
            data["raw_photo_url"] = urllib.parse.urljoin(self.BASE_URL, img_tag["src"])
        else:
            data["raw_photo_url"] = None

        # --- Minor detection ---
        # Page shows a red "Adult" label under the name. Anything else is treated as a minor
        # so we blur by default rather than expose a child's photo.
        age_cat = (data.get("age_cat") or "").strip().lower()
        data["is_minor"] = bool(age_cat) and age_cat != "adult"

        data.setdefault("full_name", data.get("circulation_number", "SAPS Missing Person"))
        return data