import ssl
import certifi
ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())

import undetected_chromedriver as uc
from bs4 import BeautifulSoup
import time
import csv
import random
import threading
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ─── CONFIG ───────────────────────────────────────────────────────────────────
INPUT_CSV  = "input.csv"        # ← upload your CSV here on GCP
OUTPUT_CSV = "output_INC.csv"   # ← output will be saved here

NUM_WORKERS   = 2          # keep at 2-3 on e2-medium to avoid RAM issues
SAVE_INTERVAL = 20
VERSION_MAIN  = 136        # ← auto-detected below, but set as fallback
# ──────────────────────────────────────────────────────────────────────────────

lock         = threading.Lock()
results      = []
save_counter = [0]

FIELDNAMES = ["profile_url","Industry","Location","Leadership",
              "Year Founded","Website","LinkedIn","Twitter","Rank"]

driver_lock = threading.Lock()


def get_chrome_version():
    """Auto-detect installed Chrome version on Linux."""
    import subprocess
    try:
        out = subprocess.check_output(
            ["google-chrome", "--version"], stderr=subprocess.DEVNULL
        ).decode().strip()
        # e.g. "Google Chrome 136.0.7103.92"
        version = int(out.split()[2].split(".")[0])
        print(f"🔍 Detected Chrome version: {version}")
        return version
    except Exception as e:
        print(f"⚠️  Could not detect Chrome version, using fallback {VERSION_MAIN}: {e}")
        return VERSION_MAIN


def make_driver():
    options = uc.ChromeOptions()

    # ── Headless / server flags (required on GCP) ──
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    )

    # ── Chrome binary on Linux/GCP ──
    options.binary_location = "/usr/bin/google-chrome"

    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.notifications": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
    }
    options.add_experimental_option("prefs", prefs)

    with driver_lock:
        driver = uc.Chrome(
            options=options,
            use_subprocess=True,
            version_main=CHROME_VERSION   # set once at startup
        )
    driver.set_page_load_timeout(25)
    return driver


def get_field(soup, label_text, is_link=False):
    tag = soup.find("strong", string=label_text)
    if not tag:
        return ""
    wrapper = tag.find_next_sibling("div", class_="content-wrapper")
    if not wrapper:
        return ""
    if is_link:
        a = wrapper.find("a")
        return a["href"] if a and a.has_attr("href") else ""
    div = wrapper.find("div", class_="details-container")
    return div.get_text(strip=True) if div else ""


def scrape_profile(driver, url):
    driver.get(url)
    try:
        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located((By.CLASS_NAME, "rank"))
        )
        time.sleep(random.uniform(1.0, 2.0))
    except:
        time.sleep(random.uniform(2.0, 3.0))

    soup = BeautifulSoup(driver.page_source, "html.parser")
    rank_tag = soup.find("h2", class_="rank")
    return {
        "profile_url" : url,
        "Industry"    : get_field(soup, "Industry"),
        "Location"    : get_field(soup, "Location"),
        "Leadership"  : get_field(soup, "Leadership"),
        "Year Founded": get_field(soup, "Year Founded"),
        "Website"     : get_field(soup, "Website",   is_link=True),
        "LinkedIn"    : get_field(soup, "LinkedIn",  is_link=True),
        "Twitter"     : get_field(soup, "Twitter",   is_link=True),
        "Rank"        : rank_tag.get_text(strip=True) if rank_tag else "",
    }


def _save(rows):
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(rows)


def worker(urls, worker_id, total, start_time):
    driver = make_driver()
    try:
        for url in urls:
            try:
                data = scrape_profile(driver, url)
                with lock:
                    results.append(data)
                    save_counter[0] += 1
                    done = save_counter[0]
                    if done % SAVE_INTERVAL == 0:
                        _save(list(results))
                        elapsed = time.time() - start_time
                        avg = elapsed / done
                        eta = avg * (total - done)
                        print(f"  💾 Saved {done}/{total} — ETA {eta/60:.1f} min "
                              f"({avg:.1f}s/profile)")
                    else:
                        print(f"  [{done}/{total}] ✓  {url}")
            except Exception as e:
                print(f"  [W{worker_id}] ✗ {url}  →  {e}")
    finally:
        driver.quit()


def main():
    global CHROME_VERSION
    CHROME_VERSION = get_chrome_version()

    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        rows = [r["profile_url"].strip()
                for r in csv.DictReader(f)
                if r.get("profile_url", "").strip()]
        rows = rows[1500:]

    total = len(rows)
    print(f"📋 {total} profiles  |  {NUM_WORKERS} parallel browsers\n")

    chunks = [rows[i::NUM_WORKERS] for i in range(NUM_WORKERS)]
    start  = time.time()

    threads = []
    for idx, chunk in enumerate(chunks):
        t = threading.Thread(target=worker, args=(chunk, idx+1, total, start))
        t.start()
        threads.append(t)
        time.sleep(3)   # stagger browser launches to avoid race conditions

    for t in threads:
        t.join()

    _save(results)
    elapsed = time.time() - start
    print(f"\n{'='*55}")
    print(f"✅  Done!  {len(results)}/{total} profiles scraped")
    print(f"⏱  {elapsed/60:.1f} min total  |  {elapsed/max(len(results),1):.1f}s per profile")
    print(f"💾  Saved → {OUTPUT_CSV}")
    print(f"{'='*55}")


if __name__ == "__main__":
    main()