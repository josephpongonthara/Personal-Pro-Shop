from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import time
import pandas as pd
import os

# ----------------- CONFIG -----------------
TARGET_BRAND = "head"  # e.g., 'wilson', 'yonex', 'head', etc.
BASE_URL = "https://www.racketlogger.com"
BRAND_URL = f"{BASE_URL}/racket/{TARGET_BRAND}"
OUTPUT_CSV = f"{TARGET_BRAND}_rackets.csv"
# -----------------------------------------

options = Options()
options.add_argument("--headless")
driver = webdriver.Chrome(options=options)
driver.get(BRAND_URL)
time.sleep(2)

# Get all frame links
soup = BeautifulSoup(driver.page_source, 'html.parser')
#frame_links = soup.select('a[title][href^="/racket/{}"]'.format(TARGET_BRAND))
#selecting links inside the second <td> of each <tr>
rows = soup.select('tr')
frame_links = []
for row in rows:
    tds = row.find_all("td")
    if len(tds) >= 2:
        # Only get the link from the second <td>
        link = tds[1].find("a", href=True)
        if link and f"/racket/{TARGET_BRAND}" in link["href"]:
            frame_links.append(link)
frame_data = []

print(f"Found {len(frame_links)} rackets under brand '{TARGET_BRAND}'")

# Loop through each frame
for link in frame_links:
    frame_name = link.text.strip()
    frame_href = link['href']
    frame_url = BASE_URL + frame_href

    driver.get(frame_url)
    time.sleep(1.5)
    frame_soup = BeautifulSoup(driver.page_source, 'html.parser')

    def extract_value(label_text, span_based=True):
        try:
            if span_based:
                label = frame_soup.find("span", text=label_text)
            else:
                label = frame_soup.find("td", text=label_text)
            if label:
                value_td = label.find_next("td", {"itemprop": "value"})
                return value_td.text.strip() if value_td else None
        except:
            return None
        return None

    specs = {
        "Brand": TARGET_BRAND.capitalize(),
        "Frame": frame_name,
        "Head size": extract_value("Head size"),
        "Stiffness": extract_value("Stiffness"),
        "Recommended tension": extract_value("Recommended tension"),
        "Composition": extract_value("Composition", span_based=False),
        "Power level": extract_value("Power level", span_based=False),
        "Swing speed": extract_value("Swing speed", span_based=False),
    }

    frame_data.append(specs)
    print(f"Scraped: {frame_name}")

df = pd.DataFrame(frame_data)
df.drop_duplicates(inplace=True)
df.to_csv(OUTPUT_CSV, index=False)
print(f"\nData saved to {OUTPUT_CSV}")
print("Full path:", os.path.abspath(OUTPUT_CSV))
print("Columns scraped:", df.columns.tolist())

driver.quit()
