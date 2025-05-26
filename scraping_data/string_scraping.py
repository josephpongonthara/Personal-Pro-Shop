#used to scrape tenniswarehouseuniversity

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import time
import os

options = Options()
options.add_argument("--headless")  # comment out if you want to see the browser
driver = webdriver.Chrome(options=options)
url = "https://twu.tennis-warehouse.com/learning_center/reporter2.php"
driver.get(url)
wait = WebDriverWait(driver, 10)

checkbox_labels = [
    "Brand",
    "Stiffness (lbs/in)",
    "Tension Loss Percent (%)",
    "Spin Potential Ratio"
]

for label in checkbox_labels:
    checkbox_xpath = f'//td[contains(text(), "{label}")]/preceding-sibling::td//input[@type="checkbox"]'
    checkbox = wait.until(EC.element_to_be_clickable((By.XPATH, checkbox_xpath)))
    if not checkbox.is_selected():
        checkbox.click()
    time.sleep(0.2)

"""
# String list (multi-string selector)
string_dropdown = Select(wait.until(EC.element_to_be_clickable((By.NAME, 'zstring[]'))))
string_dropdown.select_by_visible_text("All Strings")
"""

#string list option 2, selecting all string options manually
string_dropdown = Select(wait.until(EC.element_to_be_clickable((By.NAME, 'zstring[]'))))
for option in string_dropdown.options:
    if option.text != "All Strings":  
        string_dropdown.select_by_visible_text(option.text)

#reference tension (need to rotate between high (62 lbs), medium (51 lbs) & low (40 lbs))
reften_dropdown = Select(wait.until(EC.element_to_be_clickable((By.NAME, 'zref_tension'))))
reften_dropdown.select_by_visible_text("Low (40 lbs)")

#swing speed (need to rotate between slow, medium & fast)
swingspeed_dropdown = Select(wait.until(EC.element_to_be_clickable((By.NAME, 'zhammer'))))
swingspeed_dropdown.select_by_visible_text("Slow")

# String material selector
material_dropdown = Select(wait.until(EC.element_to_be_clickable((By.NAME, 'zmaterial'))))
material_dropdown.select_by_visible_text("All")

# sort order
material_dropdown = Select(wait.until(EC.element_to_be_clickable((By.NAME, 'sort1'))))
material_dropdown.select_by_visible_text("String")

#"Get Report" button
submit_button = wait.until(EC.presence_of_element_located((By.ID, 'getreport')))
driver.execute_script("arguments[0].scrollIntoView(true);", submit_button)
time.sleep(0.5)
driver.execute_script("arguments[0].click();", submit_button)
wait.until(lambda driver: len(driver.find_elements(By.XPATH, '//table//tr')) > 5)
html = driver.page_source
driver.quit()
soup = BeautifulSoup(html, 'html.parser')
table = soup.find('table')
df = pd.read_html(StringIO(str(table)))[0]

df.to_csv("string_database.csv", index=False)
print("Data saved to string_database.csv")
print("Saved to:", os.path.abspath("string_database.csv"))
print("Columns scraped:", df.columns.tolist())
