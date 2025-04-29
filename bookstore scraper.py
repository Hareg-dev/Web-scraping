from bs4 import BeautifulStoneSoup
import pandas as pd
import time
import random
import requests
from datetime import datetime
import html
import re

# Headers to mimic a browser
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124"}

# Base URL for pagination
base_url = "http://quotes.toscrape.com/page/{}/"
news = []
page = 1
max_pages = 5  # Limit to ~50 quotes (10 per page)


while page<=max_page:
  url=base_url.format(page)
  try:
    response=requests.get(url,headers=headers,timeout=5)
    response.raise_for_status()
  except requests.RequestException as e:
    print(f"no quotes on page {page} : {e}")
    break

  soup=BeautifulSoup(response.text,"html")
  new_articles=soup.find_all("div",class_="gradient-overlay")
  if nor new_articles:
    print(f"no new in page {page}")
    break

  for new in new_articles:
    try:
      text=new.find("a",href_="").text.strip()
      author = new.find("small", class_="author").text.strip()
            news.append({"New": new})
        except AttributeError:
            print("Skipping a quote")

    print(f"Scraped page {page}")
    time.sleep(random.randint())  # 1-second delay
    page += 1

df = pd.DataFrame(news)
df.to_csv("quotes_paginated.csv", index=False)
print("Data saved to quotes_paginated.csv")