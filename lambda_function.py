import bs4
import requests
import sqlite3
import os

def lambda_handler(event,context):
    scrape()

def pg_to_bs(url):
  """
    With the url given as its parameter, the function uses the
    request & bs4 packages to retrieve the content of a web page
    If it's successful, it returns the content as BeautifulSoup
    object. If it's not, if returns False.
  """
  
  try:
    webpage = requests.get(url)
    if webpage.status_code == 200:
      webpage_bs = bs4.BeautifulSoup(webpage.content, 'html.parser')
      print("Success")
      return webpage_bs
    elif webpage.status_code == 404:
      print("Item has already been sold : "+url)
      return False
    else:
      print(webpage.status_code+" "+url)
      return False
  except Exception as e:
    print("requests does not work : "+url)
    return False
    
    
def extract_features(bs):
  """
    With the BeautifulSoup object given as its parameter, the function
    extracts and returns the key components from a daangn web page. 
    The key components are region, title, category, price, manner, time.
  """
  
  try:
    price = bs.find("p", {"id": "article-price"}).text.strip()
    price = int(price.replace("원","").replace(",",""))
  except:
    try:
      # takes care of cases when a post is about free sharing
      price = bs.find("p", {"id": "article-price-nanum"}).text.strip()
      price = -1
    except:
      # takes care of cases when a post is a question
      return None, None, None, None, None, None
  
  region = bs.find("div", {"id":"region-name"}).text
  title = bs.find("h1", {"id": "article-title"}).text
  cate_time_split_txt = "∙"
  cate_time = bs.find("p", {"id": "article-category"}).text.split(cate_time_split_txt)
  category = cate_time[0].strip()
  # extracts how many minutes ago a post was uploaded
  time = cate_time[1].strip().split("분")[0]
  manner_split_txt = "°"
  manner = float(bs.find("dd").text.split(manner_split_txt)[0].strip())

  return region, title, category, price, manner, time
  
  
def create_table(dir):
  """
    With the directory path given as its parameter, the function
    creates a table if it does not exist in the directory.
  """
  conn = sqlite3.connect(dir)
  conn.execute('''
    CREATE TABLE IF NOT EXISTS DanngnPage (
      ArticleNum INTEGER PRIMARY KEY NOT NULL,
      Region VARCHAR NOT NULL,
      Title VARCHAR NOT NULL,
      Category VARCHAR NOT NULL,
      Price INTEGER NOT NULL,
      Manner REAL NOT NULL
    )
  ''')
  conn.commit()
  conn.close()
  
  
def scrape():
  """
    Because AWS Lambda only runs a single function, I wrote
    the whole web scraping process inside this function
  """
  TradePages_dir = os.path.abspath(os.getcwd())+'/daangn_pg.db'
  create_table(TradePages_dir)

  # When I first ran this code, the start_num indicated a post
  # that was post 2 hours ago
  count, time, incre, start_num = 0, "-1", 100, 377612683
  base_url = "https://www.daangn.com/articles/"

  # After the first run, the start_num becomes the most recent ArticleNum
  # instead of the fixed number
  conn = sqlite3.connect(TradePages_dir)
  cursor = conn.execute('''
    SELECT * FROM DanngnPage ORDER BY ArticleNum DESC LIMIT 1;
  ''')
  for r in cursor:
    start_num = r[0]+50
  conn.close()

  conn = sqlite3.connect(TradePages_dir)
  # runs web scraping until the latest post was uploaded a minute ago
  while str(time) != "1":
    bs = pg_to_bs(base_url+str(start_num))
    if not bs:
      start_num += incre
      continue
    region, title, category, price, manner, time = extract_features(bs)
    print(str(time))

    if region is not None:
      conn.execute("""
        INSERT INTO DanngnPage VALUES(?, ?, ?, ?, ?, ?);
      """, (start_num, region, title, category, price, manner))
      count += 1
    
    # It takes a lot of time and storage to collect all post. 
    # Thus, I skipped some posts
    start_num += incre
    if count is 50:
      conn.commit()
      count = 0
    
  conn.commit()
  conn.close()
  
  
