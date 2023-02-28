import bs4
import requests
import sqlite3
import os



def pg_to_bs(url):
	req = requests.get(url)

	if req.ok:
		return bs4.BeautifulSoup(req.content, "html.parser")
	else:
		print("Error in page response.")
		return None


def extract_features(soup):
	cards = []
	for card in soup.find_all("div", class_="card"):
		# Format is: (Title, Employer, Location, Time)
		cards.append((
			card.find("h2").string.strip(),
			card.find("h3").string.strip(),
			card.find("p", class_="location").string.strip(),
			card.find("time").string.strip(),
		))

	return cards

def create_table(dir):
    """
      With the directory path given as its parameter, the function
      creates a table if it does not exist in the directory.
    """
    conn = sqlite3.connect(dir)
    conn.execute('''
    CREATE TABLE IF NOT EXISTS Jobs (
	  ID INTEGER PRIMARY KEY AUTOINCREMENT,
      JobTitle VARCHAR NOT NULL,
      Employers VARCHAR NOT NULL,
      Location VARCHAR NOT NULL,
      Date VARCHAR NOT NULL
    )
    ''')
    conn.commit()
    conn.close()


def scrape():
	TradePages_dir = os.path.abspath(os.getcwd()) + '/jobs.db'
	create_table(TradePages_dir)

	base_url = "https://realpython.github.io/fake-jobs/"

	cards = [] # Stops linter from bitching
	
	if (soup == pg_to_bs(base_url)):
		cards = extract_features(soup)
	else:
		raise Exception("Page error")
	
	conn = sqlite3.connect(TradePages_dir)
	curs = conn.cursor()
	curs.execute("DELETE FROM Jobs") # Erases table, as we will rewrite each new parse.
	conn.commit()
	
	# Start actual storage job
	for card in cards:
		curs.execute(
			"INSERT INTO Jobs (JobTitle, Employers, Location, Date) VALUES(?, ?, ?, ?);",
			card
		)

	conn.commit()
	conn.close()

scrape()