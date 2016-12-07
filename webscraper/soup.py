import multiprocessing as mp
import Queue
import threading

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient

DATABASE_NAME = "beer_advocate"
COLLECTION_NAME = "reviews"

client = MongoClient(connect=False)
db = client[DATABASE_NAME]
coll = db[COLLECTION_NAME]


class ReviewCollector(object):

    def __init__(self, url):
        '''
        Initialize ReviewCollector object
        '''
        self.base_url = 'https://www.beeradvocate.com{}'
        self.url = url
        self.soup = None
        self.locations = {}
        self.places = {}
        self.breweries = None

    def run(self):
        '''
        Runs review webscraper
        '''
        self.soup = self.get_soup(self.url)
        self.get_location_urls()

    def get_soup(self, url):
        '''
        Get soup from given url
        INPUT:
            url (str): URL to be soupified
        OUTPUT:
            soup (BeautifulSoup): soupified url
        '''
        content = requests.get(url).content
        soup = BeautifulSoup(content, 'html.parser')
        return soup

    def get_location_urls(self):
        for tag in self.soup.find_all('div', class_='break'):
            for a in tag.find_all('a'):
                if 'directory' in a['href']:
                    self.locations[a.text] = self.base_url.format(a['href'])

    def get_places_urls(self, location):
        soup = self.get_soup(self.locations[location])
        for tag in soup.find('table').find('table').find_all('a'):
            self.places[tag.text.split()[0]] = self.base_url.format(tag['href'])

    def get_breweries_urls(self):
        # not using
        breweries = {}
        breweries_soup = self.get_soup(self.places['Breweries'])
        for tag in breweries_soup.find('table').find_all('tr')[3:]:
            if 'profile' in tag.find('a')['href']:
                breweries[self.base_url.format(tag.find('a')['href'])] = tag.find('a').text
        return breweries

    def get_breweries(self):
        urls = []
        names = []
        contacts = []
        breweries_soup = self.get_soup(self.places['Breweries'])
        for tag in breweries_soup.find('table').find_all('tr')[3:-1:2]:
            urls.append(self.base_url.format(tag.find('a')['href']))
            names.append(tag.find('a').text)
        for tag in breweries_soup.find('table').find_all('tr')[4:-1:2]:
            contacts.append(tag.find('td', class_='hr_bottom_dark').get_text('\n'))
        self.breweries = dict(zip(urls, zip(names, contacts)))
        # TODO: next page

    def get_brewery_info(self):
        for brewery_url, brewery_info in self.breweries.iteritems():
            brew_soup = self.get_soup(brewery_url)
            info = []
            for tag in brew_soup.find_all('div', class_='break'):
                for span in tag.find_all('span'):
                    try:
                        info.append(float(span.text))
                    except:
                        pass
            self.insert_brewery_info(info)

    def insert_brewery_info(self, info):
        titles = ['beer_avg', 'num_beers', 'num_place_reviews',
                  'num_place_ratings', 'place_avg']
        brew_info = dict(zip(titles, info))
        # insert into breweries collection

    def get_beers_and_info(self):
        beer_info = {}
        titles = ['beer_name', 'beer_type', 'abv', 'avg_rating', 'num_ratings', 'bros']
        for tag in beers_soup.find('table').find_all('tr')[3:]:
            url = self.base_url.format(tag.find('a')['href'])
            clean = [x.strip() for x in tag.get_text(', ').split(',')]
            beer_info[url] = dict(zip(titles, clean))
