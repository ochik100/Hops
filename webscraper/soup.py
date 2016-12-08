# -*- coding: UTF-8 -*-
import multiprocessing as mp
import Queue
import threading
import time

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

    def run_parallel(self):
        '''
        Runs review webscraper on multiple processes
        '''
        self.soup = self.get_soup(self.url)
        self.get_location_urls()

        coll.remove({})  # be careful with this
        self.locations = {}
        self.locations['california'] = 'https://www.beeradvocate.com/place/directory/9/US/CA/'

        processes = []
        for location, url in self.locations.iteritems():
            proc = mp.Process(target=self.get_places_urls,
                              args=(location, url, ))
            proc.start()
            processes.append(proc)

        for proc in processes:
            proc.join()

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
        '''
        Retrieve US location urls
        '''
        for tag in self.soup.find_all('div', class_='break'):
            for a in tag.find_all('a'):
                if 'directory' in a['href'] and 'US' in a['href']:
                    self.locations[a.text] = self.base_url.format(a['href'])

    def get_places_urls(self, location, location_url):
        '''
        Retrieve the places (breweries for now) for the specified location

        INPUT:
            location (str): name of the location
            location_url (str): url of the location
        '''
        soup = self.get_soup(location_url)
        # for tag in soup.find('table').find('table').find_all('a'):
        #     self.places[tag.text.split()[0]] = self.base_url.format(tag['href'])
        # breweries_url = self.places['Breweries']
        for tag in soup.find('table').find('table').find_all('a')[:1]:
            self.get_list_of_breweries(self.base_url.format(tag['href']))

    def get_list_of_breweries(self, breweries_url):
        '''
        Retrieve a list of breweries

        INPUT:
            breweries_url (str): url of the breweries at a specific location
        '''
        urls = []
        breweries_soup = self.get_soup(breweries_url)
        page_info = breweries_soup.find('table').find_all('tr')

        for tag in page_info[3:-1:2]:
            urls.append(self.base_url.format(tag.find('a')['href']))
            brewery_url = self.base_url.format(tag.find('a')['href'])
            brewery_name = tag.find('a').text
            self.get_list_of_beers(brewery_url, brewery_name)

        try:
            next_page = page_info[1].find_all('a')[-2]['href']
        except:
            next_page = None

        if next_page:
            url = self.base_url.format(next_page)
            self.get_list_of_breweries(url)

    def get_list_of_beers(self, brewery_url, brewery_name):
        '''
        Retrieve a list of beers from the given brewery

        INPUT:
            brewery_url (str): url of the brewery
            brewery_name (str): name of the brewery
        '''
        beers_soup = self.get_soup(brewery_url)
        beer_info = {}
        titles = ['beer_name', 'beer_style', 'abv', 'avg_rating', 'num_ratings', 'bros']
        for tag in beers_soup.find('table').find_all('tr')[3:]:
            url = self.base_url.format(tag.find('a')['href'])
            clean = [x.strip() for x in tag.get_text(', ').split(',')]
            beer_info[url] = dict(zip(titles, clean))

        for url, info in beer_info.iteritems():
            info['brewery_name'] = brewery_name
            self.get_beer_reviews_concurrently(url, info)
            print brewery_name, url

    def get_beer_reviews_concurrently(self, beer_url, beer_info):
        '''
        Retrieve the beer reviews for the specified beer using multiple threads

        INPUT:
            beer_url (str): url of the beer
            beer_info (dict): dictionary of information about the beer
        '''
        reviews_soup = self.get_soup(beer_url)
        ba_score = reviews_soup.find('div', class_='break').find('span').text
        beer_info['weighted_ba_score'] = ba_score

        q = Queue.Queue()
        q.put(reviews_soup)

        count = 25
        first = True
        while not q.empty():
            soup = q.get()
            reviews = soup.find_all('div', class_='user-comment')
            if reviews:
                threads = len(reviews)

                jobs = []
                for i in range(0, threads):
                    thread = threading.Thread(target=self.scrape_beer_review,
                                              args=(reviews[i], beer_info))
                    jobs.append(thread)
                    thread.start()
                for j in jobs:
                    j.join()

                q.task_done()
                first = False

                prep = '{}?view=beer&sort=&start={}'
                next_page = prep.format(beer_url, count)
                more_soup = self.get_soup(next_page)
                q.put(more_soup)
                count += 25
            else:
                if not first:
                    return
                else:
                    pass

            # try:
            #     prep = '{}?view=beer&sort=&start={}'
            #     next_page = prep.format(beer_url, count)
            #     more_soup = self.get_soup(next_page)
            #     q.put(more_soup)
            #     count += 25
            # except:
            #     return

    def scrape_beer_review(self, review, beer_info):
        '''
        Scrape the information from an individual beer review and add it to the database

        INPUT:
            review (bs4 tag): an individual beer review
            beer_info (dict): information about the beer
        '''
        ba_score = float(review.find('span', class_='BAscore_norm').text)
        breakdown = []
        for tag in review.find_all('br')[0]:
            breakdown.append(tag.text)

        try:
            lstfo = {x[0].strip(): x[1].strip() for x in [x.split(":")
                                                          for x in breakdown[0].split('|')]}
            text = breakdown[1].split('★'.decode('utf-8'))[0]
            self.insert_beer_review(beer_info, ba_score, lstfo, text)
        except:
            pass

    def insert_beer_review(self, beer_info, ba_score, lstfo, text):
        '''
        Insert the beer review into the collection

        INPUT:
            beer_info (dict): information about the beer
            ba_score (float): the individual's weighted score of the beer
            lstfo (dict): the individual's look, smell, taste, feel, overall scores of the beer
            text (str): the individual's text review of the beer
        '''
        item = beer_info.copy()
        item['ba_score'] = ba_score
        item.update(lstfo)
        item['text'] = text
        coll.insert_one(item)
        time.sleep(0.25)

    def get_brewery_info(self):
        # not using
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
        # not using
        titles = ['beer_avg', 'num_beers', 'num_place_reviews',
                  'num_place_ratings', 'place_avg']
        brew_info = dict(zip(titles, info))
        # insert into breweries collection

    def get_breweries_urls(self):
        # not using
        breweries = {}
        breweries_soup = self.get_soup(self.places['Breweries'])
        for tag in breweries_soup.find('table').find_all('tr')[3:]:
            if 'profile' in tag.find('a')['href']:
                breweries[self.base_url.format(tag.find('a')['href'])] = tag.find('a').text
        return breweries

    def get_breweries(self, breweries_url):
        # not using
        urls = []
        names = []
        contacts = []
        print breweries_url
        breweries_soup = self.get_soup(breweries_url)
        for tag in breweries_soup.find('table').find_all('tr')[3:-1:2]:
            urls.append(self.base_url.format(tag.find('a')['href']))
            names.append(tag.find('a').text)
        for tag in breweries_soup.find('table').find_all('tr')[4:-1:2]:
            contacts.append(tag.find('td', class_='hr_bottom_dark').get_text('\n'))
        self.breweries = dict(zip(urls, zip(names, contacts)))
        # TODO: next page
