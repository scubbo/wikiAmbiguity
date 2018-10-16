#!/usr/bin/env python3

# Obviously, this is *super* scrappy. Among other things, it counts "See Also"
# links in a given disambiguation page as being relevant

import re
import requests
import datetime
from time import sleep
from bs4 import BeautifulSoup
from collections import deque

DOMAIN = 'https://en.wikipedia.org'

def isALinkAndIsInALi(elem):
  return elem.name == 'a' and elem.parent.name == 'li'

def findComplexityOfPage(path):
  soup = soupOfPage(path)
  return len(soup.find(id='mw-content-text').findAll(isALinkAndIsInALi))

def soupOfPage(path):
  return BeautifulSoup(requests.get(DOMAIN + path).text, 'lxml')

class PageProvider:
  def __init__(self, first_page):
    self.path_of_next_page = first_page
    self.have_reached_final_path = False
    self.queue = deque()
    self.refresh()

  def __iter__(self):
    return self

  def __next__(self):
    try:
      return self.queue.popleft()
    except IndexError:
      if self.have_reached_final_path:
        raise StopIteration
      self.refresh()
      return self.queue.popleft()

  def refresh(self):
    soup = soupOfPage(self.path_of_next_page)

    # Add all the links on that page to the queue
    try:
      self.queue.extend(soup.findAll('div', {'class':'mw-category'})[0].findAll(isALinkAndIsInALi))
    except IndexError:
      with open('log.txt', 'a') as f:
        f.write('Got an index error when trying to refresh! Logging url and soup below')
        f.write(self.path_of_next_page)
        f.write(str(soup))
        raise

    # Set the path to the next page of links
    try:
      self.path_of_next_page = soup.find(id='mw-pages').findAll(text=re.compile('next page'))[0].parent['href']
    # The "next page" text on the final page has no href -
    # currently, that is https://en.wikipedia.org/w/index.php?title=Category:All_disambiguation_pages&pagefrom=%E5%B6%BA%E5%8D%97#mw-pages
    except KeyError:
      # Yes, this duplicates the logic already in the first `try` above, but this should
      # only be called on the final page anyway, so doesn't affect efficiency too much
      links = soup.findAll('div', {'class':'mw-category'})[0].findAll(isALinkAndIsInALi)
      if '黑山' in [link.text for link in links]:
        self.have_reached_final_path = True
        self.path_of_next_page = None

    with open('log.txt', 'a') as f:
      f.write(str(datetime.datetime.now()) + ' - Refreshed! path_of_next_page is now ' + str(self.path_of_next_page) + '\n')

def main():
  pp = PageProvider('/wiki/Category:All_disambiguation_pages')
  for idx, link in enumerate(pp):
    with open('output.txt', 'a') as f:
      f.write(link.text.replace(' (disambiguation)', '') + '\t' + str(findComplexityOfPage(link['href'])) + '\n')
    if not idx % 100 and idx > 0:
      print(str(datetime.datetime.now()) + '\t' + 'Handled ' + str(idx) + ' links \t')

if __name__ == '__main__':
  main()