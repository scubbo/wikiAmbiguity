#!/usr/bin/env python3

# Obviously, this is *super* scrappy. Among other things, it counts "See Also"
# links in a given disambiguation page as being relevant

import re
import requests
import datetime
from time import sleep
from bs4 import BeautifulSoup
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from threading import Thread

DOMAIN = 'https://en.wikipedia.org'

def isAnInterestingLink(elem):
  return elem.name == 'a' and \
    elem.parent.name == 'li' and \
    not 'toc' in [p.get('id') for p in elem.parents]
    # Need to add the check for TOCs because some Disambiguation pages
    # are long enough to have their own Tables of Contents - the one
    # for `Aliabad` added 134 entries!

def findComplexityOfPage(path):
  soup = soupOfPage(path)
  return len(soup.find(id='mw-content-text').findAll(isAnInterestingLink))

def soupOfPage(path):
  return BeautifulSoup(getContent(path), 'lxml')

def getContent(path, delay=0):
  if delay > 10:
    raise ConnectionRefusedError('Retried 10 times (unsuccessfully) to fetch ' + path)
  sleep(delay)

  try:
    response = requests.get(DOMAIN + path)
  except ConnectionError as e:
    # Sometimes we get `Failed to establish a new connection: [Errno 60] Operation timed out`
    getContent(path, delay+1)

  if response.status_code == 200:
    return response.text
  elif response.status_code == 429:
    return getContent(path, delay+1)
  else:
    raise ConnectionRefusedError('Got status code ' + str(response.status_code) + ' from ' + path + ' - text was ' + response.text)

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
      self.queue.extend(soup.findAll('div', {'class':'mw-category'})[0].findAll(isAnInterestingLink))
    except IndexError:
      with open('page_provider_log.txt', 'a') as f:
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
      links = soup.findAll('div', {'class':'mw-category'})[0].findAll(isAnInterestingLink)
      if '黑山' in [link.text for link in links]:
        self.have_reached_final_path = True
        self.path_of_next_page = None

    with open('page_provider_log.txt', 'a') as f:
      f.write(str(datetime.datetime.now()) + ' - Refreshed! path_of_next_page is now ' + str(self.path_of_next_page) + '\n')

class WritingQueue:

  def __init__(self, output_file):
    self.output_file = output_file
    self.work_queue = deque()

  def send(self, content):
    self.work_queue.append(content)

  def _do_work(self):
    while True:

      popped_content = []
      try:
        # Building a longer string to write on the (untested) assumption
        # that file-writing is slower than in-memory manipulation
        for i in range(10):
          popped_content.append(self.work_queue.popleft())
      except IndexError:
        sleep(1)

      if popped_content:
        with open(self.output_file, 'a') as f:
          f.write(''.join(popped_content))

  def start(self):
    Thread(target=self._do_work).start()

def download_from_link(wq, link):
  wq.send(link.text.replace(' (disambiguation)', '') + '\t' + str(findComplexityOfPage(link['href'])) + '\n')

def main():
  pp = PageProvider('/wiki/Category:All_disambiguation_pages')
  tpe = ThreadPoolExecutor()
  writingQueue = WritingQueue('output.txt')
  writingQueue.start()
  for idx, link in enumerate(pp):
    while tpe._work_queue.full():
      sleep(0.1)
    tpe.submit(download_from_link, writingQueue, link)
    if not idx % 100 and idx > 0:
      with open('log.txt', 'a') as f:
        f.write(str(datetime.datetime.now()) + '\t' +
          'Handled ' + str(idx) + ' links \t' +
          'Threads active: ' + str(len(tpe._threads)) + '\t'
          'Work queue size: ' + str(tpe._work_queue.qsize()) + '\t'
          'Writing queue size: ' + str(len(writingQueue.work_queue)) + '\n')
  with open('log.txt', 'a') as f:
    f.write(str(datetime.datetime.now()) + ' - Fetching work appears to be finished, but continuing to let the writing queue drain')
  while True:
    sleep(5)
    with open('log.txt', 'a') as f:
      f.write(str(datetime.datetime.now()) + '\t' +
        'Handled ' + str(idx) + ' links \t' +
        'Threads active: ' + str(len(tpe._threads)) + '\t'
        'Work queue size: ' + str(tpe._work_queue.qsize()) + '\t'
        'Writing queue size: ' + str(len(writingQueue.work_queue)) + '\n')

if __name__ == '__main__':
  main()