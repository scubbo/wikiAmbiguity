#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Obviously, this is *super* scrappy. Among other things, it counts "See Also"
# links in a given disambiguation page as being relevant
import os
import re
import requests
import datetime
import argparse
from time import sleep
from bs4 import BeautifulSoup
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from threading import Thread

DOMAIN = 'https://en.wikipedia.org'


def is_an_interesting_link(elem):
  if elem.name != 'a' or elem.parent.name != 'li':
    return False

  for parent in elem.parents:
    # Need to add the check for TOCs because some Disambiguation pages
    # are long enough to have their own Tables of Contents - the one
    # for `Aliabad` added 134 entries!
    #
    # The vertical-navbox links brought "Role_of_women_in_religion"'s
    # link count up from 14 to 532!
    if (parent.get('id') is not None and 'toc' in parent.get('id')) \
            or ('class' in parent.attrs and 'vertical-navbox' in parent['class']):
      return False
  return True


def find_complexity_of_page(path):
  soup = soup_of_page(path)
  return len(soup.find(id='mw-content-text').findAll(is_an_interesting_link))


def soup_of_page(path):
  return BeautifulSoup(get_content(path), 'lxml')


def get_content(path, delay=0):
  if delay > 10:
    raise ConnectionRefusedError('Retried 10 times (unsuccessfully) to fetch ' + path)
  sleep(delay)

  try:
    response = requests.get(DOMAIN + path)
  except requests.exceptions.ConnectionError as e:
    # Sometimes we get `Failed to establish a new connection: [Errno 60] Operation timed out`
    return get_content(path, delay + 1)

  if response.status_code == 200:
    return response.text
  elif response.status_code == 429:
    return get_content(path, delay + 1)
  else:
    raise ConnectionRefusedError(
      'Got status code ' + str(response.status_code) + ' from ' + path + ' - text was ' + response.text)


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
    soup = soup_of_page(self.path_of_next_page)

    # Add all the links on that page to the queue
    try:
      self.queue.extend(soup.findAll('div', {'class': 'mw-category'})[0].findAll(is_an_interesting_link))
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
    # currently, that is
    # https://en.wikipedia.org/w/index.php?title=Category:All_disambiguation_pages&pagefrom=%E5%B6%BA%E5%8D%97#mw-pages
    except KeyError:
      # Yes, this duplicates the logic already in the first `try` above, but this should
      # only be called on the final page anyway, so doesn't affect efficiency too much
      links = soup.findAll('div', {'class': 'mw-category'})[0].findAll(is_an_interesting_link)
      if '黑山' in [link.text for link in links]:
        self.have_reached_final_path = True
        self.path_of_next_page = None

    with open('page_provider_log.txt', 'a') as f:
      f.write(str(datetime.datetime.now()) + ' - Refreshed! path_of_next_page is now ' + str(
        self.path_of_next_page) + '\n')


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
  if not should_ignore(link):
    wq.send(link.text.replace(' (disambiguation)', '') + '\t' + str(find_complexity_of_page(link['href'])) + '\n')


# Ignore some less-than-interesting links - for instance,
# "Book:Wikipedia Signpost" was a cool thing that I found
# out about thanks to this, but it's hardly "ambiguous"
#
# User:Jerzy/sandbox currently has 472 links!
def should_ignore(link):
  return link.text.startswith('Book:')\
         or link.text.startswith('User:')


def get_last_disambig_page_from_log():
  # "Give me the last 10 lines of the file, in reverse order"
  reversed_lines = list(open("page_provider_log.txt"))[:-11:-1]
  for i in range(len(reversed_lines)):
    line = reversed_lines[i]
    if 'Refreshed!' in line:
      break  # i retains its value
  else:
    raise RuntimeError("Could not find a restart point in page_provider_log.txt")
  line = reversed_lines[i + 1]
  return line[line.index('/w/index.php?title'):-1]  # -1 to account for the line-break at the end


def main(args):
  if args.restart:
    # TODO - these should be referenced from a central context
    for filename in ['log.txt', 'output.txt', 'page_provider_log.txt']:
      try:
        os.remove(filename)
      except OSError:
        pass
    pp = PageProvider('/wiki/Category:All_disambiguation_pages')
  else:
    restart_point = get_last_disambig_page_from_log()
    with open('page_provider_log.txt', 'a') as f:
      f.write(str(datetime.datetime.now()) + ' - Restarting from ' + restart_point + '\n')
    pp = PageProvider(restart_point)
  # TODO - use `with ThreadPoolExecutor as tpe`
  tpe = ThreadPoolExecutor()
  writing_queue = WritingQueue('output.txt')
  writing_queue.start()
  for idx, link in enumerate(pp):
    while tpe._work_queue.full():
      sleep(0.1)
    tpe.submit(download_from_link, writing_queue, link)
    if not idx % 100 and idx > 0:
      with open('log.txt', 'a') as f:
        f.write(
          str(datetime.datetime.now()) + '\t' +
          'Handled ' + str(idx) + ' links \t' +
          'Threads active: ' + str(len(tpe._threads)) + '\t'
          'Work queue size: ' + str(tpe._work_queue.qsize()) + '\t'
          'Writing queue size: ' + str(len(writing_queue.work_queue)) + '\n')
  with open('log.txt', 'a') as f:
    f.write(str(
      datetime.datetime.now()) +
        ' - Fetching work appears to be finished, but continuing to let the writing queue drain')
  while True:
    sleep(5)
    with open('log.txt', 'a') as f:
      f.write(
        str(datetime.datetime.now()) + '\t' +
        'Handled ' + str(idx) + ' links \t' +
        'Threads active: ' + str(len(tpe._threads)) + '\t'
        'Work queue size: ' + str(tpe._work_queue.qsize()) + '\t'
        'Writing queue size: ' + str(len(writing_queue.work_queue)) + '\n')


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument(
    '--restart',
    dest='restart',
    action='store_true',
    help="If this flag is set, the script will clear all state and start scraping data " +
         "from the beginning. (If absent, the script will start again from the last set of " +
         "disambiguation pages - which will result in some duplication)")
  main(parser.parse_args())
