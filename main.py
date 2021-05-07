import random
import socket
import time
from pprint import pprint

import pandas as pandas
import urllib3
from bs4 import BeautifulSoup

import requests


def delay():
    sleep = random.randint(10, 20)  # Delay for 10 to 20 seconds. ====================================================
    print('Sleep:', sleep)  # =======================================================================================
    time.sleep(sleep)


def soup(sp_url, sp_parser, sp_head, sp_parmters, sp_proxy):
    response = requests.get(sp_url, headers=sp_head, params=sp_parmters, proxies=sp_proxy)
    print(response)  # =============================================================================================
    delay()

    if response.status_code == 200:
        sp_soup = BeautifulSoup(response.text, sp_parser)
    else:
        raise (socket.gaierror, urllib3.exceptions.NewConnectionError, urllib3.exceptions.MaxRetryError,
               requests.exceptions.ConnectionError)

    return sp_soup


def scraper(s_url):
    if scraper_testing is True:
        s_url = 'https://www.booking.com/hotel/gr/lato.html'

    s_head = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/89.0.4389.128 Safari/537.36 OPR/75.0.3969.243'}

    try:  # Sometimes connection got error. So catch some exceptions.

        s_soup = soup(s_url, 'html.parser', s_head, None, None)
        # print(s_soup.prettify())  # ================================================================================
        # print(80 * '=')  # =========================================================================================

        try:
            name = s_soup.find('a', {'id': 'hp_hotel_name_reviews'}).text.lstrip().rstrip()
        except Exception:
            name = 'none'

        try:
            rating = s_soup.find('span', {'class': 'bui-rating bui-rating--smaller'}).get('aria-label')
        except Exception:
            rating = 'none'

        try:
            address = s_soup.find('span', {'class': 'hp_address_subtitle js-hp_address_subtitle jq_tooltip'}).text
            address = address.lstrip().rstrip()
        except Exception:
            address = 'none'

        try:
            latitude = s_soup.find('a', {'id': 'hotel_address'}).get('data-atlas-latlng')
        except Exception:
            latitude = 'none'

        try:
            score = s_soup.find('div', {'class': 'bui-review-score__badge'}).text.lstrip()
        except Exception:
            score = 'none'

        try:
            reviews = s_soup.find('div', {'class': 'bui-review-score__text'}).text
            reviews = reviews[0:reviews.index('reviews') - 1].lstrip()
        except Exception:
            reviews = 'none'

        # print('Name:', name)
        # print('Rating:', rating)
        # print('Address:', address)
        # print('Latitude:', latitude)
        # print('Score:', score)
        # print('Reviews:', reviews)

        return [name, rating, address, latitude, score, reviews, s_url]

    except (socket.gaierror, urllib3.exceptions.NewConnectionError, urllib3.exceptions.MaxRetryError,
            requests.exceptions.ConnectionError) as err:

        print('Error connecting:', err)
        return 'error'


def sample_urls():
    su_head = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/89.0.4389.128 Safari/537.36 OPR/75.0.3969.243'}

    su_parmters = {
        'ss': 'Heraklio Town, Heraklio, Greece',
        'offset': '0'
    }

    su_entry_list = ['https://www.booking.com/searchresults.html?']

    su_entry_urls = []
    try:  # Sometimes connection got error. So catch some exceptions.
        for su_cat_urls_cnt, su_cat_url in enumerate(su_entry_list):
            su_soup = soup(su_cat_url, 'html.parser', su_head, su_parmters, None)
            print(su_soup.prettify())  # ===========================================================================
            print(80 * '=')  # =====================================================================================

            su_entry_url_tags = su_soup.find_all('a', {'class': 'js-sr-hotel-link hotel_name_link url'})
            for su_entry_url_tags_cnt, su_entry_url_tag in enumerate(su_entry_url_tags):
                su_entry_urls.append('https://www.booking.com' +
                                     su_entry_url_tag.get('href')[:su_entry_url_tag.get('href').index('?')].lstrip())

            su_loop = su_soup.\
                find('li', {'class': 'bui-pagination__item bui-pagination__next-arrow'}).\
                find_previous('div').text
            # print(su_loop)  # ======================================================================================

            for su_cnt in range(1, int(su_loop)):
                su_parmters['offset'] = su_cnt * 25
                su_soup = soup(su_cat_url, 'html.parser', su_head, su_parmters, None)

                su_entry_url_tags = su_soup.find_all('a', {'class': 'js-sr-hotel-link hotel_name_link url'})
                for su_entry_url_tags_cnt, su_entry_url_tag in enumerate(su_entry_url_tags):
                    su_entry_urls.append('https://www.booking.com' +
                                         su_entry_url_tag.get('href')[:su_entry_url_tag.get('href').index('?')].lstrip())

                print('This is #' + str(su_cnt) + ' from ' + str(su_loop))

        pprint(su_entry_urls)  # ===================================================================================
        print('Sample urls:', len(su_entry_urls))

        su_entry_urls_df = pandas.DataFrame(su_entry_urls)
        su_entry_urls_df = su_entry_urls_df.sample(frac=1)  # Make it random.
        su_entry_urls_df.to_csv('sample urls.csv', index=False)

    except (socket.gaierror, urllib3.exceptions.NewConnectionError, urllib3.exceptions.MaxRetryError,
            requests.exceptions.ConnectionError) as err:
        print('Error connecting:', err)
        return 'error'


def sample():
    sl_headers = ['Name', 'Rating', 'Address', 'Latitude', 'Score', 'Reviews', 'URL']

    sl_entry_urls_df = pandas.read_csv('sample urls.csv')
    # pprint(sl_entry_urls_df) # =====================================================================================

    sl_df = pandas.DataFrame(columns=sl_headers)

    for cnt in range(1, len(sl_entry_urls_df) + 1):
        if cnt % team[1] == team[0]:
            sl_url = sl_entry_urls_df.iloc[cnt - 1]['0']
            sl_row = scraper(sl_url)

            if sl_row == 'error':
                break
            else:
                sl_series = pandas.Series(sl_row, index=sl_headers)
                sl_df = sl_df.append(sl_series, ignore_index=True)

                # Below: Counting, so that you can know which row is it.  # ==========================================
                print('The above is #' + str(cnt) + ' from ' + str(len(sl_entry_urls_df)) + ' entries.')
                print(80 * '=')  # =================================================================================

    pprint(sl_df)  # ===========================================================================================
    sl_df.to_csv(path_or_buf='sample ' + str(team[0]) + '.csv', index=False)


if __name__ == '__main__':
    scraper_testing = True
    team = [0, 1]  # [(#), (Total)]
    # scraper(None)
    # sample_urls()
    # sample()
