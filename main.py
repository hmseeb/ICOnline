import aiohttp
import asyncio
from bs4 import BeautifulSoup
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

base_url = 'https://ic-online.com/all-product.html'
urls = []
categories = []
sub_urls = []
pagination_urls = []

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Cache-Control': 'max-age=0',
}


def homepage_scraper(main_page):
    soup = BeautifulSoup(main_page, 'html.parser')
    div = soup.select('div.all-catePage')[0]
    uls = div.find_all('ul')
    for ul in uls:
        lis = ul.find_all('li')
        for li in lis:
            a = li.find('a', recursive=False)
            url = a['href'].strip()
            category = a.contents[0].strip()
            urls.append(url + '?product_list_limit=120')
            categories.append(category)


def suburl_scraper(response):
    soup = BeautifulSoup(response, 'html.parser')
    container = soup.select('div.product-items')[0]
    print(container.text)
    # for c in container:
    #     products = c.find_all()
    # data = {
    #     'foo':,
    #     'bar':,
    #     'baz':,
    # }
    time.sleep(10)
    # df = pd.DataFrame('/testfiles/data.csv')
    # df.to_csv('/testfiles/data.csv', mode='a')


async def fetch(session, url):
    count = 0
    try:
        async with session.get(url, headers=headers) as response:
            print(response.url)
            if "We can't find products matching the selection." in await response.text():
                return
            return await response.text()

    except aiohttp.ClientError as e:
        print(f"Error fetching {url}: {e}")
        return


async def main(url):
    async with aiohttp.ClientSession() as session:
        tasks = []
        if (isinstance(url, str)):
            return await fetch(session, url)
        else:
            for url in urls:
                task = asyncio.create_task(fetch(session, url))
                tasks.append(task)
        responses = await asyncio.gather(*tasks)
        for response in responses:
            suburl_scraper(response)

        return ''.join(responses)

if __name__ == '__main__':
    start_time = time.time()
    main_page = asyncio.run(main(base_url))
    homepage_scraper(main_page)
    sub_url = asyncio.run(main(urls))
    print(f'Time elapsed: {time.time()-start_time}')
