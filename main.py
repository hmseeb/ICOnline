import asyncio
import time
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
import os

base_url = 'https://ic-online.com/all-product.html'
urls = []
categories = []

# data
mpns = []
skus = []
descriptions = []
manufacturers = []
pdfs = []

# scrapped data
scrapped_mpns = []
scrapped_skus = []
scrapped_descriptions = []
scrapped_manufacturers = []
scrapped_pdfs = []

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    "Cache-Control": "no-cache, max-age=0"
}


async def fetch_homepage(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()


async def homepage_scraper(home_page):
    soup = BeautifulSoup(home_page, 'html.parser')
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


async def scrape(res):
    try:
        soup = BeautifulSoup(res, 'html.parser')
        container = soup.select_one('div.product-items')
        div = container.find_all('div', class_='tr item')
        for d in div:
            mpn = d.select('a.product-item-link')
            sku = d.select('div > div:not([class]):nth-of-type(2)')
            des = d.find_all('div', class_='desc')
            man = d.find_all('div', class_='brand')
            pdf = d.select('div > div:not([class]):nth-of-type(5) a')
            for mp, sk, de, ma, p in zip(mpn, sku, des, man, pdf):
                mpns.append(mp.text.strip())
                skus.append(sk.text.strip())
                descriptions.append(de.text.strip())
                manufacturers.append(ma.text.strip())
                pdfs.append(p['href'])
        data = {
            'MPN': mpns,
            'SKU': skus,
            'Description': descriptions,
            'Manufacturer': manufacturers,
            'PDF': pdfs,
        }
        frame = pd.DataFrame(data)
        csv = 'data.csv'
        frame.to_csv(csv, mode='a', header=not os.path.isfile(
            'data.csv'), index=False)
        frame.to_json('data.json', orient='records', lines=True, mode='a')
        df = pd.read_csv(csv)
        print(f'Scrapped {len(df)} items so far.')
        mpns.clear()
        skus.clear()
        descriptions.clear()
        manufacturers.clear()
        pdfs.clear()
    except Exception as e:
        print(f'Error: {e}')


async def url_scraper(response, session):  # Got a url response from list
    url = response.url
    response = await response.text()
    if url in urls:
        await scrape(response)  # Scrape first url
    page_counter = 2
    while True:
        url = f"{url}&p={page_counter}"
        response = await fetch(session, url)
        if response is None:
            return
        else:
            await scrape(response)
            page_counter += 1


async def fetch(session, url):
    try:
        if url in urls:
            async with session.get(url, headers=headers) as response:
                await url_scraper(response, session)
        else:
            async with session.get(url, headers=headers) as response:
                url = response.url
                response = await response.text()
                if 'Manufacturer Part No' in response or str(url) == base_url:
                    return response
                else:
                    return None
    except aiohttp.ClientError as e:
        print(f"Error fetching {url}: {e}")
        return '404'


semaphore = asyncio.Semaphore(10)


async def main():
    async with semaphore:
        async with aiohttp.ClientSession() as session:
            tasks = []
            for url in urls[:]:
                task = asyncio.ensure_future(fetch(session, url))
                tasks.append(task)
            await asyncio.gather(*tasks)


if __name__ == '__main__':
    start_time = time.time()
    main_page = asyncio.run(fetch_homepage(base_url))
    asyncio.run(homepage_scraper(main_page))
    asyncio.run(main())
    print(f'Time elapsed: {float((time.time() - start_time) / 60)} minutes.')
