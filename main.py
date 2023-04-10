import asyncio
import time
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
base_url = 'https://ic-online.com/all-product.html'
urls = []
categories = []
sub_urls = []
pagination_urls = []

# data
mpns = []
skus = []
descriptions = []
manufacturers = []
pdfs = []
data = {}


def fetch_data():
    return {
        'MPN': mpns,
        'SKU': skus,
        'Description': descriptions,
        'Manufacturer': manufacturers,
        'PDF': pdfs,
    }


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Cache-Control': 'max-age=0',
}


def homepage_scraper(home_page):
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


def suburb_scraper(res):
    try:
        soup = BeautifulSoup(res, 'html.parser')
        container = soup.select('div.product-items')[0]
        div = container.find_all('div', class_='item')
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
    except IndexError:
        return
    except AttributeError:
        print("Error: Could not find one or more of the required HTML tags (a.product-item-link, div > div:not([class]):nth-of-type(2), div.desc, div.brand, div > div:not([class]):nth-of-type(5) a).")


async def fetch(session, url):
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
        if isinstance(url, str):
            return await fetch(session, url)
        else:
            for url in urls:
                task = asyncio.create_task(fetch(session, url))
                tasks.append(task)
        responses = await asyncio.gather(*tasks)
        for response in responses:
            suburb_scraper(response)

        return ''.join(responses)

if __name__ == '__main__':
    start_time = time.time()
    main_page = asyncio.run(main(base_url))
    homepage_scraper(main_page)
    sub_url = asyncio.run(main(urls))
    print(f'Scrapping complete! {len(mpns)} items found.')
    frame = pd.DataFrame(fetch_data())
    frame.to_json('data.json', orient='records')
    print(f'Time elapsed: {time.time() - start_time}')
