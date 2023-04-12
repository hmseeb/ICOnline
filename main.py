import asyncio
import time
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

base_url = 'https://ic-online.com/all-product.html'
urls = []
categories = []
pagination_urls = []
responses = []

# data
mpns = []
skus = []
descriptions = []
manufacturers = []
pdfs = []
data = {}

# scrapped data
scrapped_mpns = []
scrapped_skus = []
scrapped_descriptions = []
scrapped_manufacturers = []
scrapped_pdfs = []


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


async def sub_url_scraper(res):
    try:
        soup = BeautifulSoup(res, 'html.parser')
        container = soup.select_one('div.product-items')
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
        print("Error: Could not find one or more of the required HTML tags.")
        return


async def fetch(session, url):
    try:
        async with session.get(url, headers=headers) as response:
            if 'Manufacturer Part No' in await response.text() or str(response.url) == base_url:
                return await response.text()
            return '404'

    except aiohttp.ClientError as e:
        print(f"Error fetching {url}: {e}")
        return


semaphore = asyncio.Semaphore(50)


async def main(url):
    async with semaphore:
        async with aiohttp.ClientSession() as session:
            tasks = []
            if isinstance(url, str):
                return await fetch(session, url)
            else:
                for url in tqdm(urls, desc="Progress"):
                    # reset page counter for each URL
                    page_counter = 2
                    # scrape the main URL first
                    task = asyncio.create_task(fetch(session, url))
                    tasks.append(task)
                    response = await task
                    await sub_url_scraper(response)
                    # then, scrape paginated URLs
                    while True:
                        url_with_pagination = f"{url}&p={page_counter}"
                        task = asyncio.create_task(fetch(session, url_with_pagination))
                        tasks.append(task)
                        response = await task
                        if response == '404':
                            break  # end pagination
                        page_counter += 1
                        responses.append(response)
            results = await asyncio.gather(*tasks)
            return ''.join(results)


def scrape(res):
    for response in res:
        sub_url_scraper(response)


if __name__ == '__main__':
    start_time = time.time()
    main_page = asyncio.run(main(base_url))
    homepage_scraper(main_page)
    sub_url = asyncio.run(main(urls))
    with ThreadPoolExecutor(max_workers=20) as executor:
        executor.map(scrape, responses)
    frame = pd.DataFrame(fetch_data())
    frame.to_json('data.json', orient='records')
    print(f'{len(mpns)} Items scrapped.')
    print(f'Time elapsed: {float((time.time() - start_time) / 60)} minutes.')
