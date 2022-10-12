import asyncio
from pyppeteer import launch
from pyppeteer_stealth import stealth
import pandas as pd

dataset = []


async def get_games(link):
    for attempt1 in range(5):
        try:
            browser = await launch(headless=True, autoClose=False)
            page = await browser.newPage()
            await stealth(page)
            await page.goto(link, {'timeout': 0})
        except Exception as e:
            print(e)
            print("Reloading...")
        else:
            for attempt2 in range(5):
                await page.waitForSelector('.main-list-ajax-preloading', {'hidden': 'true'})
                if await page.querySelector('#main-iframe') is not None:
                    print("Captcha bypass failed!")
                    print("Reloading...")
                    await page.reload()
                elif await page.querySelector('.ajax-error') is not None:
                    print("Ajax error!")
                    print("Reloading...")
                    await page.reload()
                else:
                    break
            if attempt2 == 4:
                print("Unable to get games from main page.")
                print("Exiting...")
                exit(1)

            results = await page.querySelectorAll('a.main-list-item-clicker')
            links = []
            titles = []
            for a in results:
                links.append(await page.evaluate('(a) => a.href', a))
                titles.append(await page.evaluate('(a) => a.title', a))

            await browser.close()
            return links, titles
    if attempt1 == 4:
        print("Unable to get games from main page.")
        print("Exiting...")
        exit(1)


async def get_data(browser, queue):
    link, title = await queue.get()
    for attempt1 in range(5):
        try:
            page = await browser.newPage()
            await stealth(page)
            await page.goto(link, {'timeout': 0})
        except Exception as e:
            print(e)
            print("Reloading...")
        else:
            for attempt2 in range(5):
                if await page.querySelector('#main-iframe') is None:
                    break
                print("Captcha bypass failed! Page:", link)
                print("Reloading...")
                await page.close()
                page = await browser.newPage()
                await stealth(page)
                await page.goto(link, {'timeout': 0})
            if attempt2 == 4:
                print("Unable to get data from", link)
                queue.task_done()
                return

            base_price = None
            x = await page.querySelector('.base-price')
            if x is not None:
                base_price = float((await page.evaluate('(x) => x.innerText', x))[:-1])

            final_price = None
            y = await page.querySelector('.final-price')
            if y is not None:
                final_price = float((await page.evaluate('(y) => y.innerText', y))[:-1])

            results = await page.querySelectorAll('div.info-title')
            date = None
            category = None
            developer = None
            for a in results:
                search = await page.evaluate("(a) => a.innerText", a)
                if search == "Released":
                    date = await page.evaluate("(a) => a.nextSibling.innerText", a)
                elif search == "Categories":
                    category = await page.evaluate("(a) => a.nextSibling.nextSibling.innerText", a)
                elif search == "Developer":
                    developer = await page.evaluate("(a) => a.nextSibling.innerText", a)

            await page.close()
            dataset.append([title, base_price, final_price, date, category, developer])
            queue.task_done()
            return
    if attempt1 == 4:
        print("Unable to get data from", link)
        queue.task_done()
        return


async def main():
    print("Getting links of games...")
    links, titles = await get_games("https://www.indiegala.com/games/on-sale/best-savings")

    print("Scraping...")
    browser = await launch(headless=True, autoClose=False)

    queue = asyncio.Queue()
    tasks = []
    for _ in range(len(links)):
        asyncio.create_task(get_data(browser, queue))

    for i in range(len(links)):
        await queue.put((links[i], titles[i]))

    await queue.join()
    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks)

    df = pd.DataFrame(
        columns=['Title', 'Base Price', 'Final Price', 'Release Date', 'Categories', 'Developer'], data=dataset
    ).set_index('Title')

    print(df)

    for column in df:
        print("\nColumn:", column)
        print("Percentage of null values:", round(df[column].isna().sum() * 100 / df[column].size, 2), "%")

    df.to_csv('dataframe.csv', encoding='utf-8')
    print("\nSaved DataFrame to dataframe.csv")

asyncio.run(main())
