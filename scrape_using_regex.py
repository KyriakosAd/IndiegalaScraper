import re
import asyncio
from pyppeteer import launch
from pyppeteer_stealth import stealth
import pandas as pd

htmls = []


async def get_main_html(link):
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
                print("Unable to get HTML of main page.")
                print("Exiting...")
                exit(1)
            html = await page.content()
            await browser.close()
            return html
    if attempt1 == 4:
        print("Unable to get HTML of main page.")
        print("Exiting...")
        exit(1)


async def get_htmls(browser, queue):
    link = await queue.get()
    link = "https://www.indiegala.com" + link
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
                if await page.querySelector('#main-iframe') is not None:
                    print("Captcha bypass failed! Page:", link)
                    print("Reloading...")
                    await page.close()
                    page = await browser.newPage()
                    await stealth(page)
                    await page.goto(link, {'timeout': 0})
                else:
                    break
            if attempt2 == 4:
                print("Unable to get HTML from", link)
                queue.task_done()
                return
            html = await page.content()
            htmls.append(html)
            await page.close()
            queue.task_done()
            return
    if attempt1 == 4:
        print("Unable to get HTML from", link)
        queue.task_done()
        return


async def get_data():
    dataset = []
    for x in range(len(htmls)):
        s_title = re.search(r"(?<=page-title\"><span>)[^<]*", htmls[x])
        title = s_title.group() if s_title is not None else None

        s_base_price = re.search(r"(?<=base-price\">)[^<]*", htmls[x])
        base_price = float(s_base_price.group()[:-1]) if s_base_price is not None else None

        s_final_price = re.search(r"(?<=final-price right\">)[^<]*", htmls[x])
        final_price = float(s_final_price.group()[:-1]) if s_final_price is not None else None

        s_date = re.search(r"(?<=Released</div><div class=\"info-cont\">)[^<]*", htmls[x])
        date = s_date.group() if s_date is not None else None

        s_category = re.search(r"(?<=<a href=\"/store/category/).*?</a>(?=</span></div>)", htmls[x])
        category = " ".join(re.findall(r"(?<=\">)[^<]*", s_category.group())) if s_category is not None else None

        s_developer = re.search(r"(?<=Developer</div><div class=\"info-cont\">)[^<]*", htmls[x])
        developer = s_developer.group() if s_developer is not None else None

        dataset.append([title, base_price, final_price, date, category, developer])
    return dataset


async def main():
    print("Getting main HTML...")
    for attempt in range(5):
        mainHTML = await get_main_html("https://www.indiegala.com/games/on-sale/percentage-off")
        search_games = re.search(r"<div class=\"main-list-items-cont \">.*<div class=\"clear\">", mainHTML)
        if search_games is not None:
            links = re.findall(r"/store/game/[^\"]+", search_games.group())
            break
        else:
            print("List of games unavailable!")
            print("Reloading...")
    if attempt == 4:
        print("Unable to get list of games.")
        print("Exiting...")
        exit(1)

    print("Getting HTMLs of games...")
    browser = await launch(headless=True, autoClose=False)

    queue = asyncio.Queue()
    tasks = []
    for _ in range(len(links)):
        asyncio.create_task(get_htmls(browser, queue))

    for i in range(len(links)):
        await queue.put(links[i])

    await queue.join()
    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks)

    print("Scraping...")
    dataset = await get_data()

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
