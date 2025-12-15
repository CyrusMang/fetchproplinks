import asyncio
import csv
import os
from playwright.async_api import async_playwright

channels = [{
    "id": "28hse",
    "url": "https://www.28hse.com/rent",
},]

async def main():
    async with async_playwright() as p:
        # Launch a browser in non-headless mode
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()

        # Open a new page
        page = await context.new_page()

        # Go to Instagram login page
        await page.goto("https://www.28hse.com/rent")

        menu = page.locator('#mainMenuDiv')
        button = menu.locator('[data-value="hk"]')
        await button.click()

        file_path = os.path.join(FOLDER, f"28hse_links.csv")

        content = page.locator('#main_content')

        async def fetch_link():
            with open(file_path, "a") as of:
                writer = csv.writer(of)
                search_results_divs = await content.locator('.property_item').all()
                for div in search_results_divs:
                    detail_page_link = div.locator('a.detail_page').first
                    link = await detail_page_link.get_attribute('href')
                    writer.writerow([link])
        
        async def go_next_page(num):
            pagination = content.locator('.pagination')
            p = pagination.locator('[attr1="{}"]'.format(num))
            if await p.is_visible():
                await p.click()
                return True
            return False
        
        init_page = 1
        while True:
            await fetch_link()
            await asyncio.sleep(3)
            init_page += 1
            has_next = await go_next_page(init_page)
            if not has_next:
                break
        
        print(links)


        # Prompt user to log in
        print("Please log in to Instagram in the browser.")
        input("Press Enter after you have logged in...")  # Wait for user input

        # Fetch the HTML body after user signals they are logged in
        html_body = await page.content()
        
        # Print the HTML body
        # print(html_body)

        # Close the browser
        await browser.close()

# Run the main function
asyncio.run(main())