import csv
import os
import time
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

ARTIFACTS_FOLDER = "artifacts"

DIR = os.path.dirname(os.path.abspath(__file__))
FOLDER = os.path.join(DIR, ARTIFACTS_FOLDER)

channels = [{
    "id": "28hse",
    "url": "https://www.28hse.com/rent",
}]

def main():
    # Launch undetected Chrome browser
    options = uc.ChromeOptions()
    # Browser will be visible by default
    driver = uc.Chrome(options=options, version_main=None, use_subprocess=False)
    
    try:
        # Go to the property page
        driver.get("https://www.28hse.com/rent/apartment/property-3488909")
        
        # Wait for page to load and click the first phone element
        wait = WebDriverWait(driver, 10)
        phone_element = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '[attr="phone"]')))
        phone_element.click()
        
        # Wait after cloudflare protection
        time.sleep(5)
        
        # Find all contact divs
        contacts = driver.find_elements(By.CLASS_NAME, 'contactsDiv')
        for contact in contacts:
            phone = contact.find_element(By.CSS_SELECTOR, '[attr="phone"]')
            wtsapp = contact.find_element(By.CSS_SELECTOR, '[attr="whatsapp"]')
            phone_link = phone.get_attribute('href')
            wtsapp_link = wtsapp.get_attribute('href')
            print(f"Phone: {phone_link}, WhatsApp: {wtsapp_link}")
        
        # Prompt user to log in
        print("Please log in to Instagram in the browser.")
        input("Press Enter after you have logged in...")  # Wait for user input
        
        # Fetch the HTML body after user signals they are logged in
        # html_body = driver.page_source
        
        # Print the HTML body
        # print(html_body)
        
    finally:
        # Close the browser
        driver.quit()

# Run the main function
if __name__ == "__main__":
    main()