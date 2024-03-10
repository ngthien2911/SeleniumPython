import json
from hashlib import md5
import requests
from typing import Any
import time
# from selenium import webdriver
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chromium.options import ChromiumOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions
import os
from dotenv import load_dotenv

load_dotenv()

MLX_URL = 'https://api.multilogin.com/'
LAUNCHER_URL = 'https://launcher.mlx.yt:45001/api/v1'
HEADERS = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
}
EMAIL =os.getenv('EMAIL')  # Change USERNAME to your MLX account email
PASSWORD = os.getenv('PASSWORD') # Change PASSWORD to your MLX account password (no need to convert md5 hash)
profileId=''
folderId=''

def get_token(username, password):
    url = f'{MLX_URL}user/signin'
    headers = HEADERS
    body = {
        'email': username,
        'password': str(md5(password.encode()).hexdigest()),
    }
    resp = requests.post(url, json=body, headers=headers)
    resp_json = resp.json()
    http_code = resp_json.get('status').get('http_code')
    if http_code != 200:
        raise RuntimeError(f"Couldn't get token: HTTP {http_code}: {resp_json['status']['message']}")
    token = resp_json.get('data').get('token')
    print('Got MLX token: ' + token)
    return token


TOKEN = get_token(EMAIL, PASSWORD)
HEADERS['Authorization'] = 'Bearer ' + TOKEN

def run_profile(profile_id: str, folder_id: str) -> WebDriver:
    # profile_id, folder_id = create_profile()

    print(f'Starting profile {profile_id} in folder {folder_id}')
    url = f'{LAUNCHER_URL}/profile/f/{folder_id}/p/{profile_id}/start?automation_type=selenium&headless_mode=false'

    resp = requests.get(url, headers=HEADERS)
    resp_json = resp.json()
    print(resp)
    print(resp_json)

    # Instantiate the Remote Web Driver to connect to the browser profile launched by previous GET request
    selenium_port = resp_json['status']['message']
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--incognito")
    driver = webdriver.Remote(command_executor=f'http://127.0.0.1:{selenium_port}',options=chrome_options)
    return driver



def stop_profile(profile_id) -> None:
    r = requests.get(f'{LAUNCHER_URL}/profile/stop/p/{profile_id}', headers=HEADERS)

    if(r.status_code != 200):
        print(f'\nError while stopping profile: {r.text}\n')
    else:
        print(f'\nProfile {profile_id} stopped.\n')



def automation():
    driver = run_profile(profileId,folderId )
    driver.get("https://www.tiktok.com/@vantoan___/video/7294298719665622305?is_from_webapp=1&sender_device=pc") #the URL of the video/reels
    time.sleep(5)
    driver.find_element(By.XPATH,'//*[@id="loginContainer"]/div/div/div/div[2]').click()
    driver.find_element(By.XPATH,'//*[@id="loginContainer"]/div[2]/form/div[1]/input').send_keys('ngletai2911@gmail.com')
    driver.find_element(By.XPATH,'//*[@id="loginContainer"]/div[2]/form/div[2]/div/input').send_keys('Benice123!')
    driver.find_element(By.XPATH,'//*[@id="loginContainer"]/div[2]/form/button').click()
    time.sleep(10)
    driver.find_element(By.XPATH,'//*[@id="main-content-video_detail"]/div/div[2]/div[1]/div[1]/div[1]/div[5]/div[2]/div[1]/div[1]').click()
    time.sleep(3) # youtube count view after at least 30s so we should leave this as 30s
    # driver.find_element(By.XPATH,'//*[@id="player"]').click()


    driver.quit()
    print('Loop is done')
    stop_profile(profileId) 

if __name__ == '__main__':
    automation()






