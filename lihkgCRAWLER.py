import json
import os.path
import time
import random
import signal
import string
import sys
import zipfile

from tqdm import tqdm
from typing import TextIO, Tuple
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver, WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

def random_string() -> str:
    '''
    Generate a random string that is suitable to be an identifier.
    '''

    length = random.randrange(10, 20)
    return ''.join(random.choices(string.ascii_lowercase, k=length))

def make_filename(from_thread: int) -> str:
    '''
    Create the file name using the format `lihkg-{from_thread}.csv`.
    '''

    filename = f'lihkg-{from_thread}.csv'
    print('INFO: Writing to file', filename)
    return filename

def get_next_page_from_json(obj: object, thread_id: int, page: int) -> Tuple[int, int]:
    '''
    Determine the next `thread_id` and `page` from the JSON object.
    If the current value of `page` is equal to `total_page`, we go to the next thread.
    Otherwise we increment the value of `page`.
    '''

    if 'response' not in obj:
        return thread_id + 1, 1

    total_pages = obj['response'].get('total_page', 0)
    if page >= total_pages:
        return thread_id + 1, 1

    return thread_id, page + 1

def get_resume_position(filename: str, from_thread: int) -> Tuple[int, int]:
    if not os.path.exists(filename):
        return from_thread, 1

    line = None
    with open(filename) as f:
        for line in f:
            pass  # locate the last line

    if line is None:
        print(f"WARNING: File {filename} is empty. Starting from thread {from_thread}, page 1.")
        return from_thread, 1

    try:
        thread_id_str, page_str, obj_str = line.rstrip('\n').split('\t')
        thread_id = int(thread_id_str)
        page = int(page_str)
        obj = json.loads(obj_str)  # will throw an exception if the string is not a valid json object

        thread_id_new, page_new = get_next_page_from_json(obj, thread_id, page)
        print('INFO: Resuming from thread', thread_id_new, 'page', page_new)
        return thread_id_new, page_new
    except Exception as e:
        print(f"ERROR: Failed to parse the last line of {filename}: {e}")
        print(f"WARNING: Starting from thread {from_thread}, page 1 due to error.")
        return from_thread, 1

def read_command_line() -> Tuple[int, int]:
    '''
    Read the command line arguments for from_thread and to_thread.
    '''
    from_thread = int(sys.argv[1])
    to_thread = int(sys.argv[2])

    return from_thread, to_thread

def get_json(browser: WebDriver, context: WebElement, url: str) -> object:
    try:
        print(f"INFO: Navigating to API URL: {url}")
        browser.execute_script('arguments[0].href = arguments[1]', context, url)
        browser.execute_script('arguments[0].click()', context)
        browser.switch_to.window(browser.window_handles[1])
        
        print("INFO: Waiting for JSON data...")
        pre = WebDriverWait(browser, timeout=15).until(
            EC.presence_of_element_located((By.TAG_NAME, 'pre'))
        )
        print("INFO: JSON data found")
        
        text = pre.text
        obj = json.loads(text)

        browser.close()
        browser.switch_to.window(browser.window_handles[0])
        print("INFO: API window closed, returning JSON object")
        return obj
    except (TimeoutException, WebDriverException, json.JSONDecodeError) as e:
        print(f"ERROR: Failed to fetch or parse JSON data from {url}: {e}")
        print(f"ERROR: Additional info: {sys.exc_info()}")
        browser.close()
        browser.switch_to.window(browser.window_handles[0])
        return None 

def get_json_of_position(browser: WebDriver, context: WebElement, thread_id: int, page: int) -> object:
    url = f'https://lihkg.com/api_v2/thread/{thread_id}/page/{page}?order=reply_time'
    retry_count = 0
    max_retries = 3
    
    while retry_count < max_retries:
        obj = get_json(browser, context, url)
        if obj is not None:
            print(f"INFO: JSON data fetched from {url} on attempt {retry_count + 1}")
            return obj
        else:
            retry_count += 1
            print(f"WARNING: Retrying to fetch JSON data from {url} (Attempt {retry_count}/{max_retries})...")
    
    print(f"ERROR: Failed to fetch JSON data from {url} after {max_retries} attempts, skipping this page.")
    return None 

def minimize_json(obj: object) -> str:
    '''
    Return the most compact representation of a JSON object.
    '''

    return json.dumps(obj, separators=(',', ':'), ensure_ascii=False)

def write_file(f: TextIO, obj: object, thread_id: int, page: int) -> None:
    '''
    Write a result to the output file in a standard CSV format.
    '''
    json_str = json.dumps(obj, separators=(',', ':'), ensure_ascii=False)

    csv_row = f'{thread_id}\t{page}\t{json_str}\n'

    old_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
    f.write(csv_row)
    signal.signal(signal.SIGINT, old_handler)

def init_lihkg_context(browser: WebDriver) -> WebElement:
    '''
    We need to open a LIHKG page and then jump to the API URL.
    This process is wrapped as the LIHKG context.
    '''
    print("INFO: Loading LIHKG thread page...")
    browser.get('https://lihkg.com/thread/3775474/page/1')
    print("INFO: LIHKG thread page loaded")

    body = WebDriverWait(browser, timeout=10).until(
        EC.presence_of_element_located((By.TAG_NAME, 'body'))
    )
    print("INFO: Body element found")

    element_id = random_string()
    browser.execute_script(f'a = document.createElement("a"); a.id = arguments[1]; a.target = "_blank"; arguments[0].appendChild(a)', body, element_id)
    context = browser.find_element(By.ID, element_id)
    print("INFO: LIHKG context created")
    return context

def start_browser(filename: str, thread_id: int, to_thread: int, page: int, pbar: tqdm):
    options = webdriver.ChromeOptions()
    options.add_argument('start-maximized')
    options.add_argument('disable-blink-features=AutomationControlled')
    options.add_argument('--headless') 
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--log-path=chromedriver.log')

    # CDP
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)

    # ChromeDriverManager自动管理更新ChromeDriver
    print("INFO: Starting Chrome browser with webdriver-manager...")
    service = Service(ChromeDriverManager().install())
    browser = webdriver.Chrome(service=service, options=options)
    print("INFO: Chrome browser started")

    # 执行JavaScript代码来隐藏webdriver特征
    browser.execute_script('Object.defineProperty(navigator, "webdriver", {get: () => undefined})')

    # Initialize LIHKG context
    print("INFO: Initializing LIHKG context...")
    context = init_lihkg_context(browser)
    print("INFO: LIHKG context initialized")

    has_exception = False

    try:
        with open(filename, 'a') as f:
            while thread_id < to_thread:
                obj = get_json_of_position(browser, context, thread_id, page)
                if obj is not None:  # 确保 obj 不是 None
                    write_file(f, obj, thread_id, page)
                    thread_id_new, page = get_next_page_from_json(obj, thread_id, page)
                else:
                    print(f"WARNING: No JSON object returned for thread {thread_id}, page {page}. Skipping to the next page.")
                    thread_id_new, page = thread_id, page + 1  # try next page
            
                sleep_time = random.uniform(4, 8)  # 随机延时 1 到 3 秒
                print(f"INFO: Sleeping for {sleep_time:.2f} seconds before the next request...")
                time.sleep(sleep_time)
                
                # 如果 thread_id 更新，说明该线程已经处理完，更新进度条
                if thread_id_new > thread_id:
                    pbar.update()
                    thread_id = thread_id_new
                
                # 如果线程 ID 没有更新，说明当前线程还有未处理的页面
                if thread_id_new == thread_id and page == 1:
                    # 如果回到第一页，说明该线程已经处理完所有页面，跳出循环
                    print(f"INFO: Finished fetching all pages for thread {thread_id}")
                    break
    except (TimeoutException, WebDriverException) as e:
        has_exception = True
        print(f"ERROR: Exception occurred: {e}")

    browser.quit()

    return has_exception, thread_id, page

def main():
    from_thread = 3775474 
    to_thread = from_thread + 1 
    filename = make_filename(from_thread)
    
    print(f"INFO: Starting to fetch thread {from_thread}")
    
    thread_id = from_thread
    page = 1
    
    pbar = tqdm(total=1, smoothing=0.)
    
    has_exception, thread_id, page = start_browser(filename, thread_id, to_thread, page, pbar)
    
    print(f"INFO: Finished fetching thread {thread_id}")
    
    pbar.close()
    print("INFO: Progress bar closed, script finished")

if __name__ == '__main__':
    main()
