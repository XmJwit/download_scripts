import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

base_url = 'http://example.com/'  # 替换为实际基础链接
output_dir = './downloads'
max_workers = 16  # 最大线程数

def make_dir(path):
    os.makedirs(path, exist_ok=True)

def download_file(url, local_path):
    try:
        response = requests.get(url)
        response.raise_for_status()
        with open(local_path, 'wb') as file:
            file.write(response.content)
        print(f'Downloaded: {url} to {local_path}')
    except requests.RequestException as e:
        print(f'Failed to download: {url} (error: {e})')

def extract_links(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f'Error fetching {url}: {e}')
        return []
    
    soup = BeautifulSoup(response.text, 'html.parser')
    links = [a['href'] for a in soup.find_all('a', href=True) if not a['href'].startswith('../')]
    return links

def sync_to_remote(local_folder, remote_folder):
    result = os.system(f'rclone copy "{local_folder}" testDrive:/webapp/file/{remote_folder.strip("/")}/')
    if result != 0:
        print(f'Failed to sync folder: {local_folder} to remote storage.')

def download_directory(directory_url, local_base_path):
    sub_links = extract_links(directory_url)
    downloads = []  # 用于存储正在进行的下载任务

    for sub_link in sub_links:
        full_sub_link = urljoin(directory_url, sub_link)

        # 如果是文件夹，则递归下载
        if sub_link.endswith('/'):
            nested_local_path = os.path.join(local_base_path, sub_link.strip('/'))
            make_dir(nested_local_path)
            download_directory(full_sub_link, nested_local_path)

            # 下载完成后立即进行同步
            sync_to_remote(nested_local_path, os.path.relpath(nested_local_path, output_dir))

            # 删除本地文件夹
            shutil.rmtree(nested_local_path)
            print(f'Deleted local folder: {nested_local_path}')
        else:
            local_file_path = os.path.join(local_base_path, sub_link.strip('/'))
            # 使用线程池下载文件
            downloads.append((full_sub_link, local_file_path))

    # 开始多线程下载文件
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(download_file, url, path): url for url, path in downloads}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                future.result()  # 等待下载完成
            except Exception as e:
                print(f'Error downloading {url}: {e}')

def main():
    date_folders = extract_links(base_url)
    
    for date_folder in date_folders:
        date_folder_url = urljoin(base_url, date_folder)
        local_date_folder = os.path.join(output_dir, date_folder.strip('/'))
        make_dir(local_date_folder)
        
        download_directory(date_folder_url, local_date_folder)

if __name__ == '__main__':
    main()
