import os
import shutil
import zipfile

import boto3
import requests
from bs4 import BeautifulSoup

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'zh-CN',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
}


def get_index_page(url='https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data'):
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        raise Exception('请求失败')
    soup = BeautifulSoup(resp.text, 'html.parser')
    result = []
    for container in soup.find_all('div', {'class': 'hdtd_downloadlink'}):
        name = container.find('div', {'class': 'hdtd_downloadlink_label'}).get_text(strip=True)
        url = container.find('a').get('href')
        year = name.split(' ')[0]
        if year not in ('2019', '2020', '2021', '2022', '2023'):
            continue
        if year == '2023' and name.split(' ')[-1] == 'Q4':
            continue
        result.append(url)
    return sorted(list(set(result)))


def download_file(url: str):
    print(f'开始下载: {url}')
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        raise Exception(f'下载失败: {url}')
    with open(url.split('/')[-1], 'wb') as f:
        f.write(resp.content)
    print(f'下载完成: {url}')
    with zipfile.ZipFile(url.split('/')[-1], 'r') as zip_ref:
        zip_ref.extractall("/Users/yulong.cai/data_develop/airflow/data/" + url.split('/')[-1].split('.')[0] + "/")
    shutil.rmtree("/Users/yulong.cai/data_develop/airflow/data/" + url.split('/')[-1].split('.')[0] + "/__MACOSX",
                  ignore_errors=True)
    os.remove(url.split('/')[-1])


def upload():
    s3 = boto3.client('s3')

    local_file_path = 'data'
    s3_file_path = 'data/'
    s3.upload_file(local_file_path, 'ylc-data-test', s3_file_path)
    print(f"File uploaded to {s3_file_path}")


def loadData():
    print(get_index_page())
    for u in get_index_page():
        download_file(u)
        break
