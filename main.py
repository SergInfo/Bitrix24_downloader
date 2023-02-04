from datetime import datetime
import os
from fast_bitrix24 import Bitrix
from pywget import wget


class MyLog:
    """
    Создание файла логирования и запись в него
    """

    def __init__(self, log_file_name, mode='a'):
        self.__file = log_file_name
        if not os.path.exists(log_file_name):
            mode = 'w'
            with open(log_file_name, mode=mode) as file:
                pass

    def write(self, string_to_log):
        now = datetime.now()
        with open(self.__file, mode='a') as file:
            file.write(f'{now.strftime("%d.%m.%Y %H:%M:%S")} {string_to_log}' + '\n')


def read_ini() -> dict:
    """
    Чтение файла настроек bitrix24_downloader.ini
    :return: dict
    """
    ini_param = dict()
    try:
        with open('bitrix24_downloader.ini', 'r', encoding='utf-8') as ini_file:
            for line in ini_file:
                if not line.lstrip().startswith('#'):
                    lst_param = line.split('=')
                    if len(lst_param) == 2:
                        ini_param[lst_param[0].strip()] = lst_param[1].strip()
    except:
        print('Error reading settings from file: bitrix24_downloader.ini')
        quit(0)
    return ini_param


def find_vats_dir(b24):
    """
    Возвращает
    Сначала ищем служебный диск Битрикс24, обычно он называется всегда Общий диск
    Далее ищем служебную папку куда интеграция ВАТС Мегафон складывает файлы
    Служебный код папки = VI_CALLS
    :param b24: 'Bitrix'
    :return:
    """
    try:
        disks = b24.get_all('disk.storage.getlist', params={'filter': {'NAME': 'Общий диск'}})
        shared_disk = disks[0]
        log.write(f'Shared disk found: {shared_disk["NAME"]}')
        disk_id = list()
        disk_id.append(shared_disk['ID'])
        folders = b24.get_by_ID('disk.storage.getchildren', disk_id, ID_field_name='id',
                                params={'filter': {'CODE': 'VI_CALLS'}})
        voip_folder = folders[0]
        log.write(f'Found folder with records VATS Megafon: {voip_folder["NAME"]}')
        root_folder_id = list()
        root_folder_id.append(voip_folder['ID'])
        return root_folder_id
    except Exception as exc:
        log.write('Not found folder with records VATS Megafon')
        raise IndexError("Not found folder with records VATS Megafon")


def get_all_folders(b24, root_folder_records):
    folder_id = '0'
    while True:
        tmp_folders = b24.get_by_ID('disk.folder.getchildren', root_folder_records, ID_field_name='id',
                                    params={'order': {'ID': 'ASC'},
                                            'filter': {'>ID': folder_id},
                                            'start': -1})
        folder_dict = {folder['NAME']: folder['ID'] for folder in tmp_folders}
        if len(tmp_folders) < 50:
            break
        folder_id = tmp_folders[-1]['ID']

    folder_dict = dict(sorted(folder_dict.items(), key=lambda elem: elem[0], reverse=False))
    return folder_dict


def download_records(b24, ini_param, folder_id, catalog_name):
    root_folder_id = list()
    root_folder_id.append(folder_id)

    count_files = 0
    file_id = '0'
    download_path = os.path.join(ini_param['download path'], catalog_name)
    download_path = os.path.abspath(download_path)
    if not os.path.exists(download_path):
        os.makedirs(download_path)

    while True:
        month_files = b24.get_by_ID('disk.folder.getchildren', root_folder_id, ID_field_name='id',
                                    params={'order': {'ID': 'ASC'},
                                            'filter': {'>ID': file_id},
                                            'start': -1})
        for file_b24 in month_files:
            file_name = os.path.join(download_path, file_b24['NAME'])
            if not os.path.exists(file_name):
                try:
                    wget.download(file_b24['DOWNLOAD_URL'], file_name)
                except Exception as exc:
                    log.write(f'Error download file {file_name}')
        count_files += len(month_files)
        if len(month_files) < 50:
            break
        file_id = month_files[-1]['ID']
    log.write(f'Total download files from {catalog_name}: {count_files}')


log = MyLog('bitrix24_downloader.log')
ini_param = read_ini()
# замените на ваш вебхук для доступа к Bitrix24
webhook = ini_param['webhook']
b24 = Bitrix(webhook)
root_folder_records = find_vats_dir(b24)
folders_with_records = get_all_folders(b24, root_folder_records)
today = datetime.now()
days_left = int(ini_param['days left'])
for key, id in folders_with_records.items():
    date_catalog = datetime(int(key[:4]), int(key[-2:]), 1)
    delta_data = today - date_catalog
    if delta_data.days > days_left:
        log.write(f'starting download catalog: {key}')
        download_records(b24, ini_param, id, key)

log.write(f'ending download catalog')
