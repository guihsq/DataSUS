import urllib.request
import sys
sys.path.insert(0, "../../lib")
import datetools

from tqdm import tqdm
from multiprocessing import Pool

def prepare_download(uf, ano, mes):
    url= f"ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Dados/RD{uf}{ano}{mes}.dbc"

    file_path = f"/dbfs/mnt/datalake/DataSUS/rd/dbc/RD{uf}{ano}{mes}.dbc"

    return url, file_path

def get_data_from_datasus(uf, ano, mes):

    url, file_path = prepare_download(uf, ano, mes);
    resp = urllib.request.urlretrieve(url, file_path)

def get_data_of_multiple_dates_by_all_ufs(uf, dates):
    for i in tqdm(dates):
        ano, mes, dia  = i.split("-")
        ano = ano[-2:]
        get_data_from_datasus(uf, ano, mes)

def prepare_parameters_dates_by_all_ufs(start_date, end_date):
    
    ufs = ["RO", "AC", "AM", "RR",
      "PA", "AP", "TO", "MA",
      "PI", "CE", "RN", "PB",
      "PE", "AL", "SE", "BA",
      "MG", "ES", "RJ", "SP",
      "PR", "SC", "RS", "MS",
      "MT", "GO", "DF"]
    
    dates = datetools.date_range(start_date, end_date, monthly=True)
    
    return [(uf, dates) for uf in ufs]

ufs_and_dates = prepare_parameters_dates_by_all_ufs("2022-01-01", "2023-05-01")

with Pool(8) as pool:
    pool.starmap(get_data_of_multiple_dates_by_all_ufs, ufs_and_dates)