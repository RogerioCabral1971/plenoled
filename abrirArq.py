
import pandas as pd
import extrair_informacoes as ext

dire=ext.ler_toml()['pastas']['dir']

def parquet(arq):
    df=pd.read_parquet(f'{dire}{arq}.parquet')
    return df

def excel(arq):
    df=pd.read_excel(f'{dire}\{arq}.xlsx')
    return df

