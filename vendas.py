import pandas as pd
import canal_venda as cv
import impostos as imp
import produtos as prod
import extrair_informacoes as ext

dire=ext.ler_toml()['pastas']['dir']

def vendas(url,dt_inicial, dt_fim, status):
  df=pd.DataFrame()
  df_prod=pd.DataFrame(), pd.DataFrame()
  df_vendas=pd.read_parquet(f'{dire}pedidos_venda.parquet')
  df_vendas=df_vendas.query(f'data>="{dt_inicial}" and data<="{dt_fim}"')

  if len(df_vendas)>0:
    df_vendas = cv.canal_venda(df_vendas)
    df_vendas = df_vendas.query(f'Descr_situacao in ({status})')
    #df=imp.incl_imposto(df_vendas)
    df_prod=prod.produtos_vendidos(df_vendas['id'].unique().tolist())
  #df_frete = prod.extrair_produtos_nf(df)[1]
  return df_vendas,df_prod[0],df_prod[1]

def venda_geral(dt_inicial, dt_fim, status):
  df_vendas = pd.read_parquet(f'{dire}pedidos_venda.parquet').query(f'data>="{dt_inicial}" and data<="{dt_fim}" and Descr_situacao in ({status})')
  df_vendas = cv.canal_venda(df_vendas)
  for idx in df_vendas.index:
    M = f'{pd.to_datetime(df_vendas["data"])[idx].month:02}'
    A = pd.to_datetime(df_vendas['data'])[idx].year
    df_vendas.loc[idx,'Ano-Mes']=f'{A}-{M}'
  return df_vendas

