import pandas as pd
import canal_venda as cv
import impostos as imp
import produtos as prod
import extrair_informacoes as ext

dire=ext.ler_toml()['pastas']['dir']

def vendas(url,dt_inicial, dt_fim, status):
  df=pd.DataFrame()
  df_prod=pd.DataFrame(), pd.DataFrame()
  #df_vendas=pd.read_parquet(f'{dire}pedidos_venda.parquet')
  #df_vendas=df_vendas.query(f'data>="{dt_inicial}" and data<="{dt_fim}"')
  faturas = pd.read_parquet(f'{dire}faturas.parquet')
  lista = list(faturas.query(f'data>="{dt_inicial}" and data<="{dt_fim}"')['id'])

  if len(lista)>0:
    #df_vendas = cv.canal_venda(df_vendas)
    #df_vendas = df_vendas.query(f'Descr_situacao in ({status})')
    #df=imp.incl_imposto(df_vendas)
    df_prod=prod.produtos_vendidos(lista)
  #df_frete = prod.extrair_produtos_nf(df)[1]
  return df_prod[0],df_prod[1]

def venda_geral(dt_inicial, dt_fim, status):
  faturas = pd.read_parquet(f'{dire}faturas.parquet')
  lista = list(faturas.query(f'data>="{dt_inicial}" and data<="{dt_fim}"')['id'])
  #df_vendas = pd.read_parquet(f'{dire}pedidos_venda.parquet').query(f'data>="{dt_inicial}" and data<="{dt_fim}" and Descr_situacao in ({status})')
  df_vendas = pd.read_parquet(f'{dire}notas_fiscais.parquet').query(f'Emitida in {lista}')
  df_vendas['id_canal_venda'] = pd.DataFrame(list(df_vendas['loja']))
  df_vendas = df_vendas[['numero', 'data', 'dataSaida', 'dataPrevista', 'total', 'id_canal_venda']].drop_duplicates()
  df_vendas = df_vendas.reset_index()
  df_vendas = cv.canal_venda(df_vendas)
  for idx in df_vendas.index:
    M = f'{pd.to_datetime(df_vendas["data"])[idx].month:02}'
    A = pd.to_datetime(df_vendas['data'])[idx].year
    df_vendas.loc[idx,'Ano-Mes']=f'{A}-{M}'
  return df_vendas

