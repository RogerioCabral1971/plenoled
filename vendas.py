import pandas as pd
import abrirArq
import produtos as prod
import extrair_informacoes as ext
import streamlit as st
import atualizar_bases

dire=ext.ler_toml()['pastas']['dir']

def vendas(url,dt_inicial, dt_fim, status):
  df=pd.DataFrame()
  df_prod=pd.DataFrame(), pd.DataFrame()
  #df_vendas=pd.read_parquet(f'{dire}pedidos_venda.parquet')
  #df_vendas=df_vendas.query(f'data>="{dt_inicial}" and data<="{dt_fim}"')
  if 'faturas' not in st.session_state:
    st.session_state['faturas']=abrirArq.parquet('faturas')
  #faturas = pd.read_parquet(f'{dire}faturas.parquet')
  faturas=st.session_state['faturas']
  if len(faturas)==0:
    atualizar_bases.fatura(atualizar_bases.dataBase, dt_fim)
    st.session_state['faturas'] = abrirArq.parquet('faturas')
    faturas = st.session_state['faturas']
  lista = list(faturas.query(f'data>="{dt_inicial}" and data<="{dt_fim}"')['id'])
  if len(lista)>0:
    #df_vendas = cv.canal_venda(df_vendas)
    #df_vendas = df_vendas.query(f'Descr_situacao in ({status})')
    #df=imp.incl_imposto(df_vendas)
    df_prod=prod.produtos_vendidos(lista)
  #df_frete = prod.extrair_produtos_nf(df)[1]
  return df_prod[0],df_prod[1]

def venda_geral(dt_inicial, dt_fim, status):
  if 'canais_venda' not in st.session_state:
    st.session_state['canais_venda']=abrirArq.parquet('canais_venda')
  #canal_venda_local = pd.read_parquet(f'{dire}canais_venda.parquet')
  canal_venda_local =st.session_state['canais_venda']
  if 'faturas' not in st.session_state:
    st.session_state['faturas']=abrirArq.parquet('faturas')
  #faturas = pd.read_parquet(f'{dire}faturas.parquet')
  faturas=st.session_state['faturas'].query(f'data>="{dt_inicial}" and data<="{dt_fim}"')
  lista = list(faturas['id'])
  #df_vendas = pd.read_parquet(f'{dire}pedidos_venda.parquet').query(f'data>="{dt_inicial}" and data<="{dt_fim}" and Descr_situacao in ({status})')
  if 'notas_fiscais' not in st.session_state:
    st.session_state['notas_fiscais']=abrirArq.parquet('notas_fiscais')
  #df_vendas = pd.read_parquet(f'{dire}notas_fiscais.parquet').query(f'Emitida in {lista}')
  df_vendas = st.session_state['notas_fiscais'].query(f'Emitida in {lista}')
  df_vendas.reset_index(inplace=True)
  df_vendas['id_canal_venda'] = pd.DataFrame(list(df_vendas['loja']))
  df_vendas = df_vendas[['numero', 'data', 'dataSaida', 'dataPrevista', 'total', 'id_canal_venda']].drop_duplicates()
  df_vendas.fillna({'id_canal_venda':0}, inplace=True)
  df_vendas['id_canal_venda'] = df_vendas['id_canal_venda'].astype('int64')
  df_vendas = df_vendas.reset_index()
  #df_vendas = cv.canal_venda(df_vendas)
  df_vendas = df_vendas.merge(canal_venda_local[['id_canal_venda', 'descricao']], on='id_canal_venda')
  df_vendas.rename(columns={'descricao':'origem_venda'}, inplace=True)
  for idx in df_vendas.index:
    M = f'{pd.to_datetime(df_vendas["data"])[idx].month:02}'
    A = pd.to_datetime(df_vendas['data'])[idx].year
    df_vendas.loc[idx,'Ano-Mes']=f'{A}-{M}'

  return df_vendas

