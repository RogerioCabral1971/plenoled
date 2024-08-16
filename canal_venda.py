import pandas as pd
import extrair_informacoes as ext
import produtos as prod
import streamlit as st
import abrirArq

dire=ext.ler_toml()['pastas']['dir']


def canal_venda(df):
    if 'canais_venda' not in st.session_state:
        st.session_state['canais_venda']=abrirArq.parquet('canais_venda')
    #canal_venda_local=pd.read_parquet(f'{dire}canais_venda.parquet')
    #canalvenda = "https://bling.com.br/Api/v3/canais-venda"
    #response_canais = requests.request("GET", canalvenda, headers=headers, data=payload)
    #df_canais = pd.DataFrame(response_canais.json()['data'])
    #df_canais=canal_venda_local
    id_canal=[]
    origem_venda=[]

    for id in df['id_canal_venda'].index:
      id_canal.append(list(df['id_canal_venda'])[0])
      try:
        origem_venda.append(list(st.session_state['canais_venda'][st.session_state['canais_venda']['id_canal_venda']==df['id_canal_venda'][id]]['descricao'])[0])
      except:
          origem_venda.append('Comercial')
    df.loc[:,'canal']=id_canal
    df.loc[:, 'origem_venda']=origem_venda
    return df

def eee(df_nf):
    plan=prod.plan_produtos()
    for idx in df_nf.index:
        x = plan[plan['CÓDIGO SKU'] == str(list(df_nf['itens'][idx])[0]['codigo']).upper()]
        if len(x) > 0:
            custo = pd.concat([custo, x])
        else:
            nachou = pd.concat([nachou, pd.DataFrame(df_nf['itens'][idx])])

