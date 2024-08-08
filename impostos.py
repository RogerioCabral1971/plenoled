import pandas as pd
import streamlit as st
import nf as nfe
import extrair_informacoes as ext

dir=ext.ler_toml()['pastas']['dir']

def extr_imposto(df):
  my_bar = st.progress(0, text="progress_text")
  cont=0
  for idx in df.index:
    cont=cont+1
    my_bar.progress(cont, text=f'Imposto lindo...: {idx} de {len(df)}')
    try:
      id=df['id'][idx]
      valor_imposto=nfe.arqXml_valor_importo(id)
      df.loc[df['id']==id,"Valor Imposto"]=float(valor_imposto)
    except:
      pass
  my_bar.empty()
  return df

def incl_imposto(df):
  df_imp=pd.read_parquet(f'{dir}impostos.parquet')
  df_imp.query(f'id in ({list(df["id"])})')
  df=df.merge(df_imp, left_on='id', right_on='id')
  return df

