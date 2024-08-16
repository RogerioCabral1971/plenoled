import pandas as pd
import time
import extrair_informacoes as extr
dire=extr.ler_toml()['pastas']['dir']

def situacao(df_vendas):
    df_situacoes_local = pd.read_parquet(f'{dire}situacoes.parquet')
    situacao = "https://bling.com.br/Api/v3/situacoes/"
    id_situacao=[]
    for id in df_vendas['situacao'].index:
      id_situacao.append(df_vendas['situacao'][id]['id'])
    df_vendas.loc[:,'id_situacao']=id_situacao
    desc_situacao=[]
    id_situacao_=df_vendas['id_situacao'].unique()
    for id2 in id_situacao_:
        desc=list(df_situacoes_local.query(f'id_situacao=={id2}')['Descr_situacao'])
        if len(desc)>0:
            desc = list(df_situacoes_local.query(f'id_situacao=={id2}')['Descr_situacao'])[0]
            desc_situacao.append(desc)
        else:
            temp_situacao = extr.extrai(situacao + str(id2))
            try:
                desc=pd.DataFrame([temp_situacao.json()['data']])['nome'][0]
            except:
                time.sleep(1)
                temp_situacao = extr.extrai(situacao + str(id2))
                desc = pd.DataFrame([temp_situacao.json()['data']])['nome'][0]
            desc_situacao.append(desc)
        time.sleep(1)

    df_situacoes=pd.DataFrame(data={'id_situacao':id_situacao_,'Descr_situacao':desc_situacao})
    df_total=pd.merge(df_vendas,df_situacoes, on='id_situacao')
    df_total.drop(columns={'situacao'}, inplace=True)

    return df_total