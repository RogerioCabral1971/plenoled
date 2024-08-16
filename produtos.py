import locale
import os.path
import time
import extrair_informacoes as extr
import pandas as pd
import streamlit as st
import abrirArq
import logging
import atualizar_bases
logger = logging.getLogger('ftpuploader')
dire=extr.ler_toml()['pastas']['dir']


def plan_produtos():
    plan=pd.DataFrame()
    cont=0
    total_plan=5
    colunas=['CÓDIGO SKU','DESCRIÇÃO DO PRODUTO','PREÇO DE CUSTO','CUSTO DE INSUMOS','MARGEM BRUTA','IMPOSTO','TAXA CARTÃO','FRETE','PREÇO SUGERIDO IDEAL','PREÇO SITE','MARGEM %','CUSTO DE ALÇAS']
    coluna_letra='B:L'
    while cont <= total_plan:
        plan_produto = pd.read_excel(f'{dire}Precificação Pleno Led.xlsx', sheet_name=cont, usecols=coluna_letra)
        if cont<5:
            plan_produto['CUSTO DE ALÇAS']=0
        if cont >0:
            plan=pd.concat([plan[colunas], plan_produto[colunas]])
        else:
            plan=plan_produto
        if cont==4:
            coluna_letra='B:M'
        cont=cont+1
    plan = plan[plan['DESCRIÇÃO DO PRODUTO'].isnull() == False]
    plan = plan.reset_index()
    plan.drop(columns={'index'}, inplace=True)
    for idx in plan.index:
        plan.loc[idx, 'Tipo Mercadoria'] = plan['DESCRIÇÃO DO PRODUTO'].str.split(' ')[idx][0]
        plan.loc[idx, 'DescrReduzida'] = str(plan['DESCRIÇÃO DO PRODUTO'][idx]).upper()[0:30]
    plan=plan.rename(columns={'CÓDIGO SKU':'codigo'})
        #plan.loc[idx, 'Potencia']=[ s for s in plan['DESCRIÇÃO DO PRODUTO'].str.upper().str.split(' ')[idx] if 'MM' in s ][0]
    return plan

def produtos_vendidos(id):
    if 'canais_venda' not in st.session_state:
        st.session_state['canais_venda']=abrirArq.parquet('canais_venda')
    #canal_venda_local = pd.read_parquet(f'{dire}canais_venda.parquet')
    canal_venda_local =st.session_state['canais_venda']
    df_itens = pd.DataFrame()
    frete = []
    id_merc=[]
    if 'notas_fiscais' not in st.session_state:
        st.session_state['notas_fiscais']=abrirArq.parquet('notas_fiscais')
    #df_nf = pd.read_parquet(f'{dire}notas_fiscais.parquet')
    df_nf=st.session_state['notas_fiscais']
    #df_nf=df_nf.drop(columns={'index', 'level_0'})
    df_nf=df_nf.query(f'Emitida in {id}')
    df_nf=df_nf.reset_index()
    for idx in df_nf.index:
        df = pd.DataFrame(list(df_nf['itens'][idx]))
        df['canal_origem']=str(df_nf['loja'][idx]['id'])
        df['id_merc'] = int(df_nf['itens'][idx][0]['produto']['id'])
        df['Valor_Frete'] = float(df_nf['transporte'][idx]['frete'])
        df['UF'] = df_nf['transporte'][idx]['etiqueta']['uf']
        df_itens = pd.concat([df_itens, df])
        frete.append(df_nf['transporte'][idx]['frete'])
        id_merc.append(df_nf['id'][idx])
    df_frete = pd.DataFrame(data={'id_merc': id_merc,'Valor_Frete':frete})
    df_itens = df_itens.rename(columns={'quantidade': 'Quantidade', 'descricao':'Descrição Mercadoria'})
    try:
        df_itens['canal_origem']=df_itens['canal_origem'].astype('int')
    except:
        if os.path.isfile(f'{dire}\pedidos_venda.parquet'):
            os.remove(f'{dire}pedidos_venda.parquet')
            st.rerun

    df_itens = df_itens.merge(canal_venda_local[['id_canal_venda','descricao' ]],left_on='canal_origem', right_on='id_canal_venda' )
    df_itens=df_itens.rename(columns={'descricao':'Canal de Venda'})
    df_itens['R$ Total']=df_itens['Quantidade']*df_itens['valor']
    colunas=['id', 'Canal de Venda','codigo', 'id_merc', 'Descrição Mercadoria', 'Quantidade', 'valor', 'R$ Total', 'Total Custo', 'PREÇO DE CUSTO', 'IMPOSTO', 'Valor_Frete', 'UF', 'peso', 'TipoEnvio', 'CustoEnvio']
    if 'custo' not in st.session_state:
        st.session_state['custo']=abrirArq.parquet('custo')
    #custo=pd.read_parquet(f'{dire}custo.parquet')
    custo=st.session_state['custo']
    if len(custo)==0:
        atualizar_bases.extrair_custos()
        st.session_state['custo'] = abrirArq.parquet('custo')
        custo = st.session_state['custo']
    custo.rename(columns={'precoCusto':'PREÇO DE CUSTO'}, inplace=True)
    df_itens['IMPOSTO'] = df_itens['R$ Total'].map(lambda x: x*0.08)
    df_itens=df_itens.merge(custo, how='left', left_on='id_merc', right_on='produto')
    #df_itens = df_itens.fillna(value=0)
    df_itens['Total Custo'] = (df_itens['PREÇO DE CUSTO']*df_itens['Quantidade']) #+(df_itens['IMPOSTO'])
    df_itens=peso(df_itens)
    try:
        return df_itens[colunas].drop_duplicates().sort_values(by='R$ Total',ascending=False), df_frete
    except Exception as e:
        st.markdown(":red[Erro em: ]" + (str(e).replace('not in index','').replace("['",'').replace("']",'').strip()))

def peso(df):
    if 'EstadosRegioes' not in st.session_state:
        st.session_state['EstadosRegioes']=abrirArq.excel('EstadosRegioes')
    #EstReg = pd.read_excel(f'{dire}\EstadosRegioes.xlsx')
    EstReg=st.session_state['EstadosRegioes']
    if 'custoEnvio' not in st.session_state:
        st.session_state['custoEnvio']=abrirArq.excel('custoEnvio')
    #custoEnvio = pd.read_excel(f'{dire}\custoEnvio.xlsx')
    custoEnvio = st.session_state['custoEnvio']
    if os.path.isfile(f'{dire}\pesos.parquet'):
        if 'pesos' not in st.session_state:
            st.session_state['pesos']=abrirArq.parquet('pesos')
        #df_peso = pd.read_parquet(f'{dire}\pesos.parquet')
        df_peso = st.session_state['pesos']
    else:
        df_peso = pd.DataFrame({'id_merc': [0], 'peso': [0.0]})
    df_ML= df[df['Canal de Venda']=='Mercado Livre']

    cont=0
    my_bar = st.progress(0, text="progress_text")
    if len(df_ML)>0:
        for idx in df_ML.index:
            if cont>99:
                cont=0
            cont=cont+1
            my_bar.progress(cont, text=f'Atualizando Pesos...{cont}')
            df.loc[idx, 'TipoEnvio'] = list(EstReg.query(f'Sigla=="{df["UF"][idx]}"')['TipoCusto'])[0]
            if df_ML['id_merc'][idx] not in df_peso['id_merc'].tolist():
                try:
                    url = "https://bling.com.br/Api/v3/produtos/" + str(df_ML['id_merc'][idx])
                    time.sleep(0.4)
                    response_peso = extr.extrai(url)
                    peso=round(response_peso.json()['data']['pesoBruto']*df['Quantidade'][idx],2)
                    df.loc[idx, 'peso']=peso
                    df_peso=pd.concat([df_peso,pd.DataFrame(data={'id_merc':[df_ML['id_merc'][idx]], 'peso': [response_peso.json()['data']['pesoBruto']]})])
                except:
                    pass
            else:
                try:
                    peso=round(list(df_peso.query(f'id_merc=={df_ML["id_merc"][idx]}')['peso'])[0]*df['Quantidade'][idx],2)
                    df.loc[idx, 'peso']=peso
                except:
                    pass
            try:
                df.loc[idx, 'CustoEnvio'] = list(custoEnvio.query(f'Peso<={peso}')[-1:][df['TipoEnvio'][idx]])[0]
            except:
                df.loc[idx, 'CustoEnvio'] = 0.0
    df_peso.query('peso>0.00').to_parquet(f'{dire}\pesos.parquet')
    my_bar.empty()
    return df


def extrair_produtos_nf(df):
    url = "https://bling.com.br/Api/v3/pedidos/vendas/"
    my_bar = st.progress(0, text="progress_text")
    df2=pd.DataFrame()
    df2_transp=pd.DataFrame()
    cont=0
    for id in df['id']:
        cont=cont+1
        if cont==100:
            cont=1
        my_bar.progress(cont, text=f'Mercadoria lida...: {cont}')
        response=extr.extrai(url+str(id))
        lista_itens=response.json()['data']['itens']
        lista_transp=response.json()['data']['transporte']
        df=pd.DataFrame(lista_itens)
        df_transp=pd.DataFrame([lista_transp])
        df2=pd.concat([df,df2], ignore_index=True)
        df2_transp=pd.concat([df_transp,df2_transp], ignore_index=True)
        colunas=['quantidade','desconto','valor','descricao','produto']
    my_bar.empty()
    return df2[colunas].sort_values(by='quantidade',ascending=False),df2_transp['frete'].sum()
