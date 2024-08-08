import pandas as pd
import canal_venda as cv
import xmltodict
import extrair_informacoes as extr
import streamlit as st

dir=extr.ler_toml()['pastas']['dir']

def nota_fiscal(url):
    cont=0
    df = pd.DataFrame()
    while True:
        cont = cont + 1
        nf = url + str(cont)
        response_nf = extr.extrai(nf)
        df_nf=pd.DataFrame(response_nf.json()['data'])
        if len(df_nf) > 0:
            df_nf = cv.canal_venda(df_nf)
            #df_nf = sit.situacao(df_nf)
            df = pd.concat([df, df_nf])
        else:
            break
    return df


def arqXml_valor_imposto(id):
    try:
        url = f"https://bling.com.br/Api/v3/pedidos/vendas/{id}"
        response_vendas = extr.extrai(url)
        id_nf=response_vendas.json()['data']['notaFiscal']['id']
        url = f"https://bling.com.br/Api/v3/nfe/{id_nf}"
        response_nf = extr.extrai(url)
        sitexml = response_nf.json()['data']['xml']
        url = sitexml
        response_xml = extr.extrai(url)
        arqXml = response_xml.text
        df = xmltodict.parse(arqXml)
        valor_total_tributo=df['nfeProc']['NFe']['infNFe']['det']['imposto']['vTotTrib']
    except:
        valor_total_tributo=0
        df=()
    return valor_total_tributo,df

def arqXml_valor_impost(id):
    try:
        #url = f"https://bling.com.br/Api/v3/pedidos/vendas/{id}"
        #%response_vendas = extr.extrai(url)
        #id_nf=response_vendas.json()['data']['notaFiscal']['id']
        #url = f"https://bling.com.br/Api/v3/nfe/{id_nf}"
        #response_nf = extr.extrai(url)
        #sitexml = response_nf.json()['data']['xml']
        #url = sitexml
        #response_xml = extr.extrai(url)
        #arqXml = response_xml.text
        notas = pd.read_parquet(f'{dir}notas_fiscais.parquet')
        id_notas=notas.query('id==20421108372')['notaFiscal'][0]['id']
        arqXml = pd.read_xml(fr'xml\{id}.xml')
        #df = xmltodict.parse(arqXml)
        valor_total_tributo=id_notas['nfeProc']['NFe']['infNFe']['det']['imposto']['vTotTrib']
    except:
        valor_total_tributo=0
        df=()
    return valor_total_tributo,df