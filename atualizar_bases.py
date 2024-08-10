import time
import extrair_informacoes as extr
import pandas as pd
import streamlit as st
import situacoes as sit
import datetime
import xmltodict

dire=extr.ler_toml()['pastas']['dir']
today = datetime.datetime.now()
dataBase=format(pd.to_datetime(today-datetime.timedelta(183)), '%Y-%m-%d')


def atualizar_situacao(pedidos_vendas):
  df_nf = pd.read_parquet(f'{dire}notas_fiscais.parquet')
  #pedidos_vendas.drop(columns={'index'}, inplace=True)
  id_aberto = pedidos_vendas.query('Descr_situacao!="Finalizado"')
  if len(id_aberto)>0:
    for idx in id_aberto.index:
      if id_aberto['id'][idx] in list(df_nf['id']):
        pedidos_vendas.loc[idx, 'Descr_situacao'] = 'Finalizado'
    pedidos_vendas.to_parquet(f'{dire}pedidos_venda.parquet')


def vendas(df_orig, dt_inicial):
  dt_fim = format(pd.to_datetime(today), '%Y-%m-%d')
  if dt_inicial!=format(dataBase):
    dt_inicial = format(pd.to_datetime(dt_fim) - datetime.timedelta(5), '%Y-%m-%d')
    reiniciar=False
  else:
    reiniciar = True
    df_orig=pd.DataFrame([{'numero':0,'data':dt_inicial}])
  ultimo_pedido_base = df_orig['numero'].max()
  continua=True
  url = f"https://bling.com.br/Api/v3/pedidos/vendas?dataInicial={dt_inicial}&dataFinal={dt_fim}&pagina="
  vendas = url + str(1)
  response_verificar = extr.extrai(vendas)

  try:
    ultimo_pedido=response_verificar.json()['data'][0]['numero']
  except:
    if response_verificar.json()['error']['message']=='Limite de requisições atingido.':
      response_verificar = extr.extrai(vendas)
      ultimo_pedido = response_verificar.json()['data'][0]['numero']
  if ultimo_pedido!=ultimo_pedido_base:
    dt_fim1 = response_verificar.json()['data'][0]['data']
    dt_inicial=list(df_orig[df_orig['numero']==ultimo_pedido_base]['data'])[0]
    url = f"https://bling.com.br/Api/v3/pedidos/vendas?dataInicial={dt_inicial}&dataFinal={dt_fim1}&pagina="
    cont = 0
    cont_bar=0
    df=pd.DataFrame()
    my_bar = st.progress(0, text="Extrair Vendas")
    while continua:
      cont=cont+1
      cont_bar=cont_bar+1
      if cont_bar>99:
        cont_bar=0
      my_bar.progress(cont_bar, text=f'Página Lida...: {cont_bar}')
      vendas = url+str(cont)
      response_vendas = extr.extrai(vendas)

      if response_vendas.status_code==200:
        df_vendas=pd.DataFrame(response_vendas.json()['data'])
        if len(df_vendas)>0:
          df_vendas = sit.situacao(df_vendas)
          df_vendas = id_canal_venda(df_vendas)
          df_vendas=df_vendas.reset_index()
          df = pd.concat([df, df_vendas])
          try:
            df.drop(columns={'index', 'level_0'}, inplace=True)
          except:
            pass
          df = df.reset_index()
          df_orig=pd.concat([df_orig, df])
        else:
          continua=False
      else:
        continua=False
    df_orig.drop(columns={'index', 'level_0'}, inplace=True)
    df_orig = df_orig.drop_duplicates()
    df_orig=df_orig.reset_index()
    df_orig.query('numero!=0 and Descr_situacao=="Finalizado" and Descr_situacao!="Cancelado"').to_parquet(f'{dire}pedidos_venda.parquet')
    if reiniciar!=True:
      df.drop(columns={'index', 'level_0'}, inplace=True)
    df = df.drop_duplicates()
    if len(df)>0:
      nf_falta(df, reiniciar)
    my_bar.empty()
  return True


def nf_falta(df, reiniciar):
  if reiniciar:
    df_nf_orig=pd.DataFrame()
  else:
    df_nf_orig=pd.read_parquet(f'{dire}notas_fiscais.parquet')
  df_nf = pd.DataFrame()
  cont=0
  cont_bar=0
  my_bar = st.progress(0, text="Extrair Notas")
  for id in df['id']:
    cont=cont+1
    if cont_bar>99:
      cont_bar=0
    cont_bar=cont_bar+1
    my_bar.progress(cont_bar, text=f'Pedidos Lido...: {cont} de {len(df["id"])}')
    url = f"https://bling.com.br/Api/v3/pedidos/vendas/{int(id)}"
    response_vendas = extr.extrai(url)
    nf = pd.DataFrame([response_vendas.json()['data']])
    nf.loc[0, 'Emitida'] = response_vendas.json()['data']['notaFiscal']['id']
    nf.loc[0, 'SIT'] = response_vendas.json()['data']['situacao']['id']
    df_nf = pd.concat([df_nf, nf])
  df_nf = df_nf.query('SIT!=12')
  df_nf = df_nf.query('Emitida>0')
  df_nf=df_nf.reset_index()
  df_nf.drop(columns={'index'}, inplace=True)
  df_nf_fim=pd.concat([df_nf,df_nf_orig])
  df_nf=df_nf.reset_index()
  df_nf_fim.to_parquet(f'{dire}notas_fiscais.parquet')
  my_bar.empty()

def extrair_custos():
  url = "https://bling.com.br/Api/v3/produtos/fornecedores?pagina="
  continua=True
  cont = 0
  cont_bar = 0
  df_custo = pd.DataFrame()

  my_bar = st.progress(0, text="Extrair Custo")
  while continua:
    cont = cont + 1
    cont_bar = cont_bar + 1
    if cont_bar > 99:
      cont_bar = 0
    my_bar.progress(cont_bar, text=f'Página Custos Lidas...: {cont_bar}')
    custos = url + str(cont)
    response_custos = extr.extrai(custos)
    if response_custos.status_code == 200 and len(response_custos.json()['data'])>0:
      cont_linha=0
      while len(response_custos.json()['data'])>cont_linha:
        df_custo = pd.concat([df_custo,pd.DataFrame(response_custos.json()['data'][cont_linha])[['produto','precoCusto']]])
        cont_linha = cont_linha+1
    else:
      continua=False
  df_custo = df_custo.reset_index()
  df_custo.drop(columns={'index'}, inplace=True)
  pd.DataFrame(df_custo).to_parquet(f'{dire}custo.parquet')
  my_bar.empty()

def extrair_prod_peso():
  url = "https://bling.com.br/Api/v3/produtos?"
  continua = True
  cont = 0
  cont_bar = 0
  df_merc = pd.DataFrame()
  my_bar = st.progress(0, text="Extrair Mercadorias Peso")
  while continua:
    cont = cont + 1
    cont_bar = cont_bar + 1
    if cont_bar > 99:
      cont_bar = 0
    my_bar.progress(cont_bar, text=f'Página Mercadorias Lidas...: {cont_bar}')
    pesos = url + 'pagina=' + str(cont) + '&criterio=2'
    response_pesos = extr.extrai(pesos)
    if response_pesos.status_code == 200 and len(response_pesos.json()['data']) > 0:
      df_merc = pd.concat([df_merc, pd.DataFrame(pd.DataFrame(response_pesos.json()['data'])['id'])])
    else:
      continua=False
  mercadoria=[]
  for merc in df_merc['id']:
    url = "https://bling.com.br/Api/v3/produtos/" + str(merc)
    time.sleep(0.3)
    response_peso = extr.extrai(url)
    mercadoria.append(response_peso.json()['data']['pesoBruto'])





def valida_dados(df, dt_inicial):
  if dt_inicial != pd.to_datetime(dataBase):
    my_bar = st.progress(0, text="Validando Dados")
    nf = pd.read_parquet(f'{dire}notas_fiscais.parquet')
    df_id=df.query(f'id not in ({list(nf["id"])})')
    reiniciar=False
    if len(df_id)>0:
      pd.DataFrame(df_id).to_parquet('id.parquet')
      my_bar.progress(0, text=f'Atualizando Notas Fiscais')
      df_falta=pd.read_parquet('id.parquet')
      nf_falta(df_falta, reiniciar)
    else:
      my_bar.empty()
      #impostos_falta()

#FUNÇÕES SEM USO
def impostos_falta():

  id_nota = pd.read_parquet(f'{dire}notas_fiscais.parquet')
  df_orig= pd.read_parquet(f'{dire}impostos.parquet')
  id_ref = id_nota.query(f'id not in ({list(df_orig["id"])})')
  #id_ref=id_ref.reset_index()
  if len(id_ref)>0:
    ExtrairImpostos(id_ref)



def ExtrairImpostos(id_nota):
  df_impostos = pd.DataFrame({'id': list(id_nota['id']), 'id_nf': pd.DataFrame(list(id_nota['notaFiscal']))['id']})
  df_impostos['Impostos'] = 0.0
  my_bar = st.progress(0, text="Extrair Impostos")
  cont=0
  for idx in df_impostos.index:
    cont = cont + 1
    if cont>99:
      cont=1
    my_bar.progress(cont, text=f'Nota Fiscal lida...: {cont}')
    if df_impostos['id_nf'][idx] > 0:
      url = f"https://bling.com.br/Api/v3/nfe/{str(df_impostos['id_nf'][idx])}"
      response_nf = extr.extrai(url)
      if response_nf.status_code == 200:
        sitexml = response_nf.json()['data']['xml']
        if sitexml != '':
          response_xml = extr.extrai(sitexml)
          arqXml = response_xml.text
          xml = xmltodict.parse(arqXml)
          if str(xml['nfeProc']['NFe']['infNFe']['det'])[0] == '[':
            if 'vTotTrib' in xml['nfeProc']['NFe']['infNFe']['det'][0]['imposto'].keys():
              impost = float(xml['nfeProc']['NFe']['infNFe']['det'][0]['imposto']['vTotTrib'])
          else:
            impost = float(xml['nfeProc']['NFe']['infNFe']['det']['imposto']['vTotTrib'])
          df_impostos.loc[idx, 'Impostos'] = impost
  df_impostos_orig=pd.read_parquet(f'{dire}impostos.parquet')
  df_impostos_orig=pd.concat([df_impostos_orig,df_impostos])
  df_impostos_orig.to_parquet(f'{dire}Impostos.parquet')
  my_bar.empty()


def nf(df):
  df_nf = pd.DataFrame()
  cont=0
  my_bar = st.progress(0, text="Extrair Vendas")
  for id in df['id']:
    cont=cont+1
    my_bar.progress(cont, text=f'Pedidos Lido...: {cont} de {len(df["id"])}')
    url = f"https://bling.com.br/Api/v3/pedidos/vendas/{id}"
    response_vendas = extr.extrai(url)
    nf = pd.DataFrame([response_vendas.json()['data']])
    df_nf = pd.concat([df_nf, nf])
  df_nf=df_nf.reset_index()
  df_nf_fim=pd.concat([df_nf,pd.read_parquet(f'{dir}notas_fiscais.parquet')])
  #df_nf_fim=df_nf_fim.drop_duplicates()
  df_nf=df_nf.reset_index()
  df_nf_fim.to_parquet(f'{dir}notas_fiscais.parquet')
  id_nota = pd.DataFrame(list(df_nf['notaFiscal']))
  id_nota = id_nota.rename(columns={'id': 'id_nf'})
  id_nota['id'] = df_nf['id']
  id_nota = id_nota.query('id_nf>0')
  id_nota = id_nota.reset_index()
  #NFxml(id_nota)
  Exportxml(id_nota)
  my_bar.empty()

def NFxml(id_nota):
  df_xml = pd.DataFrame()
  cont=0
  my_bar = st.progress(0, text="Extrair xml")
  for id_nf in id_nota['id']:
    cont=cont+1
    my_bar.progress(cont, text=f'Pedidos Lido...: {cont} de {len(id_nota["id"])}')
    try:
      if id_nf > 0:
        url = f"https://bling.com.br/Api/v3/nfe/{id_nf}"
        response_nf = extr.extrai(url)
        sitexml = response_nf.json()['data']['xml']
        response_xml = extr.extrai(sitexml)
        arqXml = response_xml.text
        xml = xmltodict.parse(arqXml)
        df = valor_chave(xml)
        df_xml=pd.concat([df_xml, df])
    except:
      pass
  df_xml.to_parquet(f'{dire}xml.parquet')
  my_bar.empty()
  return df_xml


def valor_chave(xml):
  df=pd.DataFrame()
  chaves = [xml['nfeProc']['NFe']['infNFe'].keys()]
  for key in pd.DataFrame.from_dict(chaves):
    nome_coluna=pd.DataFrame.from_dict(chaves)[key][0]
    df[nome_coluna]=str(xml['nfeProc']['NFe']['infNFe'][nome_coluna])

  chaves = [xml['nfeProc']['NFe']['Signature'].keys()]
  for key in pd.DataFrame.from_dict(chaves):
    nome_coluna = pd.DataFrame.from_dict(chaves)[key][0]
    df[nome_coluna] = str(xml['nfeProc']['NFe']['Signature'][nome_coluna])

  chaves = [xml['nfeProc']['protNFe']['infProt'].keys()]
  for key in pd.DataFrame.from_dict(chaves):
    nome_coluna = pd.DataFrame.from_dict(chaves)[key][0]
    df[nome_coluna] = str(xml['nfeProc']['protNFe']['infProt'][nome_coluna])
  return df


def Exportxml(id_nota):
  cont = 0
  cont_bar=0
  imposto=[]
  ID_nf=[]
  ID_pedido=[]
  my_bar = st.progress(0, text="Extrair Impostos")
  for idx in id_nota.index:
    cont = cont + 1
    cont_bar=cont_bar+1
    if cont_bar>99:
      cont_bar=0
    my_bar.progress(cont_bar, text=f'Extrair Impostos {cont} de {len(id_nota)} : {id_nota["id_nf"][idx]}')
    #print(f'{cont} de {len(id_nota)} ID:{str(id_nota["id"][idx])}')

    try:
      if id_nota["id_nf"][idx] > 0:
        url = f"https://bling.com.br/Api/v3/nfe/{str(id_nota['id_nf'][idx])}"
        response_nf = extr.extrai(url)
        sitexml = response_nf.json()['data']['xml']
        response_xml = extr.extrai(sitexml)
        arqXml = response_xml.text
        xml = xmltodict.parse(arqXml)
        impost=xml['nfeProc']['NFe']['infNFe']['det']['imposto']['vTotTrib']
        imposto.append(impost)
        ID_nf.append(int(id_nota["id_nf"][idx]))
        ID_pedido.append(int(id_nota["id"][idx]))
        data = str(xml['nfeProc']['NFe']['infNFe']['ide']['dhEmi'])[0:10]
        #pd.DataFrame(xml).to_xml(fr"xml\{id_nf}.xml")
    except:
      pass
  df_orig=pd.read_parquet(f'{dire}impostos.parquet')
  df=pd.DataFrame({'id':ID_pedido,'Id NF':ID_nf,'Impostos':imposto})
  df['Impostos'] = df['Impostos'].astype('float')
  df_orig=pd.concat([df_orig,df])
  df_orig=df_orig.drop_duplicates()
  df_orig.to_parquet(f'{dire}impostos.parquet')
  my_bar.empty()


def id_canal_venda(df_canal):
  for idx in df_canal.index:
    df_canal.loc[idx, 'id_canal_venda'] = int(df_canal['loja'][idx]['id'])
  df_canal['id_canal_venda']=df_canal['id_canal_venda'].astype('int')
  df_canal.drop(columns={'loja','contato'}, inplace=True)
  return df_canal
