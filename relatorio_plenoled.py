import pandas as pd
import vendas as vd
#import nf as nf

status_pedido = ['Em Expedição','Finalizado','Nota Fiscal Antecipada']

def resumo_canal_new(dt_inicial,dt_fim):
    df = vd.vendas('url', dt_inicial, dt_fim, status_pedido)
    #df_resumo = df[0]
    df_prod=df[0]
    df_frete=df[1]
    if len(df[1]) > 0:
        #df2 = df_resumo.groupby('origem_venda').total.agg('count')
        df2 = df_prod.groupby('Canal de Venda')['R$ Total'].count()
        df2 = df2.reset_index()
        #df2=df2.rename(columns={'origem_venda':'Canal de Venda'})
        df2 = df2.rename(columns={'R$ Total': 'total'})
        df_resumo=df_prod.groupby('Canal de Venda')[['R$ Total','Total Custo', 'IMPOSTO', 'CustoEnvio']].sum()
        df_resumo=pd.merge(df_resumo, df2, how='inner', on='Canal de Venda')

    return df_resumo, df_prod, df_frete

def resumo_canal(dt_inicial,dt_fim):
    df = vd.vendas('url', dt_inicial, dt_fim, status_pedido)
    df_resumo=df[0]
    df_prod=df[1]
    df_frete=df[2]
    df2=pd.DataFrame()
    if len(df[0]) > 0:
        df2=df_resumo.groupby('origem_venda').total.agg(['count','sum','mean']).rename(columns={
           'count':'Quantidade Venda','sum':'Total R$', 'mean':'Media R$'})
        df2.reset_index(inplace=True)
        df3 = df_resumo.groupby('origem_venda')['Impostos'].agg('sum')
        df2=pd.merge(df2,df3,how = 'inner', on = 'origem_venda')
        df2['% Venda']=((df2['Total R$'] / df2['Total R$'].sum()) * 100).round(0)
        df2=df2.rename(columns={'origem_venda': 'Canal de Venda'})
        df2.sort_values('Total R$', inplace=True, ascending=False)
    return df2, df_prod, df_frete

def vendas_historica(inicio, fim):
    vendas=vd.venda_geral(inicio, fim, status_pedido)
    if (fim-inicio).days>60:
        resumo = vendas.groupby(['Ano-Mes'])['total'].sum()
    else:
        resumo = vendas.groupby(['data'])['total'].sum()
    return resumo,vendas

def resumo_vendas(dt_inicial,dt_fim):
    df=vd.vendas(f"https://bling.com.br/Api/v3/pedidos/vendas?dataInicial={dt_inicial}&dataFinal={dt_fim}&pagina=")
    df2=pd.pivot_table(df[['Descr_situacao', 'origem_venda','total']],index=['origem_venda','Descr_situacao'], aggfunc="sum").rename(columns={'origem_venda':'Canal de Venda'})
    return df2

def resumo_nf(dt_inicial, dt_fim):
    df=nf.nota_fiscal(f"https://bling.com.br/Api/v3/nfe?dataEmissaoFinal={dt_fim}&dataEmissaoInicial={dt_inicial}&pagina=")
    return df