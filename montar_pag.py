import pandas as pd
import streamlit as st
import relatorio_plenoled as rel
import locale
import matplotlib.pyplot as plt
import extrair_informacoes as ext
import abrirArq
from colorama import Fore, Back, Style

locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
status = rel.status_pedido
dire=ext.ler_toml()['pastas']['dir']

def formatar_num(valor):
    valor_frmt="{:,.2f}".format(valor).replace(',','_').replace('.',',').replace('_','.')
    return valor_frmt

def resumo_grafico(df,origem, dias):
    if dias>60:
        df=df.query(f'origem_venda=="{origem}"').groupby(['Ano-Mes'])['total'].sum()
    else:
        df = df.query(f'origem_venda=="{origem}"').groupby(['data'])['total'].sum()
    return df

def grafico_resumo_geral(inicio, fim ):
    df=rel.vendas_historica(inicio, fim)
    dias=(fim - inicio).days
    colunasInf1, colunasInf2 = st.columns([0.40,0.60])


    with colunasInf2:
        with st.container(height=900):
            st.markdown(':orange[GRÁFICO EVOLUÇÃO DAS VENDAS]')
            colGra11, colGra21, colGra31 = st.columns([0.25, 0.5, 0.25])
            colGra1, colGra2, colGra3 = st.columns([0.3, 0.3, 0.3])

            resumo_vendas = pd.DataFrame(df[1]).groupby('origem_venda').total.sum()
            resumo_vendas = pd.DataFrame(resumo_vendas).reset_index().rename(
                columns={'origem_venda': 'Canal de Venda', 'total': 'Venda'})
            explode = (0.1, 0.1, 0.1)
            fig, ax = plt.subplots()
            try:
                ax.pie(list(resumo_vendas['Venda']), labels=list(resumo_vendas['Canal de Venda']), explode=explode,
                   autopct='%1.1f%%',
                   shadow=True, startangle=90, textprops={'color': 'white', 'fontsize': 8, 'weight':'bold'}, colors=['#D2691E', '#FFA500','#FFFF00'])
            except:
                ax.pie(list(resumo_vendas['Venda']), labels=list(resumo_vendas['Canal de Venda']),
                       autopct='%1.1f%%',
                       shadow=True, startangle=90, textprops={'color': 'white', 'fontsize': 8, 'weight': 'bold'},
                       colors=['#D2691E', '#FFA500', '#FFFF00'])

            fig.set_facecolor('#01030c')
            fig.set_size_inches(4, 2)
            colGra21.pyplot(fig)

            st.markdown(f':orange[TOTAL]   :blue-background[{locale.currency(df[1]["total"].sum(), grouping=True)}]')
            st.bar_chart(df[0], height=250, color=['#FF8C00'])
            maior_venda_total=maior_vendas(df[0])[0]
            data_maior_venda_total=maior_vendas(df[0])[1]
            media_venda_total=maior_vendas(df[0], dias)[2]


            origem='Comercial'
            colGra1.markdown(f':orange[{origem}]   :blue-background[{locale.currency(resumo_grafico(df[1], origem, dias).sum(), grouping=True)}]')
            colGra1.bar_chart(resumo_grafico(df[1], origem, dias), height=200, color=['#D2691E'])
            maior_venda_total_c = maior_vendas(resumo_grafico(df[1], origem, dias))[0]
            data_maior_venda_total_c = maior_vendas(resumo_grafico(df[1], origem, dias))[1]
            media_venda_total_c = maior_vendas(resumo_grafico(df[1], origem, dias), dias)[2]

            origem = 'Mercado Livre'
            colGra2.markdown(f':orange[{origem}]   :blue-background[{locale.currency(round(resumo_grafico(df[1], origem, dias).sum(),2), grouping=True)}]')
            colGra2.bar_chart(resumo_grafico(df[1], origem, dias), height=200, color=['#FFA500'])
            maior_venda_total_m = maior_vendas(resumo_grafico(df[1], origem, dias))[0]
            data_maior_venda_total_m = maior_vendas(resumo_grafico(df[1], origem, dias))[1]
            media_venda_total_m = maior_vendas(resumo_grafico(df[1], origem, dias), dias)[2]

            origem = 'Tray'
            colGra3.markdown(f':orange[{origem}]   :blue-background[{locale.currency(resumo_grafico(df[1], origem, dias).sum(), grouping=True)}]')
            colGra3.bar_chart(resumo_grafico(df[1], origem, dias), height=200, color=['#FFFF00'])
            maior_venda_total_t = maior_vendas(resumo_grafico(df[1], origem, dias))[0]
            data_maior_venda_total_t = maior_vendas(resumo_grafico(df[1], origem, dias))[1]
            media_venda_total_t = maior_vendas(resumo_grafico(df[1], origem, dias), dias)[2]



    with colunasInf1:
        with st.container(height=900):
            st.markdown(':orange[RESUMO]')
            st.markdown(f'''
                :gray[*Relatório Demonstrativo de Venda Bruta, Valores retirados da Emissão de NFs*]
                
            Faturamento Total Bruto de :orange[{locale.currency(df[1]["total"].sum(), grouping=True)}] no período de :orange[{format(inicio,"%d/%m/%Y")} a {format(fim,"%d/%m/%Y")}]
                
            Melhor venda de :blue-background[{locale.currency(maior_venda_total,grouping=True)}] em {data_maior_venda_total}
            
            *Média de Venda por dia {locale.currency(media_venda_total,grouping=True)}*
            
            ---
            
            :orange[MELHORES VENDAS POR CANAL]
            
            - ***Comercial*** :blue-background[{locale.currency(maior_venda_total_c,grouping=True)}] em {data_maior_venda_total_c}.
            
            - ***Mercado Livre*** :blue-background[{locale.currency(maior_venda_total_m,grouping=True)}] em {data_maior_venda_total_m}
            
            - ***Site*** :blue-background[{locale.currency(maior_venda_total_t,grouping=True)}] em {data_maior_venda_total_t}
            
            ---
            
            :gray[*Para Escolher um Período, favor abrir o menu ao lado*]
            ''')




def cartao_resumo(df,df2, dias):
    if 'CustoOperacional' not in st.session_state:
        st.session_state['CustoOperacional']=abrirArq.parquet('CustoOperacional')
    custo_Operacao=st.session_state['CustoOperacional']
    frete=df2.groupby('Canal de Venda')['Valor_Frete'].sum()
    frete=pd.DataFrame(frete).reset_index()
    df=pd.DataFrame(df).reset_index()
    df=df.drop_duplicates()

    colunas = st.columns([0.32, 0.20, 0.19, 0.19])
    colunas[0].metric(f"TOTAL VENDA - {df['total'].sum()} Vdas", f" {locale.currency(df['R$ Total'].sum().round(0), grouping=True)}")
    colunas[0].markdown(f':gray[Total Frete {locale.currency(frete["Valor_Frete"].sum(), grouping=True)}]')
    colunas[0].divider()

    # INSERIR CUSTO POR PERCENTUAL DE ACORDO COM VALOR DE VENDA E CUSTOS DE ACORDO COM VALORES MENSAIS POR CANAL
    custo_perc_total=0
    for idx in df['Canal de Venda'].index:
        if custo_Operacao[custo_Operacao['Canal de Venda'] == df['Canal de Venda'][idx]]['Percentual'].sum()>0:
            custo_perc=((custo_Operacao[custo_Operacao['Canal de Venda'] == df['Canal de Venda'][idx]]['Percentual'].sum()) / 100) * df["R$ Total"][idx]
            custo_perc_total=custo_perc_total+custo_perc
        else:
            custo_perc=0

        custo_valor=((custo_Operacao[custo_Operacao['Canal de Venda'] == df['Canal de Venda'][idx]]['Custo Mensal'].sum()/30)*dias)
        if custo_perc>0:
            custo_total=custo_perc+custo_valor
        else:
            custo_total=custo_valor
        try:
            custo_Oper_Canal=custo_total
        except:
            custo_Oper_Canal=0

        colunas[idx + 1].metric(f"{str(df['Canal de Venda'][idx]).upper()} - {df['total'][idx]} Vdas",f"R$ {formatar_num(df['R$ Total'][idx])}")
        colunas[idx + 1].markdown(
            f':gray[{locale.currency(list(frete[frete["Canal de Venda"] == df["Canal de Venda"][idx]]["Valor_Frete"])[0], grouping=True)}]')
        colunas[idx + 1].divider()
        #colunas[idx + 1].markdown('-')
        colunas[idx + 1].markdown(f':red[-]{locale.currency(df["Total Custo"][idx], grouping=True)} :orange[({int((df["Total Custo"][idx]/df["R$ Total"][idx])*100)}%)]')
        colunas[idx + 1].markdown(f':red[-]{locale.currency(custo_Oper_Canal, grouping=True)} :orange[({int((custo_Oper_Canal/df["R$ Total"][idx])*100)}%)]')
        colunas[idx + 1].markdown(f':red[-]{locale.currency(df["IMPOSTO"][idx], grouping=True)}')
        venda_liq_canal = f" {locale.currency(df['R$ Total'][idx] - df['Total Custo'][idx] - df['IMPOSTO'][idx] - custo_Oper_Canal - df['CustoEnvio'][idx] , grouping=True)}"
        colunas[idx+1].markdown(f":gray-background[{venda_liq_canal}]")
        colunas[idx+1].markdown(f":gray-background[{int(((df['R$ Total'][idx] - df['Total Custo'][idx] - df['IMPOSTO'][idx] - custo_Oper_Canal)/df['R$ Total'][idx])*100)}%]")
    #SOMA DOS VALORES DOS CANAIS
    #colunas[0].markdown(':blue-background[RESULTADO DO PERÍODO:]')
    colunas[0].markdown(f":orange[Custo Mercadoria] :red[-]{locale.currency(df['Total Custo'].sum(), grouping=True)} :orange[({int((df['Total Custo'].sum()/df['R$ Total'].sum())*100)}%)]")
    colunas[0].markdown(f":orange[Custo] :red[-]{locale.currency(((custo_Operacao['Custo Mensal'].sum()/30)*dias)+custo_perc_total, grouping=True)} :orange[({int(((((custo_Operacao['Custo Mensal'].sum()/30)*dias)+custo_perc_total)/df['R$ Total'].sum())*100)}%)]")
    colunas[0].markdown(f":orange[Impostos 8%] :red[-]{locale.currency(df['IMPOSTO'].sum(), grouping=True)}")
    venda_liq_total = f":orange[Venda Líquida] :gray-background[{locale.currency(df['R$ Total'].sum() - df['Total Custo'].sum() - df['IMPOSTO'].sum() - custo_perc_total- (custo_Operacao['Custo Mensal'].sum()/30)*dias - df['CustoEnvio'].sum(), grouping=True)}]"
    colunas[0].markdown(f'{venda_liq_total}')
    colunas[0].markdown(f' :orange[Margem] :gray-background[{int(((df["R$ Total"].sum() - df["Total Custo"].sum() - df["IMPOSTO"].sum() - custo_perc_total - (custo_Operacao["Custo Mensal"].sum()/30)*dias)/df["R$ Total"].sum())*100)}%]')

def detalhe_cartao_resumo():
    st.markdown('''
    :orange[Fonte das Informações]
    1. Valores extraidos conforme notas fiscais geradas no Bling
    1. Custos das Mercadorias retirados de :orange[*Home\Cadastros\Produtos\Fornecedores*]
    1. Impostos calculado de acordo com preço de venda = :orange[*Preço de Venda * 8%*]
    1. Custo Operacional calculado de acordo os valores informados nesse aplicativo
    ---
    ''')
def grafico_vendas(df):
    df['Venda Líquida']=df['R$ Total']-df['Total Custo']
    df['Margem Bruta']=df['Venda Líquida']/df['R$ Total']
    contem=st.container(height=350, border=True)
    contem.markdown(':orange[Gráfico comparação Venda Líquida x Margem]')
    contem.bar_chart(df[['Canal de Venda','Margem Bruta', 'Venda Líquida']], x='Canal de Venda', y='Margem Bruta', color='Venda Líquida', width=150, height=300, use_container_width=True)

def tabela_resumo(df,inicial,fim):
    df=st.data_editor(df,key='tabresumo')
    st.data_editor(rel.resumo_nf(inicial,fim))

def tabela_produto(df):
    df=pd.DataFrame(df).reset_index()
    #df = moedaLocal(df)
    df_resumo=df.groupby(['Canal de Venda', 'Descrição Mercadoria'])[['Quantidade','R$ Total', 'Total Custo', 'IMPOSTO', 'CustoEnvio']].sum()
    df_resumo['Venda Líquida'] = df_resumo['R$ Total'] - df_resumo['Total Custo'] - df_resumo['IMPOSTO'] - df_resumo['CustoEnvio']
    df_resumo['Margem'] = round((df_resumo['Venda Líquida']/df_resumo['R$ Total'])*100,0)
    df_resumo['R$ Total']=df_resumo['R$ Total'].map(lambda x: locale.currency(x, grouping=True))
    df_resumo['Total Custo'] = df_resumo['Total Custo'].map(lambda x: locale.currency(x, grouping=True))
    df_resumo['Venda Líquida'] = df_resumo['Venda Líquida'].map(lambda x: locale.currency(x, grouping=True))
    df_resumo['IMPOSTO'] = df_resumo['IMPOSTO'].map(lambda x: locale.currency(x, grouping=True))
    df_resumo['CustoEnvio'] = df_resumo['CustoEnvio'].map(lambda x: locale.currency(x, grouping=True))

    df_resumo=pd.DataFrame(df_resumo).reset_index()

    conteudo1 = st.container(height=200)
    conteudo2 = st.container(height=200)
    conteudo3 = st.container(height=200)

    try:
        conteudo1.markdown('Maiores Vendas no Canal de Vendas :orange[COMERCIAL]')
        conteudo1.data_editor(df_resumo[df_resumo['Canal de Venda']=='Comercial'].sort_values('Quantidade', ascending=False), hide_index=True)

        conteudo2.markdown('Maiores Vendas no Canal de Vendas :orange[MERCADO LIVRE]')
        conteudo2.data_editor(
        df_resumo[df_resumo['Canal de Venda'] == 'Mercado Livre'].sort_values('Quantidade', ascending=False), hide_index=True)

        conteudo3.markdown('Maiores Vendas no Canal de Vendas :orange[SITE]')
        conteudo3.data_editor(
        df_resumo[df_resumo['Canal de Venda'] == 'Tray'].sort_values('Quantidade', ascending=False), hide_index=True)
    except:
        pass
    #df = df.sort_values('Quantidade', ascending=False)
    #st.data_editor(df, hide_index=True, width=1200, height=1200,
    #               column_config={"R$ Total": {"alignment": "center"}, "Quantidade": {"alignment": "center"}})

    #if st.button('Copiar'):
    #    df.to_clipboard(excel=True)



def maior_vendas(df, dias=0):
    maior_venda_total = pd.DataFrame(df)['total'].max()
    if dias>0:
        media_venda=pd.DataFrame(df)['total'].sum()/(dias+1)
    else:
        media_venda=0
    if len(df) > 0:
        data_maior_venda_total = pd.DataFrame(df).reset_index().query(f'total=={maior_venda_total}')
        data_maior_venda_total = list(data_maior_venda_total[data_maior_venda_total.columns[0]])[0]
    else:
        data_maior_venda_total=0
        data_maior_venda_total=0
    return maior_venda_total, data_maior_venda_total, media_venda