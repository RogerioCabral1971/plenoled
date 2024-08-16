import os
import datetime
import locale
import streamlit as st
import pandas as pd
import montar_pag as pag
import atualizar_bases
import extrair_informacoes as ext
import relatorio_plenoled as rel
import baixar_atualização
import abrirArq

# CONFIGURAÇÃO DA PAGINA
st.set_page_config(layout='wide', page_title='PlenoLed',initial_sidebar_state='collapsed')

today=datetime.date.today()
dt_inicio=today-datetime.timedelta(90)
dia=datetime.timedelta(1)
dire=ext.ler_toml()['pastas']['dir']
if 'custoOperacional' not in st.session_state:
    st.session_state['custoOperacional']=abrirArq.parquet('CustoOperacional')
custoDia=st.session_state['custoOperacional']['Custo Mensal'].sum()/30
atualiza=False
ver=1.3

my_bar1 = st.progress(0, text='# :gray-background[ :red[**ATUALIZANDO BASES,  A G U A R D E ...**]]')


#Verificar se arquivo de pedidos de venda está atualizado, se não estiver será atualizado
if os.path.isfile(f'{dire}\pedidos_venda.parquet'):
    arquivo_data=f'{dire}\pedidos_venda.parquet'
    arquivo_data=os.stat(arquivo_data)[8]
    arquivo_data=datetime.datetime.fromtimestamp(arquivo_data).strftime('%d/%m/%Y')
    if arquivo_data!=today.strftime('%d/%m/%Y'):
        os.remove(f'{dire}pedidos_venda.parquet')
        os.remove(f'{dire}pesos.parquet')
        atualizar_bases.extrair_custos()
        atualiza=True
else:
    st.session_state.clear()
    atualiza = True
    atualizar_bases.extrair_custos()



with open(f'{dire}styles.css') as f:
    st.markdown(f'<style>{f.read()}<style>', unsafe_allow_html=True)



def gerar_relatorios_tela(inicial, fim):
    #df = rel.resumo_canal(inicial, fim)
    df2= rel.resumo_canal_new(inicial, fim)
    if len(df2[1]) > 0:
        with aba1:
            pag.grafico_resumo_geral(inicial, fim)

        with aba2:
            st.markdown(f"Resumo do Período de :orange[{format(inicial,'%d/%m/%Y')} até {format(fim,'%d/%m/%Y')}]")
            #st.markdown('---')
            colunaInf1, colunaInf2 = st.columns([0.30, 0.70])

            with colunaInf2:
                with st.container(height=850):
                    pag.cartao_resumo(df2[0], df2[1], (fim-inicial).days+1)
                    st.divider()
                    pag.grafico_vendas(df2[0])

            with colunaInf1:
                with st.container(height=850):
                    pag.detalhe_cartao_resumo()

        with aba3:
            pag.tabela_produto(df2[1])


        with aba4:
            st.markdown(':orange[Custos Fixos por Canal de Venda]')
            col_aba4_1, col_aba4_2 = st.columns([0.55,0.45])

            with col_aba4_1:
                with st.container(height=650):
                    contem = st.container()
                    custos = st.data_editor(st.session_state['custoOperacional'], hide_index=True, num_rows='dynamic')
                    if contem.button('Salvar'):
                        custos.to_parquet(f'{dire}CustoOperacional.parquet')
                        st.rerun()


            with col_aba4_2:
                with st.container(height=650):
                    st.markdown(f'''
                    Informar na tabela ao lado os custos que serão deduzidos da Venda Bruta
                    
                    - Esses custos são de operação para o negocio funcionar, como exemplo, água, luz, salário e etc...
                    - Lembrando que os custos das mercadorias já são informados automaticamente pelo Sistema Bling, portanto 
                    não é necesssário ser informado aqui.
                    - Para inserir uma nova linha, basta clicar na linha logo abaixo da última com informação.
                    - Inserir os custos por Canal de Venda, pois assim conseguimos calcular exatamente o custo de cada canal
                    e apurar a margem
                    
                    :blue-background[*Os nomes dos Canais, precisam ser digitados como abaixo:*]
                    
                    1. :orange[Comercial]
                    1. :orange[Mercado Livre]
                    1. :orange[Tray]
                    
                    :orange[CUSTO POR DIA:] :blue-background[{locale.currency(custoDia,grouping=True)}]
                    :orange[CUSTO POR MÊS:] :blue-background[{locale.currency(st.session_state['custoOperacional']['Custo Mensal'].sum(),grouping=True)}]
                    
                    ***O custo acima são valores informados na Tabela ao Lado, não estão somados aqui os custo que são calculados de acordo com o*** :blue-background[percentual da venda]
                    ''')

    else:
        st.warning('Data sem Movimentação')




# INFORMAÇÕES INICIAL DA PÁGINA
with st.container(border=True):
    col1,col2,col3,col4,col5,col6=st.columns([0.2,0.1,0.2,0.1,0.1,0.1])
    col1.image(f'{dire}img/plenoled.com.br.webp',width=250)
    menu = st.sidebar

    periodo=menu.date_input('Selecione o Periodo', value=(pd.to_datetime(f'{today-datetime.timedelta(30)}'), pd.to_datetime(f'{today}')))
    CTbut = menu.container()
    menu.markdown("""Período para Analise das vendas( :orange[Dados armazenado de 12 meses] )""")


    menu.divider()
    if menu.button('Resetar Aplicativo'):
        data=format(pd.to_datetime(today-datetime.timedelta(60)), '%Y-%m-%d')
        if os.path.isfile(f'{dire}\pedidos_venda.parquet'):
            if 'pedidos_venda' not in st.session_state:
                st.session_state['pedidos_venda']=abrirArq.parquet('pedidos_venda')
                id=st.session_state['pedidos_venda'].query(f'data>="{data}"')['id']
                st.session_state['notas_fiscais'].query(f'id not in {list(id)}').to_parquet(f'{dire}notas_fiscais.parquet')
        if os.path.isfile(f'{dire}\pedidos_venda.parquet'):
            os.remove(f'{dire}pedidos_venda.parquet')
        if os.path.isfile(f'{dire}\pesos.parquet'):
            os.remove(f'{dire}pesos.parquet')
        if os.path.isfile(fr'{dire}\faturas.parquet'):
            os.remove(f'{dire}faturas.parquet')
        st.session_state.clear()
        #atualiza=True
        st.rerun()

    menu.markdown("""Limpar dados Armazenados local e baixar dados antigos e novos ( :orange[Será feito limpeza e baixado todas as informações novamente do Bling] )""")
    menu.divider()

    menu.write(f':blue-background[Versão Atual: {ver}]')
    if menu.button('Baixar Nova Atualização'):
        baixar_atualização.atualizar()
        st.rerun()
    menu.markdown(
        """Baixar atualizações feito para o Aplicativo de Dashboard ( :orange[Baixar Atualizações para o Aplicativo] )""")


# ATUALIZAÇÃO DA BASE DE DADOS

if atualiza:
    if os.path.isfile(f'{dire}pedidos_venda.parquet'):
        if 'pedidos_venda' not in st.session_state:
            st.session_state['pedidos_venda']=abrirArq.parquet('pedidos_venda')
        #vendas=pd.read_parquet(f'{dire}pedidos_venda.parquet')
        vendas = st.session_state['pedidos_venda']
        dt_inicial = pd.to_datetime(vendas['data'].max())

    else:
        vendas=pd.DataFrame()
        dt_inicial = pd.to_datetime(atualizar_bases.dataBase)
        #atualizado=pd.to_datetime(today)==dt_inicial

    if 'base_atualizada' not in st.session_state:
        st.session_state['base_atualizada']=atualizar_bases.vendas(vendas, format(dt_inicial, '%Y-%m-%d'))

    if dt_inicial!=format(atualizar_bases.dataBase):
        if len(vendas)==0:
            if 'pedidos_venda' not in st.session_state:
                st.session_state['pedidos_venda']=pd.read_parquet(f'{dire}pedidos_venda.parquet')
            vendas=st.session_state['pedidos_venda']
    #atualizar_bases.atualizar_situacao(vendas)
#atualizar_bases.valida_dados(vendas, dt_inicial)
aba1, aba2, aba3, aba4 = st.tabs(['Histórico de Vendas', 'Resumo Canal de Venda', 'Resumo por Mercadoria', 'Custos Fixos'])

my_bar1.empty()

# GERAÇÃO DOS RELATÓRIOS
if CTbut.button(f'Aplicar Filtro'):
    inicial=periodo[0]
    fim=periodo[1]
    gerar_relatorios_tela(inicial,fim)
else:
    gerar_relatorios_tela(pd.to_datetime(dt_inicio), pd.to_datetime(f'{today}'))

















