
from tkinter.filedialog import askdirectory, askopenfilename

pasta=askdirectory(title='Abrir')
print(pasta)




import streamlit as st
col1, col2, col3 = st.columns(3)
with col1:
    with st.form('login'):
        st.markdown('### Painel de Login')
        st.text_input('Email', placeholder='Digite seu Email')
        st.text_input('Senha', placeholder='Digite sua senha', type='password')
        st.form_submit_button('Login')