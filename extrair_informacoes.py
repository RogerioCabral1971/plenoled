import time
import requests
import base64
import toml
import streamlit as st


def ler_toml():
    with open("C:/PlenoLed/secrets.toml", "r") as f:
        valores = toml.load(f)
    return valores

CLIENT_ID = ler_toml()['credenClient']['client_id']
CLIENT_SECRET = ler_toml()['credenClient']['client_secrets']
REDIRECT_URI = "https://www.google.com/"


def extrai(url):
    payload = {}
    headers = {
        'Accept': 'application/json',
        'Authorization': f"{ler_toml()['credenciais']['token']}",
        'Cookie': f"{ler_toml()['credenciais']['Cookie']}"}
    response = requests.request("GET", url, headers=headers, data=payload)
    if response.status_code!=200:
        if response.status_code==401:
            refreshToken()
            #st.warning(response.json()['description'])

    return response

def refreshToken():
    refresh_token=ler_toml()['credenciais']['refresh']
    base64_encoded_clientid_clientsecret = base64.b64encode(str.encode(f'{CLIENT_ID}:{CLIENT_SECRET}'))
    base64_encoded_clientid_clientsecret = base64_encoded_clientid_clientsecret.decode('ascii')
    url = f"https://bling.com.br/Api/v3/oauth/token"
    headers = {
        'Content-Type': "application/x-www-form-urlencoded",
        'Authorization': f'Basic {base64_encoded_clientid_clientsecret}'
    }
    data = {'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET
            }
    r = requests.post(url, headers=headers, data=data)
    #if r.json()['error']['description'] =='Invalid refresh token':
    #    print(r.json())
    salvar_toml(r)



def codigo_acesso():
    codigo='e0d8ce7b32d6032b2dee5eaedbd61d1d111c80aa'
    base64_encoded_clientid_clientsecret = base64.b64encode(str.encode(f'{CLIENT_ID}:{CLIENT_SECRET}'))
    base64_encoded_clientid_clientsecret = base64_encoded_clientid_clientsecret.decode('ascii')
    url = f"https://bling.com.br/Api/v3/oauth/token"
    headers = {
        'Content-Type': "application/x-www-form-urlencoded",
        'Authorization': f'Basic {base64_encoded_clientid_clientsecret}'
    }
    data = {'grant_type': 'authorization_code',
            'redirect_uri': REDIRECT_URI,
            'code': codigo
            }
    r = requests.post(url, headers=headers, data=data)
    response = r.json()
    print(response)
    return response['access_token'],response['refresh_token']

def salvar_toml(r):
    valores= ler_toml()
    r.json()
    valores['credenciais']['token']= 'Bearer ' + r.json()['access_token']
    valores['credenciais']['refresh'] = r.json()['refresh_token']
    with open("C:/PlenoLed/secrets.toml", 'w') as f:
        toml.dump(valores, f)
    st.success('AGUARDE...')
    st.rerun()



