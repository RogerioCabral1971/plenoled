from git import Repo
import extrair_informacoes as ext
import datetime
import os
import shutil

def atualizar():
    today=datetime.date.today()
    dia=today.strftime('%d%m%Y')

    dire=ext.ler_toml()['pastas']['dir']
    repo_url = 'https://github.com/RogerioCabral1971/plenoled'
    local_dir = rf'{dire}appNovo'

    repo = Repo.clone_from(repo_url, local_dir)

    os.rename(rf'{dire}app', fr'{dire}app{str(dia)}')
    os.rename(rf'{dire}appNovo', rf'{dire}app')
