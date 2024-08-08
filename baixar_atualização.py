from git import Repo
import extrair_informacoes as ext
import shutil
import datetime

today=datetime.date.today()
dia=today.strftime('%d/%m/%Y')

dire=ext.ler_toml()['pastas']['dir']
repo_url = 'https://github.com/RogerioCabral1971/plenoled'
local_dir = fr'{dire}app'

shutil.copytree(fr'{dire}app', fr'{dire}app{dia}')
repo = Repo.clone_from(repo_url, local_dir)