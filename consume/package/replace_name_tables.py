import os

import re




root = r'..\..\querys\SQL'
files = os.listdir(root)

def replace_name(file):
    with open(os.path.join(root, file), 'r+') as query:
        content = query.read()
        
        # Substitui o padrão no conteúdo do arquivo
        content = re.sub(r"(?<=INSERT INTO\s)\b\w*\b", 'OBITOS', content)
        
        # Move o cursor para o início do arquivo
        query.seek(0)
        
        # Escreve o conteúdo modificado de volta para o arquivo
        query.write(content)
        
        # Trunca o restante do conteúdo, caso o novo conteúdo seja menor
        query.truncate()

    print(f'Arquivo: {file} modificado com sucesso!')


#insert_into(files[0])

for file in files:
    replace_name(file)



# Para rodar entrar no diretorio:
#PS C:\Users\Administrador\Desktop\Trabalho\Projetos\sus\consume\package>