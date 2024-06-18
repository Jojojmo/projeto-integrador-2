import os
import bd_classes
import json


bd = bd_classes.Consume_bd()

root = r'..\..\querys\SQL'
files = os.listdir(root)

def insert_into(file):
    with open(os.path.join(root, file), 'r') as query:
        content = query.read()
        content_list = content.split(';')
        for row in content_list[1:-1]:
            bd.make_query(row)
    #print(f'Arquivo: {file} inserido com sucesso!')


def load_log():
    log_file = 'files_logs.json'
    if not os.path.exists(log_file):
        raise FileNotFoundError("O arquivo de log não existe.")

    with open(log_file, 'r') as f:
        return json.load(f)


def check_processed(file_name, processed_files):
    # Verifica se o arquivo já foi processado
    if file_name in processed_files:
        print(f"Arquivo {file_name} já processado. Pulando...")
        return False
    else:
        return True

def write_to_log(file_name, processed_files):
    log_file = 'files_logs.json'

    # Adiciona o arquivo ao log
    processed_files.append(file_name)

    # Escreve os arquivos processados de volta no arquivo de log
    with open(log_file, 'w') as f:
        json.dump(processed_files, f)

    print(f"Arquivo {file_name} adicionado ao log com sucesso.")



table_definida = True
if not table_definida:
    with open(r'..\..\definition_table.sql') as query:
        content = query.read()
        bd.make_query(content)


#insert_into(files[0])

stops = 0

for file in files:
    log = load_log()
    if check_processed(file, log):
        insert_into(file)
        write_to_log(file, log)
        stops += 1
    if stops >= 5:
        break


# Para rodar entrar no diretorio:
#PS C:\Users\Administrador\Desktop\Trabalho\Projetos\sus\consume\package>
    
# Tarefas:
# Criar um método para fazer mais inserts
# Ver intervalos de commits e suas diferenças 
# Melhorar ou criar recriar partes do Consume para não quebrar se achar algum erro no insert into
#