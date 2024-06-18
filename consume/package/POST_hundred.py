import os
import bd_classes
import json
import math

bd = bd_classes.Consume_bd()

root = r'..\..\querys\SQL'
files = os.listdir(root)

def cleaning_content(content):
    index_table = content.find(';')
    slice_table = content[index_table:]
    content_list = slice_table.split('\n')
    return content_list[1:-1]


def insert_into(file,page=100):
    with open(os.path.join(root, file), 'r') as query:
        content = query.read()
        insert_list = cleaning_content(content)
        times = math.ceil(len(insert_list) / page)
        for i in range(times):
            query = ''.join(insert_list[page*i:page*(i+1)])
            try:
                bd.make_query(query)
            except Exception as e:
                error_log(i,e,file)


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


def error_log(i, e, file_name):
    log = {
        "file_name": file_name,
        "index_failed": i,
        "type_error": str(e),
    }

    if os.path.exists("error_log.json"):
        with open("error_log.json", "r", encoding="utf-8") as file:
            error = json.load(file)
            error.append(log)

        with open("error_log.json", "w", encoding="utf-8") as file:
            json.dump(error, file, ensure_ascii=False, indent=4)
    else:
        with open("error_log.json", "w", encoding="utf-8") as file:
            json.dump([log], file, ensure_ascii=False, indent=4)



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