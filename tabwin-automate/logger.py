import json
from datetime import datetime

def write_log(name, status="Ok"):
    # Cria um dicionário com os dados do registro
    data = {
        "name": name,
        "status": status,
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    # Tenta abrir o arquivo de log
    try:
        with open("log.json", "r") as file:
            log = json.load(file)  # Carrega os registros existentes
    except FileNotFoundError:
        # Se o arquivo não existir, cria um log vazio
        log = []

    # Adiciona o novo registro ao log
    log.append(data)

    # Grava o log de volta no arquivo
    with open("log.json", "w") as file:
        json.dump(log, file, indent=4)

