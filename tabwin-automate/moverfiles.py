import os
import shutil  # Adicionando a importação do módulo shutil

def list_files(pasta):
    # Verifica se o caminho da pasta é válido
    if os.path.isdir(pasta):
        # Lista todos os arquivos e diretórios dentro da pasta
        return os.listdir(pasta)
    else:
        return "Caminho inválido!"

def move_files(origem, destino):
    try:
        # Move os arquivos da origem para o destino
        shutil.move(origem, destino)  # Corrigindo para usar shutil.move
    except Exception as e:
        # Levanta a exceção
        raise e




# Caminho da pasta que você deseja listar
root_dbf = r"C:\Users\jmoni\OneDrive\Área de Trabalho\TABWIN\SIM\DBF"
destiny_dbf = r"C:\Users\jmoni\OneDrive\Área de Trabalho\TABWIN\SIM\AUTOMACAO\MAKED"
destiny_sql = r"C:\Users\jmoni\OneDrive\Área de Trabalho\TABWIN\SIM\AUTOMACAO\SQL"

# Chama a função para listar os arquivos e diretórios dentro da pasta especificada
arquivos_e_diretorios = list_files(root_dbf)

#move_files(arquivos_e_diretorios[0])

first_relative_dbf = arquivos_e_diretorios[0]
first_relative_sql = arquivos_e_diretorios[1]

#move_files(os.path.join(root_dbf,first_relative_dbf),destiny_dbf)
#move_files(os.path.join(root_dbf,first_relative_sql),destiny_sql)

