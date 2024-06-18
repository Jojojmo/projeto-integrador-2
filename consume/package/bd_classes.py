import psycopg2
import json
import os

class Consume_bd:
    """
    Uma classe para interagir com um banco de dados PostgreSQL usando psycopg2.

    Métodos:
        - start(): Inicia a conexão com o banco de dados usando as configurações fornecidas em 'settings_bd.json'.
        - end(): Encerra a conexão com o banco de dados.
        - make_query(query: str) -> list or None: Executa a consulta SQL fornecida e retorna os resultados como uma lista.
          Retorna None se a consulta não retornar dados.

    Exemplo de uso:
        >>> consumidor = Consume_bd()
        >>> resultado = consumidor.make_query("SELECT * FROM tabela;")
        >>> print(resultado)
        [(1, 'valor1'), (2, 'valor2'), ...]
        >>> consumidor.end()
    """

    def start(self):
        """
        Inicia a conexão com o banco de dados usando as configurações fornecidas em 'settings_bd.json'
        localizado na raiz do projeto.

        Retorna:
            bool: True se a conexão for bem-sucedida, False em caso de falha.
        """
        root_directory = os.path.dirname(os.path.dirname(os.path.abspath('__file__')))
        settings_path = os.path.join(root_directory, 'settings_bd.json')

        if not os.path.exists(settings_path):
            print(f"Arquivo 'settings_bd.json' não encontrado em {root_directory}. Certifique-se de que o arquivo existe.")
            return False

        try:
            with open(settings_path, 'r') as setting:
                kwargs = json.load(setting)
            self.__conn = psycopg2.connect(**kwargs)
            self.__cur = self.__conn.cursor()
            return True
        except psycopg2.Error as e:
            print("Erro ao conectar ao banco de dados. Verifique as informações em 'settings_bd.json'.")
            print("="*83)
            print(f"Detalhes do erro: {e}")
            return False

    def end(self):
        """
        Encerra a conexão com o banco de dados.
        """
        self.__conn.close()
        self.__cur.close()

    def make_query(self, query: str) -> list | None:
        """
        Executa a consulta SQL fornecida e retorna os resultados como uma lista.
        Retorna None se a consulta não retornar dados.

        Parâmetros:
            query (str): A consulta SQL a ser executada.

        Retorna:
            list or None: Uma lista contendo os resultados da consulta ou None se a consulta não retornar dados.
        """
        if not self.start():
            return None

        self.__cur.execute(query)
        self.__conn.commit()
        if self.__cur.description is not None:
            fetch = self.__cur.fetchall()
        else:
            fetch = self.__cur.rowcount
        self.end()
        return fetch


class Basic_select(Consume_bd):
    """
    Uma classe que herda de Consume_bd e fornece funcionalidades básicas para operações SELECT em consultas SQL.

    Parâmetros do Construtor:
        - table (str): Nome da tabela no banco de dados.
        - columns (list, opcional): Lista de colunas a serem selecionadas. Padrão é None (seleciona todas as colunas).
        - where (str, opcional): Cláusula WHERE para filtrar os dados. Padrão é None.

    Métodos Públicos:
        - run(): Executa a consulta SQL e retorna os resultados.

    Exemplo de Uso:
        >>> selector = Basic_select(table='tabela', columns=['col1', 'col2'], where='col1 = 42')
        >>> resultado = selector.run()
        >>> print(resultado)
        [(1, 'valor1'), (2, 'valor2'), ...]
    """
    def __init__(self, table: str, columns: list = None, where: str = None,):
        self.table = table
        self.columns = columns
        self.where = where
        self.query = self.body_query()


    def body_query(self):
        """
        Constrói e formata a consulta SQL com base nos parâmetros fornecidos.

        Retorna:
            str: Consulta SQL formatada.
        """
        query = f"""
            SELECT {', '.join(self.columns) if self.columns is not None else '*'} 
            FROM {self.table}
        """
        query += '\nWHERE ' + self.where if self.where else ''

        query_formated = [row.strip() for row in query.splitlines() if row.strip()]
        return '\n'.join(query_formated)
    

    def run(self):
        """
        Executa a consulta SQL e retorna os resultados.

        Retorna:
            list or None: Uma lista contendo os resultados da consulta ou None se a consulta não retornar dados.
        """
        return self.make_query(self.query)
    

class Basic_aggregate(Consume_bd):
    """
    Uma classe que herda de Consume_bd e fornece funcionalidades básicas de agregação em consultas SQL.

    Parâmetros do Construtor:
        - table (str): Nome da tabela no banco de dados.
        - col_aggregate (str): Nome da coluna na qual a operação de agregação será realizada.
        - where (str, opcional): Cláusula WHERE para filtrar os dados. Padrão é None.
        - cols_group (list, opcional): Lista de colunas para agrupar. Padrão é None.
        - round (int, opcional): Número de casas decimais para arredondar os resultados. Padrão é None.
        - order (str, opcional): Coluna para ordenar os resultados. Padrão é None.
        - append (str, opcional): Trecho adicional a ser adicionado ao final da consulta. Padrão é None.

    Nota:
        Ao fazer uma instância com o objetivo de utilizar o método run_count sem informar o atributo cols_group,
        é recomendado passar o asterisco "*" no atributo col_aggragate. Por exemplo:
        
        >>> aggregador = Basic_aggregate(table='tabela', col_aggragate='*', round=2)
        >>> resultado = aggregador.run_count()
        >>> print(resultado)
        [(1, 'valor1'), (2, 'valor2'), ...]

    Métodos Públicos:
        - run_sum(): Executa uma operação de soma na coluna especificada.
        - run_max(): Executa uma operação de máximo na coluna especificada.
        - run_min(): Executa uma operação de mínimo na coluna especificada.
        - run_avg(): Executa uma operação de média na coluna especificada.
        - run_count(): Executa uma operação de contagem na coluna especificada.

    Exemplo de Uso:
        >>> aggregador = Basic_aggregate(table='tabela', col_aggragate='coluna', cols_group=['col1', 'col2'], round=2, order='col1')
        >>> resultado = aggregador.run_avg()
        >>> print(resultado)
        [(1, 'valor1'), (2, 'valor2'), ...]
    """

    def __init__(self, table: str, col_aggregate: str, where: str = None, cols_group: list = None, round: int = None, order: str = None, append: str = None):
        self.opperation = ("replace", "as_replace")
        self.table = table
        self.col_aggregate = col_aggregate
        self.where = where
        self.cols_group = cols_group
        self.round = round
        self.order = order
        self.append = append
        self.query = self.body_query()

    def cols_string(self, part):
        """
        Retorna uma string formatada para a parte específica da consulta SQL.

        Parâmetros:
            - part (str): Parte da consulta (SELECT, WHERE, GROUP BY, ORDER BY).

        Retorna:
            str: String formatada para a parte específica da consulta SQL.
        """
        col_group_unpack = ', '.join(self.cols_group) if self.cols_group is not None else ''
        dict_strings = {
            'SELECT': col_group_unpack + ',' if col_group_unpack else '',
            'WHERE': 'WHERE ' + self.where if self.where else '',
            'GROUP BY': 'GROUP BY ' + col_group_unpack if col_group_unpack else '',
            'ORDER BY': f"ORDER BY {self.opperation[1]} {self.order}" if self.order else ''
        }
        return dict_strings[part]

    def body_query(self):
        """
        Constrói e formata a consulta SQL com base nos parâmetros fornecidos.

        Retorna:
            str: Consulta SQL formatada.
        """
        if self.round is not None:
            aggregate_function = f'ROUND({self.opperation[0]}({self.col_aggregate}),{self.round}) AS {self.opperation[1]}'
        else:
            aggregate_function = f'{self.opperation[0]}({self.col_aggregate}) AS {self.opperation[1]}'

        query = f"""
            SELECT {self.cols_string('SELECT')} {aggregate_function}
            FROM {self.table}
            {self.cols_string('WHERE')}
            {self.cols_string('GROUP BY')}
            {self.cols_string('ORDER BY')}
            {self.append if self.append else ''}
        """
        query_formated = [row.strip() for row in query.splitlines() if row.strip()]
        return '\n'.join(query_formated)

    def operations(aggregate, as_aggregate):
        """
        Decorador para definir a operação de agregação e o alias.

        Parâmetros:
            - aggregate (str): Operação de agregação (SUM, MAX, MIN, AVG, COUNT).
            - as_aggregate (str): Alias para a operação de agregação.

        Retorna:
            function: Função decorada.
        """
        def run_operations(func):
            def closure(self):
                self.opperation = (aggregate, as_aggregate)
                self.query = self.body_query()
                result = self.make_query(self.query)
                return result
            return closure
        return run_operations

    @operations("SUM", "total")
    def run_sum(self):
        """
        Executa uma operação de soma na coluna especificada.

        Retorna:
            list or None: Uma lista contendo os resultados da consulta ou None se a consulta não retornar dados.
        """
        return None

    @operations("MAX", "maximo")
    def run_max(self):
        """
        Executa uma operação de máximo na coluna especificada.

        Retorna:
            list or None: Uma lista contendo os resultados da consulta ou None se a consulta não retornar dados.
        """
        return None

    @operations("MIN", "minimo")
    def run_min(self):
        """
        Executa uma operação de mínimo na coluna especificada.

        Retorna:
            list or None: Uma lista contendo os resultados da consulta ou None se a consulta não retornar dados.
        """
        return None

    @operations("AVG", "media")
    def run_avg(self):
        """
        Executa uma operação de média na coluna especificada.

        Retorna:
            list or None: Uma lista contendo os resultados da consulta ou None se a consulta não retornar dados.
        """
        return None

    @operations("COUNT", "contagem")
    def run_count(self):
        """
        Executa uma operação de contagem na coluna especificada.

        Retorna:
            list or None: Uma lista contendo os resultados da consulta ou None se a consulta não retornar dados.
        """
        return None


class New_table_SUS(Consume_bd):
    """
    Uma classe que herda de Consume_bd e cria novas tabelas para o banco de dados chamado SUS.

    Schema:
        - ID (VARCHAR PRIMARY KEY);
        - DESCRICAO (VARCHAR)

    Parâmetros do Construtor:
        - name_table (str): Nome da nova tabela para o banco de dados SUS.
        - insert_rows (dict): Dicionário onde cada par chave-valor representa um registro a ser inserido na nova tabela.
          Exemplo: {'ID': 'DESCRICAO'}

    Métodos:
        - __init__(name_table: str, insert_rows: dict) -> None: Construtor da classe que cria uma nova tabela e insere registros nela.
        - create_table(): Cria a nova tabela no banco de dados SUS, se ela não existir.
        - add_rows(): Insere os registros fornecidos no dicionário na nova tabela.

    Exemplo de Uso:
        >>> nova_tabela = New_table_SUS("minha_tabela", {1: "descricao1", 2: "descricao2"})
    """
    def __init__(self, name_table:str, insert_rows:dict) -> None:
        self.name_table =  name_table
        self.insert_rows = insert_rows
        self.create_table()
        self.add_rows()
    
    def create_table(self):
        """
        Cria a tabela no banco de dados SUS, se ela ainda não existir.
        """
        self.make_query(f"""
                        CREATE TABLE IF NOT EXISTS {self.name_table}(
                        ID VARCHAR PRIMARY KEY,
                        descricao VARCHAR
                        )""")
        return None
    
    def add_rows(self):
        """
        Insere os registros fornecidos no dicionário na nova tabela.

        Este método itera sobre os registros fornecidos no dicionário de inserção e os insere na tabela recém-criada no banco de dados SUS.
        Cada chave-valor no dicionário representa um registro, onde a chave é o ID e o valor é a descrição.
        
        Raises:
            psycopg2.Error: Se ocorrer algum erro ao tentar inserir um registro na tabela, uma exceção do tipo psycopg2.Error será levantada.
        """
        for key, value in self.insert_rows.items():
            try:
                id_query = key if isinstance(key, int) else f"'{key}'"
                self.make_query(f"INSERT INTO {self.name_table} VALUES ({id_query},'{value}')")
            except psycopg2.Error as e:
                print(f"Não foi possível inserir a linha\n{e}")


    def update_null_values(self,relation):
        result = self.make_query(f"UPDATE {relation}\n"+
                                 f"SET {self.name_table} = 'NULL'\n"+
                                 f"WHERE {self.name_table} IS NULL OR {self.name_table} = '';")
        return result

    def make_constraint(self, relation):
        updated = self.update_null_values(relation)
        result = None  # Definindo uma valor padrão para result

        if updated:
            result = self.make_query(
                f"ALTER TABLE {relation}\n"+
                f"ADD CONSTRAINT fk_{self.name_table}\n"+
                f"FOREIGN KEY ({self.name_table}) REFERENCES {self.name_table}(id);"
            )
        return result


if __name__ == '__main__':
    pass
