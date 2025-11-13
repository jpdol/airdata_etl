import shutil

from airflow.sdk import dag, task
import os
import sys
import requests
from typing import Optional
from requests.auth import HTTPBasicAuth


class TurtleLoader:
    """
    Classe para carregar arquivos Turtle (.ttl) no Apache Jena Fuseki
    """

    def __init__(self, fuseki_url: str = "http://localhost:3030", dataset: str = "airdata",
                 auth_user: str = "admin", auth_pass: str = "admin123", verbose: bool = True):
        """
        Inicializa o loader com a URL do Fuseki e o dataset.

        Args:
            fuseki_url: URL base do servidor Fuseki (padrão: http://localhost:3030)
            dataset: Nome do dataset no Fuseki (padrão: ds)
        """
        self.fuseki_url = fuseki_url.rstrip('/')
        self.dataset = dataset
        self.data_endpoint = f"{self.fuseki_url}/{dataset}/data"
        self.auth = HTTPBasicAuth(auth_user, auth_pass) if auth_user and auth_pass else None
        self.verbose = verbose
        print('Instância da classe TurtleLoader criada!')
        print('informações do objeto:')
        print(f'{self.fuseki_url=}')
        print(f'{self.dataset=}')
        print(f'{self.data_endpoint=}')
        print(f'{self.verbose=}')

    def print(self, *args, **kwargs):
        if self.verbose:
            print(*args, **kwargs)

    def load_from_directory(self, dir_path: str, graph_uri: Optional[str] = None) -> dict:
        self.print(f'Arquivos serão carregados pelo diretório {dir_path}')

        # estrutura "total" de result = {
        #     'success': bool,
        #     'message': str,
        #     'status_code': int,
        #     'error': str,
        #     'traceback': str
        # }
        total_result = {
            'file_path': [],
            'success': [],
            'message': [],
            'status_code': [],
            'error': [],
            'traceback': []
        }
        for dir, _, file_names in os.walk(dir_path):
            for file_name in file_names:
                file_path = os.path.join(dir, file_name)
                self.print(f'Arquivo selecionado: {file_path}')
                result = self.load_from_file(file_path=file_path, graph_uri=graph_uri)
                self.print(result)

                total_result['file_path'].append(file_path)
                # Armazena os resultados de todas as inserções
                possible_fields = ['succes', 'message', 'status_code', 'error', 'traceback']
                for field in possible_fields:
                    if field in result.keys():
                        total_result[field].append(result[field])
                    else:
                        total_result[field].append(None)

        self.print('Arquivos carregados com sucesso, retornando resultados')
        return total_result

    def load_from_file(self, file_path: str, graph_uri: Optional[str] = None) -> dict:
        """
        Carrega um arquivo .ttl no Fuseki.

        Args:
            file_path: Caminho para o arquivo .ttl
            graph_uri: URI do grafo nomeado (opcional, se None usa o grafo padrão)

        Returns:
            dict com status da operação
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                ttl_content = file.read()
                self.print('Arquivo lido!')
            return self.load_from_string(ttl_content, graph_uri)

        except FileNotFoundError:
            return {
                "success": False,
                "message": f"Arquivo não encontrado: {file_path}"
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Erro ao ler arquivo: {str(e)}"
            }

    def load_from_string(self, ttl_content: str, graph_uri: Optional[str] = None) -> dict:
        """
        Carrega conteúdo Turtle (string) no Fuseki.

        Args:
            ttl_content: Conteúdo Turtle como string
            graph_uri: URI do grafo nomeado (opcional)

        Returns:
            dict com status da operação
        """
        self.print(f'String lida {ttl_content[:300]}')
        headers = {
            'Content-Type': 'text/turtle; charset=utf-8'
        }

        # Se especificar graph_uri, usa named graph
        params = {}
        if graph_uri:
            params['graph'] = graph_uri

        try:
            self.print('Fazendo a requisição!')
            response = requests.post(
                self.data_endpoint,
                data=ttl_content.encode('utf-8'),
                headers=headers,
                params=params,
                auth=self.auth
            )

            if response.status_code in [200, 201, 204]:
                self.print('Dados carregados com sucesso!')
                return {
                    "success": True,
                    "message": "Dados carregados com sucesso",
                    "status_code": response.status_code
                }
            else:
                self.print(f'Código de resposta não positivo. Código: {response.status_code}')
                return {
                    "success": False,
                    "message": f"Erro ao carregar dados: {response.text}",
                    "status_code": response.status_code
                }

        except requests.exceptions.ConnectionError as e:
            self.print('Não foi possivel conectar ao Fuseki')
            return {
                "success": False,
                "message": "Não foi possível conectar ao Fuseki. Verifique se está rodando.",
                "error": str(e)
            }
        except Exception as e:
            self.print('Erro inesperado!!')
            import traceback
            return {
                "success": False,
                "message": f"Erro inesperado: {str(e)}",
                "error": str(e),
                "traceback": traceback.format_exc()
            }

    def clear_dataset(self, graph_uri: Optional[str] = None) -> dict:
        """
        Limpa todos os dados do dataset ou de um grafo específico usando SPARQL UPDATE.

        Args:
            graph_uri: URI do grafo para limpar (se None, limpa o grafo padrão)

        Returns:
            dict com status da operação
        """
        # Endpoint de update (não de data)
        update_endpoint = f"{self.fuseki_url}/{self.dataset}/update"

        # Construir a query SPARQL DELETE apropriada
        if graph_uri:
            # Limpar um grafo nomeado específico
            self.print('Limpando um grafo especifico')
            sparql_update = f"""
            DELETE WHERE {{
                GRAPH <{graph_uri}> {{
                    ?s ?p ?o .
                }}
            }}
            """
        else:
            # Limpar o grafo padrão (default graph)
            sparql_update = """
            DELETE WHERE {
                ?s ?p ?o .
            }
            """

        headers = {
            'Content-Type': 'application/sparql-update'
        }

        try:
            self.print('Realizando a requisição da limpeza do dataset')
            self.print(f'Query:\n{sparql_update}')
            response = requests.post(
                update_endpoint,
                data=sparql_update.encode('utf-8'),
                headers=headers,
                auth=self.auth
            )

            if response.status_code in [200, 204]:
                if graph_uri:
                    msg = f"Grafo <{graph_uri}> limpo com sucesso"
                else:
                    msg = "Dataset (grafo padrão) limpo com sucesso"
                self.print(f'{msg}')
                return {
                    "success": True,
                    "message": msg
                }
            else:
                self.print(f"Erro ao limpar dataset: {response.text}")
                return {
                    "success": False,
                    "message": f"Erro ao limpar dataset: {response.text}",
                    "status_code": response.status_code
                }

        except Exception as e:
            import traceback
            return {
                "success": False,
                "message": f"Erro ao limpar dataset ({type(e)}): {str(e)}",
                "error": str(e),
                "traceback": traceback.format_exc()
            }


@dag(dag_id='turtle_processing', schedule='0 6 * * *', max_active_runs=1)
def turtle_insertion():
    # from JENA_FUSEKI.TurtleLoader import TurtleLoader
    # TODO entender melhor como funcionam volumes e pastas dentro de um container (aparentemente não é o mesmo do que o computador)
    new_ttls_dir = "/opt/airflow/turtles/new_ttls"
    processed_ttls_dir = "/opt/airflow/turtles/processed_ttls"
    jena_fuseki_database = "airdata"

    os.makedirs(new_ttls_dir, exist_ok=True)
    os.makedirs(processed_ttls_dir, exist_ok=True)

    auth_user = "admin"
    auth_pass = "admin123"

    @task
    def insert_turtles(input_dir: str) -> dict:
        if not os.listdir(input_dir):
            print('Diretório vazio.')
            return {}

        tl = TurtleLoader(
            fuseki_url="http://localhost:3030",
            dataset=jena_fuseki_database,
            auth_user=auth_user,
            auth_pass=auth_pass,
            verbose=True
        )

        responses = tl.load_from_directory(
            dir_path=input_dir
        )
        return responses

    @task
    def move_files(output_dir: str, responses: dict):
        if not responses:
            print('Sem nenhum resultado para processar')
            return

        # Códigos de cores ANSI
        RED = "\033[91m"
        GREEN = "\033[92m"
        YELLOW = "\033[93m"
        CYAN = "\033[96m"
        RESET = "\033[0m"
        BOLD = "\033[1m"

        print(CYAN + '-' * 40 + RESET)
        print(BOLD + 'COMEÇANDO ANÁLISE DE RESULTADOS DOS TTL INSERIDOS:' + RESET)
        print(CYAN + '-' * 40 + RESET)

        for i, [file_path, success, message, status_code, error, traceback] in enumerate(responses.values()):
            color = GREEN if success else RED
            status_text = "SUCESSO" if success else "FALHA"

            print(f"{BOLD}{color}→ Resultado {i + 1}: {status_text}{RESET}")
            print(f'Caminho do arquivo: {file_path}')
            print(f'Status HTTP: {status_code}')
            print(f'Mensagem:\n{message}')

            if error:
                print(f'{RED}Erro: {error}{RESET}')
            if traceback:
                print(f'{YELLOW}Traceback: {traceback}{RESET}')

            if success:
                print(f'{GREEN}Instâncias inseridas com sucesso! Movendo arquivo para: {output_dir}{RESET}')
                try:
                    os.makedirs(output_dir, exist_ok=True)
                    novo_caminho = shutil.move(file_path, output_dir)
                    print(f'{GREEN}Arquivo movido para: {novo_caminho}{RESET}')
                except Exception as e:
                    print(f'{RED}Erro ao mover o arquivo: {e}{RESET}')
            else:
                print(f'{YELLOW}Operação não concluída. Arquivo mantido em: {file_path}{RESET}')

            print(CYAN + '-' * 30 + RESET)
        print(CYAN + '-' * 40 + RESET)

    responses = insert_turtles(input_dir=new_ttls_dir)
    move_files(output_dir=processed_ttls_dir, responses=responses)


turtle_insertion()
