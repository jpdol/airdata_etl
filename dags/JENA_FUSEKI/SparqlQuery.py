import requests
from typing import List, Dict, Any, Optional

from requests.auth import HTTPBasicAuth


class SparqlQuery:
    """
    Classe para executar consultas SPARQL no Apache Jena Fuseki
    """

    def __init__(self, fuseki_url: str = "http://localhost:3030", dataset: str = "airdata",
                 auth_user: str = "admin", auth_pass: str = "admin123"):
        """
        Inicializa o executor de queries.

        Args:
            fuseki_url: URL base do servidor Fuseki (padrão: http://localhost:3030)
            dataset: Nome do dataset no Fuseki (padrão: ds)
        """
        self.fuseki_url = fuseki_url.rstrip('/')
        self.dataset = dataset
        self.query_endpoint = f"{self.fuseki_url}/{dataset}/query"
        self.update_endpoint = f"{self.fuseki_url}/{dataset}/update"
        self.auth = HTTPBasicAuth(auth_user, auth_pass) if auth_user and auth_pass else None
        print('Instância de SparqlQuery criada!')
        print('Informações do objeto:')
        print(f'{self.fuseki_url=}')
        print(f'{self.dataset=}')
        print(f'{self.query_endpoint=}')
        print(f'{self.update_endpoint=}')

    def select(self, query: str) -> Dict[str, Any]:
        """
        Executa uma query SELECT SPARQL.

        Args:
            query: Query SPARQL SELECT

        Returns:
            dict com resultados e metadados
        """
        headers = {
            'Accept': 'application/sparql-results+json'
        }

        params = {
            'query': query
        }

        try:
            print('Fazendo a operação SELECT')
            print(f'Query utilizada:\n{query}')
            response = requests.get(
                self.query_endpoint,
                params=params,
                headers=headers
            )

            if response.status_code == 200:
                data = response.json()
                return {
                    "success": True,
                    "results": data.get('results', {}).get('bindings', []),
                    "variables": data.get('head', {}).get('vars', []),
                    "count": len(data.get('results', {}).get('bindings', []))
                }
            else:
                return {
                    "success": False,
                    "message": f"Erro na query: {response.text}",
                    "status_code": response.status_code
                }

        except requests.exceptions.ConnectionError as e:
            return {
                "success": False,
                "message": "Não foi possível conectar ao Fuseki. Verifique se está rodando.",
                "error": str(e)
            }
        except Exception as e:
            import traceback
            return {
                "success": False,
                "message": f"Erro inesperado: {str(e)}",
                "error": str(e),
                "traceback": traceback.format_exc()
            }

    def ask(self, query: str) -> Dict[str, Any]:
        """
        Executa uma query ASK SPARQL (retorna booleano).

        Args:
            query: Query SPARQL ASK

        Returns:
            dict com resultado booleano
        """
        headers = {
            'Accept': 'application/sparql-results+json'
        }

        params = {
            'query': query
        }

        try:
            print('Fazendo a operação ASK')
            print(f'Query utilizada:\n{query}')
            response = requests.get(
                self.query_endpoint,
                params=params,
                headers=headers
            )

            if response.status_code == 200:
                data = response.json()
                return {
                    "success": True,
                    "result": data.get('boolean', False)
                }
            else:
                return {
                    "success": False,
                    "message": f"Erro na query: {response.text}",
                    "status_code": response.status_code
                }

        except Exception as e:
            import traceback
            return {
                "success": False,
                "message": f"Erro inesperado: {str(e)}",
                "error": str(e),
                "traceback": traceback.format_exc()
            }

    def construct(self, query: str) -> Dict[str, Any]:
        """
        Executa uma query CONSTRUCT SPARQL (retorna grafo RDF).

        Args:
            query: Query SPARQL CONSTRUCT

        Returns:
            dict com grafo resultante em formato Turtle
        """
        headers = {
            'Accept': 'text/turtle'
        }

        params = {
            'query': query
        }

        try:
            print('Fazendo a operação CONSTRUCT')
            print(f'Query utilizada:\n{query}')
            response = requests.get(
                self.query_endpoint,
                params=params,
                headers=headers
            )

            if response.status_code == 200:
                return {
                    "success": True,
                    "graph": response.text,
                    "format": "turtle"
                }
            else:
                return {
                    "success": False,
                    "message": f"Erro na query: {response.text}",
                    "status_code": response.status_code
                }

        except Exception as e:
            return {
                "success": False,
                "message": f"Erro inesperado: {str(e)}"
            }

    def update(self, query: str) -> Dict[str, Any]:
        """
        Executa uma operação SPARQL UPDATE (INSERT, DELETE, etc).

        Args:
            query: Query SPARQL UPDATE

        Returns:
            dict com status da operação
        """
        headers = {
            'Content-Type': 'application/sparql-update'
        }

        try:
            print('Fazendo a operação UPDATE')
            print(f'Query utilizada:\n{query}')
            response = requests.post(
                self.update_endpoint,
                data=query.encode('utf-8'),
                headers=headers,
                auth=self.auth
            )

            if response.status_code in [200, 204]:
                return {
                    "success": True,
                    "message": "Update executado com sucesso"
                }
            else:
                return {
                    "success": False,
                    "message": f"Erro no update: {response.text}",
                    "status_code": response.status_code
                }

        except Exception as e:
            return {
                "success": False,
                "message": f"Erro inesperado: {str(e)}"
            }

    def get_all_triples(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Recupera todas as triplas do dataset (útil para testes).

        Args:
            limit: Número máximo de resultados (opcional)

        Returns:
            dict com todas as triplas
        """
        limit_clause = f"LIMIT {limit}" if limit else ""

        query = f"""
        SELECT ?subject ?predicate ?object
        WHERE {{
            ?subject ?predicate ?object .
        }}
        {limit_clause}
        """

        print('Obtendo todas as triplas')
        print(f'Query utilizada:\n{query}')

        return self.select(query)


# Exemplo de uso
if __name__ == "__main__":
    # Inicializa o executor
    sparql = SparqlQuery()

    # # Exemplo 1: SELECT simples
    # query1 = """
    # PREFIX ex: <http://example.org/>
    #
    # SELECT ?pessoa ?nome ?idade
    # WHERE {
    #     ?pessoa a ex:Pessoa ;
    #             ex:nome ?nome ;
    #             ex:idade ?idade .
    # }
    # """
    # result = sparql.select(query1)
    # if result['success']:
    #     print(f"Encontradas {result['count']} pessoas:")
    #     for row in result['results']:
    #         print(f"- {row['nome']['value']}, {row['idade']['value']} anos")
    #
    # # Exemplo 2: ASK - verificar se existe algo
    # query2 = """
    # PREFIX ex: <http://example.org/>
    #
    # ASK {
    #     ?pessoa ex:nome "João Silva" .
    # }
    # """
    # result = sparql.ask(query2)
    # print(f"João Silva existe? {result['result']}")
    #
    # # Exemplo 3: UPDATE - inserir dados
    # update_query = """
    # PREFIX ex: <http://example.org/>
    #
    # INSERT DATA {
    #     ex:pessoa2 a ex:Pessoa ;
    #                ex:nome "Maria Santos" ;
    #                ex:idade 25 .
    # }
    # """
    # result = sparql.update(update_query)
    # print(result['message'])

    # Exemplo 4: Ver todas as triplas (limitado a 10)
    result = sparql.get_all_triples()
    print(f"\nTotal de triplas recuperadas: {result['count']}")
