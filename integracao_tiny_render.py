# -*- coding: utf-8 -*-
"""
Script Python de exemplo para integrar o ERP Olist Tiny com um banco de dados PostgreSQL no Render.

Este script demonstra:
1. Como obter um token de acesso da API do Tiny (processo simplificado, o fluxo OAuth2 completo
   envolve redirecionamento do usuário e obtenção de um código de autorização).
2. Como fazer uma requisição para um endpoint da API do Tiny (ex: listar produtos).
3. Como conectar a um banco de dados PostgreSQL hospedado no Render.
4. Como criar uma tabela (se não existir) e inserir/atualizar dados no PostgreSQL.

Lembre-se de que este é um exemplo didático. Para um ambiente de produção, você precisará:
- Implementar o fluxo OAuth2 completo para obtenção do código de autorização.
- Gerenciar o refresh token para manter o acesso à API.
- Tratar erros de forma robusta (exceções de rede, erros da API, erros do banco).
- Implementar lógica de paginação para buscar todos os dados da API do Tiny.
- Estruturar melhor o código em funções e classes.
- Armazenar credenciais de forma segura (NUNCA diretamente no código em produção).
- Considerar o agendamento da execução do script (ex: cron no Mac, Render Cron Jobs, etc.).
"""

import requests
import psycopg2
import json
import os # Para ler variáveis de ambiente (recomendado para credenciais)
import time

# --- Configurações da API v2 do Tiny ---
TINY_API_V2_BASE_URL = "https://api.tiny.com.br/api2"
TINY_API_V2_TOKEN = os.getenv("TINY_API_V2_TOKEN", "14c9d28dc8d8dcb2d7229efb1fafcf0392059b2579414c17d02eee4c821f4677")

# --- Configurações do Banco de Dados PostgreSQL no Render ---
# Substitua com suas credenciais do banco de dados obtidas no Render
# É ALTAMENTE RECOMENDADO usar variáveis de ambiente para estas credenciais.
DB_HOST = os.getenv("DB_HOST", "dpg-d0h4vjgdl3ps73cihsv0-a.oregon-postgres.render.com")
DB_NAME = os.getenv("DB_NAME", "banco_tiny_fusion")
DB_USER = os.getenv("DB_USER", "banco_tiny_fusion_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "9JFbOUjBU1f3tM10EZygXgtOp8vKoUyb")
DB_PORT = os.getenv("DB_PORT", "5432") # Geralmente 5432 para PostgreSQL


def listar_produtos_tiny_v2(api_token, pagina=1, pesquisa=""):
    """
    Busca uma lista de produtos da API v2 do Tiny.
    A API v2 do Tiny também usa paginação.
    Endpoint: https://api.tiny.com.br/api2/produtos.pesquisa.php
    Método: POST (comum em APIs mais antigas para enviar parâmetros no corpo)
    Parâmetros: token, formato, pesquisa, pagina
    """
    endpoint = f"{TINY_API_V2_BASE_URL}/produtos.pesquisa.php"
    # Na API v2, o token e outros parâmetros são enviados no corpo da requisição (formato x-www-form-urlencoded)
    payload = {
        "token": api_token,
        "formato": "json",
        "pesquisa": pesquisa, # Para listar todos, uma string vazia ou um curinga pode ser necessário
        "pagina": pagina
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    print(f"Buscando produtos da API v2 (página {pagina}, pesquisa: '{pesquisa}')...")
    print(f"Endpoint: {endpoint}")
    print(f"Payload: {payload}")

    try:
        response = requests.post(endpoint, data=payload, headers=headers)
        response.raise_for_status() # Lança exceção para respostas de erro (4xx ou 5xx)
        
        # Adicionar verificação do status_processamento e status no retorno
        response_data = response.json()
        if response_data.get("retorno", {}).get("status") == "Erro":
            print(f"Erro retornado pela API Tiny v2: {response_data.get('retorno', {}).get('erros')}")
            return None
        
        print(f"Produtos da página {pagina} (API v2) obtidos com sucesso!")
        return response_data # Retorna os dados dos produtos como um dicionário Python
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar produtos (API v2): {e}")
        if response is not None:
            print(f"Detalhes do erro (API v2): {response.text}")
        return None

def conectar_db():
    """Conecta ao banco de dados PostgreSQL e retorna o objeto de conexão."""
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        print("Conexão com o PostgreSQL bem-sucedida!")
    except (Exception, psycopg2.Error) as error:
        print(f"Erro ao conectar ao PostgreSQL: {error}")
    return conn

def criar_tabela_produtos(conn):
    """Cria a tabela 'produtos' se ela não existir."""
    try:
        cursor = conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS produtos (
            id_produto_tiny INT PRIMARY KEY,
            nome_produto VARCHAR(255) NOT NULL,
            sku VARCHAR(100) UNIQUE,
            preco_venda NUMERIC(10, 2),
            unidade VARCHAR(10),
            situacao VARCHAR(20),
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        print("Tabela 'produtos' verificada/criada com sucesso.")
    except (Exception, psycopg2.Error) as error:
        print(f"Erro ao criar tabela 'produtos': {error}")
    finally:
        if cursor:
            cursor.close()

def inserir_ou_atualizar_produto(conn, produto_tiny):
    """
    Insere um novo produto ou atualiza um existente na tabela 'produtos'.
    Usa a cláusula ON CONFLICT DO UPDATE (UPSERT) do PostgreSQL.
    """
    # Adapte os campos conforme a estrutura retornada pela API do Tiny para produtos
    # Este é um exemplo genérico.
    id_produto = produto_tiny.get("id")
    nome = produto_tiny.get("nome")
    sku = produto_tiny.get("codigo")  # 'codigo' é geralmente o SKU no Tiny

    # Adiciona verificação para SKU vazio ou nulo
    if not sku:
        print(f"AVISO: Produto ID Tiny {id_produto} (Nome: {nome}) está com SKU vazio. Este produto não será inserido/atualizado.")
        return

    preco = produto_tiny.get("preco")
    unidade = produto_tiny.get("unidade")
    situacao = produto_tiny.get("situacao") # Ex: 'A' para Ativo

    if not id_produto or not nome:
        print(f"Produto com dados incompletos, pulando: {produto_tiny.get('id')}")
        return

    try:
        cursor = conn.cursor()
        # A cláusula ON CONFLICT (id_produto_tiny) DO UPDATE SET ...
        # garante que, se um produto com o mesmo id_produto_tiny já existir,
        # ele será atualizado. Caso contrário, um novo será inserido.
        upsert_query = """
        INSERT INTO produtos (id_produto_tiny, nome_produto, sku, preco_venda, unidade, situacao, data_atualizacao)
        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (id_produto_tiny)
        DO UPDATE SET
            nome_produto = EXCLUDED.nome_produto,
            sku = EXCLUDED.sku,
            preco_venda = EXCLUDED.preco_venda,
            unidade = EXCLUDED.unidade,
            situacao = EXCLUDED.situacao,
            data_atualizacao = CURRENT_TIMESTAMP;
        """
        cursor.execute(upsert_query, (id_produto, nome, sku, preco, unidade, situacao))
        conn.commit()
        # print(f"Produto ID {id_produto} inserido/atualizado com sucesso.")
    except (Exception, psycopg2.Error) as error:
        print(f"Erro ao inserir/atualizar produto ID {id_produto}: {error}")
        conn.rollback() # Desfaz a transação em caso de erro
    finally:
        if cursor:
            cursor.close()

def main():
    """Função principal para orquestrar a integração."""
    print("Iniciando script de integração Tiny ERP (API v2) -> Render PostgreSQL...")

    # Conectar ao banco de dados primeiro
    conn = conectar_db()
    if not conn:
        print("Não foi possível conectar ao banco de dados. Saindo.")
        return

    criar_tabela_produtos(conn) # Garante que a tabela exista

    print(f"\nUsando API v2 do Tiny com o token: {TINY_API_V2_TOKEN[:10]}...")

    pagina_atual = 1
    total_registros_processados = 0

    while True:
        # Para a API v2, o parâmetro "pesquisa" é obrigatório.
        # Se deixado em branco, pode não retornar todos os produtos ou dar erro.
        # Uma pesquisa vazia "" ou com um curinga como "%" pode ser necessária para listar todos.
        # Vamos testar com uma string vazia para "pesquisa" inicialmente.
        # Se não funcionar, o usuário pode precisar especificar um termo de pesquisa ou usar "%" se a API suportar.
        dados_produtos_pagina = listar_produtos_tiny_v2(TINY_API_V2_TOKEN, pagina=pagina_atual, pesquisa="")

        if dados_produtos_pagina and dados_produtos_pagina.get("retorno", {}).get("status") == "OK":
            produtos_da_pagina = dados_produtos_pagina.get("retorno", {}).get("produtos", [])
            
            if not produtos_da_pagina and pagina_atual == 1:
                print("Nenhum produto encontrado na primeira página. Verifique o parâmetro 'pesquisa' ou se há produtos no Tiny.")
                break
            if not produtos_da_pagina:
                print("Não há mais produtos para buscar.")
                break

            print(f"Processando {len(produtos_da_pagina)} produtos da página {pagina_atual}...")
            for item_produto in produtos_da_pagina:
                produto = item_produto.get("produto") # Na API v2, os dados do produto estão aninhados
                if produto:
                    inserir_ou_atualizar_produto(conn, produto)
                    total_registros_processados += 1
            
            numero_total_paginas = int(dados_produtos_pagina.get("retorno", {}).get("numero_paginas", 0))
            if pagina_atual >= numero_total_paginas:
                print("Todas as páginas de produtos foram processadas.")
                break
            pagina_atual += 1
            time.sleep(1) # Pequena pausa para não sobrecarregar a API
        else:
            print("Falha ao buscar produtos da API v2 ou resposta inesperada. Verifique os logs.")
            if dados_produtos_pagina:
                print(f"Resposta da API (detalhes):\n{json.dumps(dados_produtos_pagina, indent=2, ensure_ascii=False)}")
            break

    print(f"\nTotal de {total_registros_processados} registros de produtos processados.")

    # Fechar a conexão com o banco de dados
    if conn:
        conn.close()
        print("Conexão com o PostgreSQL fechada.")

    # --- Conexão com o Banco de Dados ---
    # conn_pg = conectar_db() # Removido pois a conexão já é feita no início do main
    # if not conn_pg:
    # return # Sai se não conseguir conectar ao banco

    # --- Criação/Verificação da Tabela no Banco ---
    # criar_tabela_produtos(conn_pg) # Removido pois a criação já é feita no início do main

    # --- Busca e Insere/Atualiza Dados do Tiny ---
    # print("\nBuscando produtos do Tiny ERP...") # Removido, lógica já está no loop principal da v2
    # pagina_atual_v3 = 1 # Variável da v3, removida
    # produtos_processados_total_v3 = 0 # Variável da v3, removida

    # while True: # Loop da v3, removido
        # dados_produtos_api = listar_produtos_tiny(TINY_ACCESS_TOKEN, pagina=pagina_atual_v3)

        # if dados_produtos_api and dados_produtos_api.get("status") == "OK":
            # produtos_da_pagina = dados_produtos_api.get("retorno", {}).get("produtos", [])
            # if not produtos_da_pagina:
                # print("Não há mais produtos para buscar.")
                # break # Sai do loop se não houver mais produtos

            # print(f"Processando {len(produtos_da_pagina)} produtos da página {pagina_atual_v3}...")
            # for item in produtos_da_pagina:
                # produto = item.get("produto") # A estrutura da API do Tiny pode aninhar o produto
                # if produto:
                    # inserir_ou_atualizar_produto(conn_pg, produto)
                    # produtos_processados_total_v3 += 1
            
            # Verifica se há mais páginas
            # A API do Tiny pode indicar isso de diferentes formas, ajuste conforme a documentação
            # Exemplo: se o número de produtos retornados for menor que o limite por página, ou se um campo específico indicar.
            # Para este exemplo, vamos assumir que se a lista de produtos estiver vazia, não há mais páginas.
            # Uma forma mais robusta seria verificar um campo como `dados_produtos_api.get("retorno", {}).get("numero_paginas")`
            # e comparar com `pagina_atual_v3`.
            # if len(produtos_da_pagina) < 20: # Supondo um limite de 20 produtos por página (verificar na API)
                 # print("Fim da paginação (suposição baseada no número de itens).")
                 # break

            # pagina_atual_v3 += 1
            # time.sleep(1) # Adiciona um pequeno delay para não sobrecarregar a API
        # else:
            # print("Falha ao buscar produtos ou status da API não é OK (v3).")
            # if dados_produtos_api:
                # print(f"Resposta da API (v3): {dados_produtos_api}")
            # break # Sai do loop em caso de erro
    
    # print(f"\nTotal de {produtos_processados_total_v3} registros de produtos processados (v3).")

    # if conn_pg:
        # conn_pg.close()
        # print("Conexão com o PostgreSQL (v3) fechada.")

if __name__ == "__main__":
    main()

