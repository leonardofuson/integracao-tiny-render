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
from datetime import datetime

# --- Configurações da API v2 do Tiny ---
TINY_API_V2_BASE_URL = "https://api.tiny.com.br/api2"
TINY_API_V2_TOKEN = os.getenv("TINY_API_V2_TOKEN", "14c9d28dc8d8dcb2d7229efb1fafcf0392059b2579414c17d02eee4c821f4677")

# --- Configurações do Banco de Dados PostgreSQL no Render ---
DB_HOST = os.getenv("DB_HOST", "dpg-d0h4vjgdl3ps73cihsv0-a.oregon-postgres.render.com")
DB_NAME = os.getenv("DB_NAME", "banco_tiny_fusion")
DB_USER = os.getenv("DB_USER", "banco_tiny_fusion_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "9JFbOUjBU1f3tM10EZygXgtOp8vKoUyb")
DB_PORT = os.getenv("DB_PORT", "5432") # Geralmente 5432 para PostgreSQL


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

def criar_tabela_categorias(conn):
    """Cria a tabela 'categorias' se ela não existir."""
    cursor = None
    try:
        cursor = conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS categorias (
            id_categoria_tiny INT PRIMARY KEY,
            nome_categoria VARCHAR(255) NOT NULL,
            id_categoria_pai_tiny INT,
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (id_categoria_pai_tiny) REFERENCES categorias (id_categoria_tiny) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS idx_categorias_id_categoria_pai_tiny ON categorias (id_categoria_pai_tiny);
        """
        cursor.execute(create_table_query)
        conn.commit()
        print("Tabela 'categorias' verificada/criada com sucesso.")
    except (Exception, psycopg2.Error) as error:
        print(f"Erro ao criar tabela 'categorias': {error}")
        if conn:
            try: conn.rollback(); print("Rollback da transação da tabela categorias realizado.")
            except psycopg2.InterfaceError as ie: print(f"Erro ao tentar rollback na tabela categorias: {ie}")
    finally:
        if cursor:
            cursor.close()

def criar_tabela_produtos(conn):
    """Cria a tabela 'produtos' se ela não existir, garantindo todas as colunas detalhadas e FK para categorias."""
    cursor = None
    try:
        cursor = conn.cursor()
        # Criação base da tabela, se não existir
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS produtos (
            id_produto_tiny INT PRIMARY KEY,
            nome_produto VARCHAR(255) NOT NULL,
            sku VARCHAR(100) UNIQUE,
            preco_venda NUMERIC(12, 2),
            unidade VARCHAR(20),
            situacao VARCHAR(50),
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        conn.commit()

        # Verificar e adicionar colunas detalhadas se não existirem
        colunas_para_verificar = {
            "preco_custo": "NUMERIC(12, 2)",
            "tipo_produto": "VARCHAR(50)",
            "estoque_atual": "NUMERIC(10, 3)",
            "id_categoria_produto_tiny": "INT"
        }

        for nome_coluna, tipo_coluna in colunas_para_verificar.items():
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name='produtos' AND column_name=%s", (nome_coluna,))
            if not cursor.fetchone():
                print(f"Adicionando coluna '{nome_coluna}' à tabela 'produtos'...")
                cursor.execute(f"ALTER TABLE produtos ADD COLUMN {nome_coluna} {tipo_coluna};")
                conn.commit()
                print(f"Coluna '{nome_coluna}' adicionada.")

        # Verificar e adicionar FK para categorias
        fk_name = 'fk_produtos_to_categorias'
        cursor.execute("""
            SELECT constraint_name FROM information_schema.table_constraints
            WHERE table_schema = 'public' AND table_name='produtos' AND constraint_name=%s AND constraint_type='FOREIGN KEY'
        """, (fk_name,))
        if not cursor.fetchone():
            print(f"Adicionando FK '{fk_name}' à tabela 'produtos'...")
            cursor.execute(f"""
                ALTER TABLE produtos
                ADD CONSTRAINT {fk_name}
                FOREIGN KEY (id_categoria_produto_tiny)
                REFERENCES categorias (id_categoria_tiny) ON DELETE SET NULL;
            """)
            conn.commit()
            print(f"FK '{fk_name}' adicionada.")

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_produtos_id_categoria_produto_tiny ON produtos (id_categoria_produto_tiny);")
        conn.commit()
        
        print("Tabela 'produtos' (detalhada) verificada/criada/atualizada com sucesso.")

    except (Exception, psycopg2.Error) as error:
        print(f"Erro ao criar/atualizar tabela 'produtos' (detalhada): {error}")
        if conn:
            try: conn.rollback(); print("Rollback da transação da tabela produtos realizado.")
            except psycopg2.InterfaceError as ie: print(f"Erro ao tentar rollback na tabela produtos: {ie}")
    finally:
        if cursor:
            cursor.close()

def criar_tabela_vendedores(conn):
    """Cria a tabela 'vendedores' se ela não existir."""
    cursor = None
    try:
        cursor = conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS vendedores (
            id_vendedor_tiny INT PRIMARY KEY,
            nome_vendedor VARCHAR(255) NOT NULL,
            situacao_vendedor VARCHAR(50),
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        print("Tabela 'vendedores' verificada/criada com sucesso.")
    except (Exception, psycopg2.Error) as error:
        print(f"Erro ao criar tabela 'vendedores': {error}")
        if conn:
            try: conn.rollback(); print("Rollback da transação da tabela vendedores realizado.")
            except psycopg2.InterfaceError as ie: print(f"Erro ao tentar rollback na tabela vendedores: {ie}")
    finally:
        if cursor:
            cursor.close()

def criar_tabela_pedidos(conn):
    """Cria a tabela 'pedidos' se ela não existir, com FK para vendedores."""
    cursor = None
    try:
        cursor = conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS pedidos (
            id_pedido_tiny INT PRIMARY KEY,
            numero_pedido_tiny VARCHAR(50),
            data_pedido DATE,
            id_lista_preco VARCHAR(50),
            descricao_lista_preco VARCHAR(255),
            nome_cliente VARCHAR(255),
            cpf_cnpj_cliente VARCHAR(20),
            email_cliente VARCHAR(255),
            fone_cliente VARCHAR(50),
            id_vendedor INT,
            nome_vendedor_pedido VARCHAR(255), -- Mantido para referência, mas a FK é para id_vendedor
            situacao_pedido VARCHAR(50),
            valor_total_pedido NUMERIC(12, 2),
            valor_desconto_pedido NUMERIC(12, 2),
            condicao_pagamento VARCHAR(255),
            forma_pagamento VARCHAR(100),
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_query)
        conn.commit()

        fk_name_pedidos_vendedores = 'fk_pedidos_to_vendedores'
        cursor.execute("""
            SELECT constraint_name FROM information_schema.table_constraints
            WHERE table_schema = 'public' AND table_name='pedidos' AND constraint_name=%s AND constraint_type='FOREIGN KEY'
        """, (fk_name_pedidos_vendedores,))
        if not cursor.fetchone():
            print(f"Adicionando FK '{fk_name_pedidos_vendedores}' à tabela 'pedidos'...")
            cursor.execute(f"""
                ALTER TABLE pedidos
                ADD CONSTRAINT {fk_name_pedidos_vendedores}
                FOREIGN KEY (id_vendedor)
                REFERENCES vendedores (id_vendedor_tiny) ON DELETE SET NULL;
            """)
            conn.commit()
            print(f"FK '{fk_name_pedidos_vendedores}' adicionada.")
        
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pedidos_id_vendedor ON pedidos (id_vendedor);")
        conn.commit()

        print("Tabela 'pedidos' verificada/criada/atualizada com sucesso.")
    except (Exception, psycopg2.Error) as error:
        print(f"Erro ao criar/atualizar tabela 'pedidos': {error}")
        if conn:
            try: conn.rollback(); print("Rollback da transação da tabela pedidos realizado.")
            except psycopg2.InterfaceError as ie: print(f"Erro ao tentar rollback na tabela pedidos: {ie}")
    finally:
        if cursor:
            cursor.close()

def criar_tabela_itens_pedido(conn):
    """Cria a tabela 'itens_pedido' se ela não existir."""
    cursor = None
    try:
        cursor = conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS itens_pedido (
            id_item_pedido SERIAL PRIMARY KEY,
            id_pedido_tiny INT NOT NULL,
            id_produto_tiny INT,
            sku_produto VARCHAR(100),
            descricao_item VARCHAR(255),
            unidade_item VARCHAR(20),
            quantidade_item NUMERIC(10, 3),
            valor_unitario_item NUMERIC(12, 2),
            valor_total_item NUMERIC(12, 2),
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (id_pedido_tiny) REFERENCES pedidos (id_pedido_tiny) ON DELETE CASCADE,
            FOREIGN KEY (id_produto_tiny) REFERENCES produtos (id_produto_tiny) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS idx_itens_pedido_id_pedido_tiny ON itens_pedido (id_pedido_tiny);
        CREATE INDEX IF NOT EXISTS idx_itens_pedido_id_produto_tiny ON itens_pedido (id_produto_tiny);
        CREATE INDEX IF NOT EXISTS idx_itens_pedido_sku_produto ON itens_pedido (sku_produto);
        """
        cursor.execute(create_table_query)
        conn.commit()
        print("Tabela 'itens_pedido' verificada/criada com sucesso.")
    except (Exception, psycopg2.Error) as error:
        print(f"Erro ao criar tabela 'itens_pedido': {error}")
        if conn:
            try: conn.rollback(); print("Rollback da transação da tabela itens_pedido realizado.")
            except psycopg2.InterfaceError as ie: print(f"Erro ao tentar rollback na tabela itens_pedido: {ie}")
    finally:
        if cursor:
            cursor.close()

def format_date_to_db(date_str):
    if not date_str: return None
    try: return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError: print(f"AVISO: Formato de data inválido: {date_str}."); return None

# --- Funções para Categorias ---
def obter_arvore_categorias_tiny_v2(api_token):
    """Busca a árvore de categorias da API v2 do Tiny."""
    endpoint = f"{TINY_API_V2_BASE_URL}/produtos.categorias.arvore.php"
    payload = {"token": api_token, "formato": "json"}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    print("Buscando árvore de categorias da API v2...")
    response = None
    try:
        response = requests.post(endpoint, data=payload, headers=headers)
        response.raise_for_status()
        response_data = response.json()

        if isinstance(response_data, dict):
            retorno_geral = response_data.get("retorno")
            if retorno_geral is None:
                print(f"API Tiny v2 (categorias): Chave 'retorno' não encontrada na resposta: {response_data}")
                return None

            if isinstance(retorno_geral, list):
                print("API Tiny v2 (categorias): 'retorno' é uma lista, processando diretamente.")
                return retorno_geral
            elif isinstance(retorno_geral, dict):
                if retorno_geral.get("status") == "Erro":
                    print(f"Erro API Tiny v2 (categorias) no 'retorno' dict: {retorno_geral.get('erros')}")
                    return None
                if "categorias" in retorno_geral and retorno_geral.get("status") == "OK":
                    return retorno_geral.get("categorias", [])
                else:
                    print(f"API Tiny v2 (categorias) 'retorno' dict status não OK ou falta 'categorias': {retorno_geral}")
                    return None
            else:
                print(f"API Tiny v2 (categorias): Conteúdo de 'retorno' não é lista nem dicionário: {type(retorno_geral)}")
                return None
        elif isinstance(response_data, list):
            print("API Tiny v2 (categorias): Resposta raiz é uma lista, processando diretamente.")
            return response_data
        else:
            print(f"Tipo de resposta raiz inesperado da API Tiny v2 (categorias): {type(response_data)} - {response_data}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Erro de requisição ao buscar árvore de categorias (API v2): {e}")
        if response is not None: print(f"Detalhes do erro (API v2 categorias): {response.text}")
        return None
    except json.JSONDecodeError:
        print(f"Erro ao decodificar JSON da API Tiny v2 (categorias). Resposta: {response.text if response else 'N/A'}")
        return None

def _inserir_ou_atualizar_categoria_recursivamente(cursor, categoria_api, id_pai=None):
    """Função auxiliar para inserir/atualizar uma categoria e suas subcategorias usando um cursor existente."""
    id_categoria = categoria_api.get("id")
    descricao = categoria_api.get("descricao")
    if not id_categoria or not descricao:
        print(f"AVISO: Categoria com ID ou descrição ausente, pulando: {categoria_api}")
        return

    upsert_query = """
    INSERT INTO categorias (id_categoria_tiny, nome_categoria, id_categoria_pai_tiny, data_atualizacao)
    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (id_categoria_tiny)
    DO UPDATE SET
        nome_categoria = EXCLUDED.nome_categoria,
        id_categoria_pai_tiny = EXCLUDED.id_categoria_pai_tiny,
        data_atualizacao = CURRENT_TIMESTAMP;
    """
    cursor.execute(upsert_query, (id_categoria, descricao, id_pai))
    
    sub_categorias = categoria_api.get("nodes", [])
    if sub_categorias:
        for sub_categoria_api in sub_categorias:
            _inserir_ou_atualizar_categoria_recursivamente(cursor, sub_categoria_api, id_pai=id_categoria)

def processar_e_inserir_categorias(conn, categorias_api):
    """Processa a lista de categorias da API e as insere no banco."""
    if not categorias_api: 
        print("Nenhuma categoria API fornecida para processamento.")
        return
    cursor = None
    try:
        cursor = conn.cursor()
        for categoria_raiz_api in categorias_api:
            _inserir_ou_atualizar_categoria_recursivamente(cursor, categoria_raiz_api, id_pai=None)
        conn.commit()
        print("Todas as categorias processadas e inseridas/atualizadas com sucesso.")
    except (Exception, psycopg2.Error) as error:
        print(f"Erro ao processar/inserir categorias: {error}")
        if conn: conn.rollback()
    finally:
        if cursor:
            cursor.close()

# --- Funções para Produtos ---
def listar_produtos_tiny_v2(api_token, pagina=1):
    """Busca uma página de produtos da API v2 do Tiny."""
    endpoint = f"{TINY_API_V2_BASE_URL}/produtos.pesquisa.php"
    payload = {"token": api_token, "formato": "json", "pagina": pagina}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    print(f"Buscando lista de produtos da API v2 (página {pagina})...")
    response = None
    try:
        response = requests.post(endpoint, data=payload, headers=headers)
        response.raise_for_status()
        response_data = response.json()
        if response_data.get("retorno", {}).get("status") == "Erro":
            print(f"Erro API Tiny (listar produtos): {response_data.get('retorno', {}).get('erros')}")
            return None
        return response_data
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar lista de produtos (API v2): {e}")
        if response is not None: print(f"Detalhes: {response.text}")
        return None
    except json.JSONDecodeError:
        print(f"Erro ao decodificar JSON da API Tiny v2 (listar produtos). Resposta: {response.text if response else 'N/A'}")
        return None

def obter_detalhes_produto_tiny_v2(api_token, id_produto_tiny):
    """Busca detalhes de um produto específico da API v2 do Tiny."""
    endpoint = f"{TINY_API_V2_BASE_URL}/produto.obter.php"
    payload = {"token": api_token, "formato": "json", "id": id_produto_tiny}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = None
    try:
        response = requests.post(endpoint, data=payload, headers=headers)
        response.raise_for_status()
        response_data = response.json()
        if response_data.get("retorno", {}).get("status") == "Erro":
            print(f"Erro API Tiny (obter produto ID {id_produto_tiny}): {response_data.get('retorno', {}).get('erros')}")
            return None
        return response_data.get("retorno", {}).get("produto") # Retorna o objeto do produto diretamente
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar detalhes do produto ID {id_produto_tiny} (API v2): {e}")
        if response is not None: print(f"Detalhes: {response.text}")
        return None
    except json.JSONDecodeError:
        print(f"Erro ao decodificar JSON da API Tiny v2 (obter produto ID {id_produto_tiny}). Resposta: {response.text if response else 'N/A'}")
        return None

def inserir_ou_atualizar_produto_detalhado(conn, produto_api):
    cursor = None
    try:
        cursor = conn.cursor()
        id_produto = produto_api.get("id")
        nome = produto_api.get("nome")
        sku = produto_api.get("codigo") # SKU
        preco_venda = produto_api.get("preco")
        preco_custo = produto_api.get("preco_custo")
        unidade = produto_api.get("unidade")
        situacao = produto_api.get("situacao")
        tipo_produto = produto_api.get("tipo") # Ex: 'P' (Produto), 'S' (Serviço), 'N' (NCM)
        estoque_atual = produto_api.get("saldo") # Estoque atual
        id_categoria = produto_api.get("id_categoria") # ID da categoria do produto

        if not id_produto or not nome:
            print(f"AVISO: Produto com ID ou nome ausente, pulando: {produto_api.get('id')}, {produto_api.get('nome')}")
            return
        
        sku = sku if sku and sku.strip() else None

        upsert_query = """
        INSERT INTO produtos (id_produto_tiny, nome_produto, sku, preco_venda, preco_custo, unidade, situacao, tipo_produto, estoque_atual, id_categoria_produto_tiny, data_atualizacao)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (id_produto_tiny)
        DO UPDATE SET
            nome_produto = EXCLUDED.nome_produto,
            sku = EXCLUDED.sku,
            preco_venda = EXCLUDED.preco_venda,
            preco_custo = EXCLUDED.preco_custo,
            unidade = EXCLUDED.unidade,
            situacao = EXCLUDED.situacao,
            tipo_produto = EXCLUDED.tipo_produto,
            estoque_atual = EXCLUDED.estoque_atual,
            id_categoria_produto_tiny = EXCLUDED.id_categoria_produto_tiny,
            data_atualizacao = CURRENT_TIMESTAMP;
        """
        cursor.execute(upsert_query, (id_produto, nome, sku, preco_venda, preco_custo, unidade, situacao, tipo_produto, estoque_atual, id_categoria))
        conn.commit()
    except psycopg2.IntegrityError as e:
        print(f"Erro de integridade ao inserir/atualizar produto ID {id_produto} (SKU: {sku}): {e}")
        if conn: conn.rollback()
    except (Exception, psycopg2.Error) as error:
        print(f"Erro ao inserir/atualizar produto ID {id_produto}: {error}")
        if conn: conn.rollback()
    finally:
        if cursor:
            cursor.close()

# --- Funções para Vendedores ---
def listar_vendedores_tiny_v2(api_token, pagina=1):
    """Busca uma página de vendedores da API v2 do Tiny."""
    endpoint = f"{TINY_API_V2_BASE_URL}/vendedores.pesquisa.php"
    payload = {"token": api_token, "formato": "json", "pagina": pagina}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    print(f"Buscando lista de vendedores da API v2 (página {pagina})...")
    response = None
    try:
        response = requests.post(endpoint, data=payload, headers=headers)
        response.raise_for_status()
        response_data = response.json()
        if response_data.get("retorno", {}).get("status") == "Erro":
            print(f"Erro API Tiny (listar vendedores): {response_data.get('retorno', {}).get('erros')}")
            return None
        return response_data
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar lista de vendedores (API v2): {e}")
        if response is not None: print(f"Detalhes: {response.text}")
        return None
    except json.JSONDecodeError:
        print(f"Erro ao decodificar JSON da API Tiny v2 (listar vendedores). Resposta: {response.text if response else 'N/A'}")
        return None

def inserir_ou_atualizar_vendedor(conn, vendedor_api):
    """Insere ou atualiza um vendedor no banco de dados."""
    cursor = None
    try:
        cursor = conn.cursor()
        id_vendedor = vendedor_api.get("id")
        nome_vendedor = vendedor_api.get("nome")
        situacao_vendedor = vendedor_api.get("situacao")

        if not id_vendedor or not nome_vendedor:
            print(f"AVISO: Vendedor com ID ou nome ausente, pulando: {vendedor_api}")
            return

        upsert_query = """
        INSERT INTO vendedores (id_vendedor_tiny, nome_vendedor, situacao_vendedor, data_atualizacao)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (id_vendedor_tiny)
        DO UPDATE SET
            nome_vendedor = EXCLUDED.nome_vendedor,
            situacao_vendedor = EXCLUDED.situacao_vendedor,
            data_atualizacao = CURRENT_TIMESTAMP;
        """
        cursor.execute(upsert_query, (id_vendedor, nome_vendedor, situacao_vendedor))
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print(f"Erro ao inserir/atualizar vendedor ID {id_vendedor}: {error}")
        if conn: conn.rollback()
    finally:
        if cursor:
            cursor.close()

# --- Funções para Pedidos ---
def listar_pedidos_tiny_v2(api_token, pagina=1, data_inicial_filtro=None, data_final_filtro=None):
    endpoint = f"{TINY_API_V2_BASE_URL}/pedidos.pesquisa.php"
    payload = {"token": api_token, "formato": "json", "pagina": pagina}
    if data_inicial_filtro: payload["dataInicial"] = data_inicial_filtro # Formato DD/MM/AAAA
    if data_final_filtro: payload["dataFinal"] = data_final_filtro     # Formato DD/MM/AAAA
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    print(f"Buscando lista de pedidos da API v2 (página {pagina})...")
    response = None
    try:
        response = requests.post(endpoint, data=payload, headers=headers)
        response.raise_for_status()
        response_data = response.json()
        if response_data.get("retorno", {}).get("status") == "Erro":
            print(f"Erro API Tiny (listar pedidos): {response_data.get('retorno', {}).get('erros')}")
            return None
        return response_data
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar lista de pedidos (API v2): {e}")
        if response is not None: print(f"Detalhes: {response.text}")
        return None
    except json.JSONDecodeError:
        print(f"Erro ao decodificar JSON da API Tiny v2 (listar pedidos). Resposta: {response.text if response else 'N/A'}")
        return None

def obter_detalhes_pedido_tiny_v2(api_token, id_pedido_tiny):
    endpoint = f"{TINY_API_V2_BASE_URL}/pedido.obter.php"
    payload = {"token": api_token, "formato": "json", "id": id_pedido_tiny}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = None
    try:
        response = requests.post(endpoint, data=payload, headers=headers)
        response.raise_for_status()
        response_data = response.json()
        if response_data.get("retorno", {}).get("status") == "Erro":
            print(f"Erro API Tiny (obter pedido ID {id_pedido_tiny}): {response_data.get('retorno', {}).get('erros')}")
            return None
        return response_data.get("retorno", {}).get("pedido")
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar detalhes do pedido ID {id_pedido_tiny} (API v2): {e}")
        if response is not None: print(f"Detalhes: {response.text}")
        return None
    except json.JSONDecodeError:
        print(f"Erro ao decodificar JSON da API Tiny v2 (obter pedido ID {id_pedido_tiny}). Resposta: {response.text if response else 'N/A'}")
        return None

def inserir_ou_atualizar_pedido(conn, pedido_api):
    cursor = None
    try:
        cursor = conn.cursor()
        id_pedido = pedido_api.get("id")
        numero_pedido = pedido_api.get("numero")
        data_pedido_str = pedido_api.get("data_pedido")
        data_pedido_db = format_date_to_db(data_pedido_str)
        
        id_lista_preco = pedido_api.get("id_lista_preco")
        descricao_lista_preco = pedido_api.get("descricao_lista_preco")
        
        cliente_info = pedido_api.get("cliente", {})
        nome_cliente = cliente_info.get("nome")
        cpf_cnpj_cliente = cliente_info.get("cpf_cnpj")
        email_cliente = cliente_info.get("email")
        fone_cliente = cliente_info.get("fone")
        
        id_vendedor = pedido_api.get("id_vendedor")
        nome_vendedor_pedido = pedido_api.get("nome_vendedor") 
        
        situacao_pedido = pedido_api.get("situacao")
        valor_total_pedido = pedido_api.get("total_pedido") 
        valor_desconto_pedido = pedido_api.get("valor_desconto")
        
        condicao_pagamento = pedido_api.get("condicao_pagamento")
        forma_pagamento = pedido_api.get("forma_pagamento")

        if not id_pedido:
            print(f"AVISO: Pedido com ID ausente, pulando: {pedido_api}")
            return

        upsert_query = """
        INSERT INTO pedidos (
            id_pedido_tiny, numero_pedido_tiny, data_pedido, id_lista_preco, descricao_lista_preco, 
            nome_cliente, cpf_cnpj_cliente, email_cliente, fone_cliente, 
            id_vendedor, nome_vendedor_pedido, situacao_pedido, valor_total_pedido, valor_desconto_pedido, 
            condicao_pagamento, forma_pagamento, data_atualizacao
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (id_pedido_tiny)
        DO UPDATE SET
            numero_pedido_tiny = EXCLUDED.numero_pedido_tiny,
            data_pedido = EXCLUDED.data_pedido,
            id_lista_preco = EXCLUDED.id_lista_preco,
            descricao_lista_preco = EXCLUDED.descricao_lista_preco,
            nome_cliente = EXCLUDED.nome_cliente,
            cpf_cnpj_cliente = EXCLUDED.cpf_cnpj_cliente,
            email_cliente = EXCLUDED.email_cliente,
            fone_cliente = EXCLUDED.fone_cliente,
            id_vendedor = EXCLUDED.id_vendedor,
            nome_vendedor_pedido = EXCLUDED.nome_vendedor_pedido,
            situacao_pedido = EXCLUDED.situacao_pedido,
            valor_total_pedido = EXCLUDED.valor_total_pedido,
            valor_desconto_pedido = EXCLUDED.valor_desconto_pedido,
            condicao_pagamento = EXCLUDED.condicao_pagamento,
            forma_pagamento = EXCLUDED.forma_pagamento,
            data_atualizacao = CURRENT_TIMESTAMP;
        """
        cursor.execute(upsert_query, (
            id_pedido, numero_pedido, data_pedido_db, id_lista_preco, descricao_lista_preco,
            nome_cliente, cpf_cnpj_cliente, email_cliente, fone_cliente,
            id_vendedor, nome_vendedor_pedido, situacao_pedido, valor_total_pedido, valor_desconto_pedido,
            condicao_pagamento, forma_pagamento
        ))
        conn.commit()

        itens_api = pedido_api.get("itens", [])
        if id_pedido:
            print(f"Limpando itens antigos do pedido ID: {id_pedido} antes de nova inserção...")
            cursor.execute("DELETE FROM itens_pedido WHERE id_pedido_tiny = %s", (id_pedido,))
            # conn.commit() # O commit será feito após todos os itens serem processados ou no final da função pedido
            print(f"Itens antigos do pedido ID: {id_pedido} limpos.")

        for item_api_wrapper in itens_api:
            item_api = item_api_wrapper.get("item")
            if item_api:
                inserir_ou_atualizar_item_pedido(conn, id_pedido, item_api)
        conn.commit() # Commit após processar todos os itens de um pedido

    except (Exception, psycopg2.Error) as error:
        print(f"Erro ao inserir/atualizar pedido ID {id_pedido if 'id_pedido' in locals() else 'Desconhecido'}: {error}")
        if conn: conn.rollback()
    finally:
        if cursor:
            cursor.close()

def inserir_ou_atualizar_item_pedido(conn, id_pedido_tiny_fk, item_api):
    cursor = None
    try:
        cursor = conn.cursor()
        id_produto_item = item_api.get("id_produto")
        sku_item = item_api.get("codigo")
        descricao_item = item_api.get("descricao")
        unidade_item = item_api.get("unidade")
        quantidade_item = item_api.get("quantidade")
        valor_unitario_item = item_api.get("valor_unitario")
        
        valor_total_item = None
        if quantidade_item is not None and valor_unitario_item is not None:
            try:
                valor_total_item = float(quantidade_item) * float(valor_unitario_item)
            except ValueError:
                print(f"AVISO: Não foi possível calcular valor_total_item para o item do pedido {id_pedido_tiny_fk}, SKU {sku_item}. Quantidade: {quantidade_item}, Unitário: {valor_unitario_item}")

        insert_query = """
        INSERT INTO itens_pedido (
            id_pedido_tiny, id_produto_tiny, sku_produto, descricao_item, unidade_item, 
            quantidade_item, valor_unitario_item, valor_total_item, data_atualizacao
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP);
        """
        
        cursor.execute(insert_query, (
            id_pedido_tiny_fk, id_produto_item, sku_item, descricao_item, unidade_item, 
            quantidade_item, valor_unitario_item, valor_total_item
        ))

    except (Exception, psycopg2.Error) as error:
        print(f"Erro ao inserir item SKU {sku_item if 'sku_item' in locals() else 'Desconhecido'} para o pedido ID {id_pedido_tiny_fk}: {error}")
    finally:
        if cursor:
            cursor.close()


def main():
    conn = conectar_db()
    if not conn:
        return

    # Intervalos de pausa (em segundos)
    PAUSA_ENTRE_CHAMADAS_DETALHE = 1.0  # Pausa após buscar detalhes de um item (produto, pedido)
    PAUSA_ENTRE_PAGINAS = 2.0        # Pausa após processar uma página de listagem

    try:
        print("\n--- Verificando/Criando Tabelas ---")
        criar_tabela_categorias(conn)
        criar_tabela_produtos(conn)
        criar_tabela_vendedores(conn) 
        criar_tabela_pedidos(conn)    
        criar_tabela_itens_pedido(conn)
        print("--- Fim da Verificação/Criação de Tabelas ---")

        print("\n--- Processando Categorias ---")
        categorias_api = obter_arvore_categorias_tiny_v2(TINY_API_V2_TOKEN)
        if categorias_api:
            processar_e_inserir_categorias(conn, categorias_api)
        else:
            print("Não foi possível obter dados das categorias da API.")
        print("--- Fim do Processamento de Categorias ---")
        time.sleep(PAUSA_ENTRE_PAGINAS) # Pausa após toda a seção de categorias

        print("\n--- Processando Produtos (Detalhado) ---")
        pagina_produto = 1
        while True:
            response_produtos_lista = listar_produtos_tiny_v2(TINY_API_V2_TOKEN, pagina=pagina_produto)
            if not response_produtos_lista or response_produtos_lista.get("retorno", {}).get("status") == "Erro":
                print(f"Erro ou fim da lista de produtos na página {pagina_produto}.")
                break
            
            produtos_da_pagina = response_produtos_lista.get("retorno", {}).get("produtos", [])
            if not produtos_da_pagina:
                print(f"Nenhum produto encontrado na página {pagina_produto}. Fim da lista de produtos.")
                break

            print(f"Processando {len(produtos_da_pagina)} produtos da página {pagina_produto}...")
            for produto_lista_wrapper in produtos_da_pagina:
                produto_lista = produto_lista_wrapper.get("produto")
                if not produto_lista or not produto_lista.get("id"):
                    print(f"AVISO: Produto na lista sem ID, pulando: {produto_lista}")
                    continue
                
                id_produto_tiny_lista = produto_lista.get("id")
                print(f"  Buscando detalhes do produto ID: {id_produto_tiny_lista}...")
                produto_detalhado_api = obter_detalhes_produto_tiny_v2(TINY_API_V2_TOKEN, id_produto_tiny_lista)
                
                if produto_detalhado_api:
                    print(f"    Inserindo/Atualizando produto detalhado ID: {produto_detalhado_api.get('id')}, Nome: {produto_detalhado_api.get('nome')}")
                    inserir_ou_atualizar_produto_detalhado(conn, produto_detalhado_api)
                else:
                    print(f"    Não foi possível obter detalhes para o produto ID: {id_produto_tiny_lista}. Pulando inserção detalhada.")
                time.sleep(PAUSA_ENTRE_CHAMADAS_DETALHE) 
            
            if pagina_produto >= int(response_produtos_lista.get("retorno", {}).get("numero_paginas", 1)):
                print("Todas as páginas de produtos processadas.")
                break
            pagina_produto += 1
            time.sleep(PAUSA_ENTRE_PAGINAS) 
        print("--- Fim do Processamento de Produtos ---")

        print("\n--- Processando Vendedores ---")
        pagina_vendedor = 1
        while True:
            response_vendedores_lista = listar_vendedores_tiny_v2(TINY_API_V2_TOKEN, pagina=pagina_vendedor)
            if not response_vendedores_lista or response_vendedores_lista.get("retorno", {}).get("status") == "Erro":
                print(f"Erro ou fim da lista de vendedores na página {pagina_vendedor}.")
                break
            
            vendedores_da_pagina = response_vendedores_lista.get("retorno", {}).get("vendedores", [])
            if not vendedores_da_pagina:
                print(f"Nenhum vendedor encontrado na página {pagina_vendedor}. Fim da lista de vendedores.")
                break

            print(f"Processando {len(vendedores_da_pagina)} vendedores da página {pagina_vendedor}...")
            for vendedor_wrapper in vendedores_da_pagina:
                vendedor_api = vendedor_wrapper.get("vendedor")
                if vendedor_api:
                    print(f"  Inserindo/Atualizando vendedor ID: {vendedor_api.get('id')}, Nome: {vendedor_api.get('nome')}")
                    inserir_ou_atualizar_vendedor(conn, vendedor_api)
                else:
                    print(f"AVISO: Dados do vendedor inválidos ou ausentes no wrapper: {vendedor_wrapper}")
                time.sleep(PAUSA_ENTRE_CHAMADAS_DETALHE)

            if pagina_vendedor >= int(response_vendedores_lista.get("retorno", {}).get("numero_paginas", 1)):
                print("Todas as páginas de vendedores processadas.")
                break
            pagina_vendedor += 1
            time.sleep(PAUSA_ENTRE_PAGINAS)
        print("--- Fim do Processamento de Vendedores ---")

        print("\n--- Processando Pedidos e Itens de Pedido ---")
        pagina_pedido = 1
        while True:
            response_pedidos_lista = listar_pedidos_tiny_v2(TINY_API_V2_TOKEN, pagina=pagina_pedido)
            if not response_pedidos_lista or response_pedidos_lista.get("retorno", {}).get("status") == "Erro":
                print(f"Erro ou fim da lista de pedidos na página {pagina_pedido}.")
                break

            pedidos_da_pagina = response_pedidos_lista.get("retorno", {}).get("pedidos", [])
            if not pedidos_da_pagina:
                print(f"Nenhum pedido encontrado na página {pagina_pedido}. Fim da lista de pedidos.")
                break

            print(f"Processando {len(pedidos_da_pagina)} pedidos da página {pagina_pedido}...")
            for pedido_lista_wrapper in pedidos_da_pagina:
                pedido_lista = pedido_lista_wrapper.get("pedido")
                if not pedido_lista or not pedido_lista.get("id"):
                    print(f"AVISO: Pedido na lista sem ID, pulando: {pedido_lista}")
                    continue
                
                id_pedido_tiny_lista = pedido_lista.get("id")
                print(f"  Buscando detalhes do pedido ID: {id_pedido_tiny_lista}...")
                pedido_detalhado_api = obter_detalhes_pedido_tiny_v2(TINY_API_V2_TOKEN, id_pedido_tiny_lista)
                
                if pedido_detalhado_api:
                    print(f"    Inserindo/Atualizando pedido detalhado ID: {pedido_detalhado_api.get('id')}, Numero: {pedido_detalhado_api.get('numero')}")
                    inserir_ou_atualizar_pedido(conn, pedido_detalhado_api) 
                else:
                    print(f"    Não foi possível obter detalhes para o pedido ID: {id_pedido_tiny_lista}. Pulando inserção.")
                time.sleep(PAUSA_ENTRE_CHAMADAS_DETALHE) 
            
            if pagina_pedido >= int(response_pedidos_lista.get("retorno", {}).get("numero_paginas", 1)):
                print("Todas as páginas de pedidos processadas.")
                break
            pagina_pedido += 1
            time.sleep(PAUSA_ENTRE_PAGINAS) 
        print("--- Fim do Processamento de Pedidos e Itens de Pedido ---")

        print("\nIntegração concluída com sucesso!")

    except (Exception, psycopg2.Error) as error:
        print(f"Erro geral na função main: {error}")
        if conn:
            try: conn.rollback(); print("Rollback da transação principal realizado.")
            except psycopg2.InterfaceError as ie: print(f"Erro ao tentar rollback na transação principal: {ie}")

    finally:
        if conn:
            conn.close()
            print("Conexão com o PostgreSQL fechada.")

if __name__ == "__main__":
    main()

