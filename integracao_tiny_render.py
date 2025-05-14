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
from datetime import datetime, timezone # Adicionado timezone
import re # Para análise de mensagens de erro de paginação

# --- Configurações da API v2 do Tiny ---
TINY_API_V2_BASE_URL = "https://api.tiny.com.br/api2"
TINY_API_V2_TOKEN = os.getenv("TINY_API_V2_TOKEN", "14c9d28dc8d8dcb2d7229efb1fafcf0392059b2579414c17d02eee4c821f4677")

# --- Configurações do Banco de Dados PostgreSQL no Render ---
DB_HOST = os.getenv("DB_HOST", "dpg-d0h4vjgdl3ps73cihsv0-a.oregon-postgres.render.com")
DB_NAME = os.getenv("DB_NAME", "banco_tiny_fusion")
DB_USER = os.getenv("DB_USER", "banco_tiny_fusion_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "9JFbOUjBU1f3tM10EZygXgtOp8vKoUyb")
DB_PORT = os.getenv("DB_PORT", "5432") # Geralmente 5432 para PostgreSQL

# --- Configurações de Robustez ---
MAX_DB_RECONNECT_ATTEMPTS = 3
DB_RECONNECT_DELAY_SECONDS = 10
PAUSA_ENTRE_CHAMADAS_DETALHE = 1.0 # Segundos de pausa após cada chamada de detalhe (produto, vendedor, pedido)
PAUSA_ENTRE_PAGINAS = 2.0      # Segundos de pausa após processar uma página de listagem

# Arquivo para guardar o progresso da paginação da carga completa de produtos
ARQUIVO_PROGRESSO_PAGINACAO_PRODUTOS = "ultima_pagina_produto_processada.txt"
PAGINAS_POR_LOTE_PRODUTOS = 3  # Quantas páginas de produtos processar por execução do lote na carga completa

# Arquivos para guardar os timestamps da última sincronização incremental bem-sucedida
ARQUIVO_TIMESTAMP_PRODUTOS = "progresso_produtos_timestamp.txt"
ARQUIVO_TIMESTAMP_PEDIDOS = "progresso_pedidos_timestamp.txt"

def conectar_db(attempt=1):
    """Conecta ao banco de dados PostgreSQL e retorna o objeto de conexão."""
    conn = None
    try:
        print(f"Tentando conectar ao PostgreSQL (tentativa {attempt})...")
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            connect_timeout=10 # Timeout para conexão inicial
        )
        conn.autocommit = False # Controlar transações manualmente
        print("Conexão com o PostgreSQL bem-sucedida!")
    except (Exception, psycopg2.Error) as error:
        print(f"Erro ao conectar ao PostgreSQL (tentativa {attempt}): {error}")
        if attempt < MAX_DB_RECONNECT_ATTEMPTS:
            print(f"Aguardando {DB_RECONNECT_DELAY_SECONDS}s para nova tentativa de conexão...")
            time.sleep(DB_RECONNECT_DELAY_SECONDS)
            return conectar_db(attempt + 1)
        else:
            print("Número máximo de tentativas de reconexão ao DB atingido.")
    return conn

def garantir_conexao(conn):
    """Garante que a conexão com o DB esteja ativa, reconectando se necessário."""
    if conn is None or conn.closed != 0:
        print("Conexão com DB está fechada ou nula. Tentando reconectar...")
        conn = conectar_db()
    return conn

def ler_timestamp_progresso(arquivo_timestamp):
    """Lê o timestamp do arquivo de progresso."""
    try:
        if os.path.exists(arquivo_timestamp):
            with open(arquivo_timestamp, "r") as f:
                timestamp_str = f.read().strip()
                if timestamp_str:
                    # Tenta converter para datetime para validar o formato, depois retorna como string dd/mm/yyyy hh:mm:ss
                    dt_obj = datetime.strptime(timestamp_str, "%d/%m/%Y %H:%M:%S")
                    print(f"Timestamp lido de {arquivo_timestamp}: {timestamp_str}")
                    return timestamp_str 
    except Exception as e:
        print(f"Erro ao ler timestamp de {arquivo_timestamp}: {e}. Considernado como não existente.")
    return None

def salvar_timestamp_progresso(arquivo_timestamp, timestamp_dt_obj):
    """Salva o timestamp (objeto datetime) no arquivo de progresso no formato dd/mm/yyyy HH:MM:SS."""
    try:
        timestamp_str = timestamp_dt_obj.strftime("%d/%m/%Y %H:%M:%S")
        with open(arquivo_timestamp, "w") as f:
            f.write(timestamp_str)
        print(f"Timestamp salvo em {arquivo_timestamp}: {timestamp_str}")
    except Exception as e:
        print(f"Erro ao salvar timestamp em {arquivo_timestamp}: {e}")

def criar_tabela_categorias(conn):
    """Cria a tabela 'categorias' se ela não existir."""
    cursor = None
    try:
        conn = garantir_conexao(conn)
        if conn is None: return conn, False
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
        return conn, True
    except (Exception, psycopg2.Error) as error:
        print(f"Erro ao criar tabela 'categorias': {error}")
        if conn and conn.closed == 0: 
            try: conn.rollback(); print("Rollback da transação da tabela categorias realizado.")
            except psycopg2.Error as rb_error: print(f"Erro no rollback de categorias: {rb_error}")
        return conn, False
    finally:
        if cursor: cursor.close()

def criar_tabela_produtos(conn):
    cursor = None
    try:
        conn = garantir_conexao(conn)
        if conn is None: return conn, False
        cursor = conn.cursor()
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
        colunas_para_verificar = {
            "preco_custo": "NUMERIC(12, 2)", "tipo_produto": "VARCHAR(50)",
            "estoque_atual": "NUMERIC(10, 3)", "id_categoria_produto_tiny": "INT"
        }
        for nome_coluna, tipo_coluna in colunas_para_verificar.items():
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name='produtos' AND column_name=%s", (nome_coluna,))
            if not cursor.fetchone():
                print(f"Adicionando coluna '{nome_coluna}' à tabela 'produtos'...")
                cursor.execute(f"ALTER TABLE produtos ADD COLUMN {nome_coluna} {tipo_coluna};")
                conn.commit()
                print(f"Coluna '{nome_coluna}' adicionada.")
        fk_name = 'fk_produtos_to_categorias'
        cursor.execute("SELECT constraint_name FROM information_schema.table_constraints WHERE table_schema = 'public' AND table_name='produtos' AND constraint_name=%s AND constraint_type='FOREIGN KEY'", (fk_name,))
        if not cursor.fetchone():
            print(f"Adicionando FK '{fk_name}' à tabela 'produtos'...")
            cursor.execute(f"ALTER TABLE produtos ADD CONSTRAINT {fk_name} FOREIGN KEY (id_categoria_produto_tiny) REFERENCES categorias (id_categoria_tiny) ON DELETE SET NULL;")
            conn.commit()
            print(f"FK '{fk_name}' adicionada.")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_produtos_id_categoria_produto_tiny ON produtos (id_categoria_produto_tiny);")
        conn.commit()
        print("Tabela 'produtos' (detalhada) verificada/criada/atualizada com sucesso.")
        return conn, True
    except (Exception, psycopg2.Error) as error:
        print(f"Erro ao criar/atualizar tabela 'produtos' (detalhada): {error}")
        if conn and conn.closed == 0: 
            try: conn.rollback(); print("Rollback da transação da tabela produtos realizado.")
            except psycopg2.Error as rb_error: print(f"Erro no rollback de produtos: {rb_error}")
        return conn, False
    finally:
        if cursor: cursor.close()

def criar_tabela_vendedores(conn):
    cursor = None
    try:
        conn = garantir_conexao(conn)
        if conn is None: return conn, False
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
        return conn, True
    except (Exception, psycopg2.Error) as error:
        print(f"Erro ao criar tabela 'vendedores': {error}")
        if conn and conn.closed == 0: 
            try: conn.rollback(); print("Rollback da transação da tabela vendedores realizado.")
            except psycopg2.Error as rb_error: print(f"Erro no rollback de vendedores: {rb_error}")
        return conn, False
    finally:
        if cursor: cursor.close()

def criar_tabela_pedidos(conn):
    cursor = None
    try:
        conn = garantir_conexao(conn)
        if conn is None: return conn, False
        cursor = conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS pedidos (
            id_pedido_tiny INT PRIMARY KEY, numero_pedido_tiny VARCHAR(50), data_pedido DATE,
            id_lista_preco VARCHAR(50), descricao_lista_preco VARCHAR(255), nome_cliente VARCHAR(255),
            cpf_cnpj_cliente VARCHAR(20), email_cliente VARCHAR(255), fone_cliente VARCHAR(50),
            id_vendedor INT, situacao_pedido VARCHAR(50), valor_total_pedido NUMERIC(12, 2),
            valor_desconto_pedido NUMERIC(12, 2), condicao_pagamento VARCHAR(255),
            forma_pagamento VARCHAR(100), data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        coluna_nome_vendedor_pedido = "nome_vendedor_pedido"
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name='pedidos' AND column_name=%s", (coluna_nome_vendedor_pedido,))
        if not cursor.fetchone():
            print(f"Adicionando coluna '{coluna_nome_vendedor_pedido}' à tabela 'pedidos'...")
            cursor.execute(f"ALTER TABLE pedidos ADD COLUMN {coluna_nome_vendedor_pedido} VARCHAR(255);")
            conn.commit()
            print(f"Coluna '{coluna_nome_vendedor_pedido}' adicionada.")
        fk_name_pedidos_vendedores = 'fk_pedidos_to_vendedores'
        cursor.execute("SELECT constraint_name FROM information_schema.table_constraints WHERE table_schema = 'public' AND table_name='pedidos' AND constraint_name=%s AND constraint_type='FOREIGN KEY'", (fk_name_pedidos_vendedores,))
        if not cursor.fetchone():
            print(f"Adicionando FK '{fk_name_pedidos_vendedores}' à tabela 'pedidos'...")
            cursor.execute(f"ALTER TABLE pedidos ADD CONSTRAINT {fk_name_pedidos_vendedores} FOREIGN KEY (id_vendedor) REFERENCES vendedores (id_vendedor_tiny) ON DELETE SET NULL;")
            conn.commit()
            print(f"FK '{fk_name_pedidos_vendedores}' adicionada.")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pedidos_id_vendedor ON pedidos (id_vendedor);")
        conn.commit()
        print("Tabela 'pedidos' verificada/criada/atualizada com sucesso.")
        return conn, True
    except (Exception, psycopg2.Error) as error:
        print(f"Erro ao criar/atualizar tabela 'pedidos': {error}")
        if conn and conn.closed == 0: 
            try: conn.rollback(); print("Rollback da transação da tabela pedidos realizado.")
            except psycopg2.Error as rb_error: print(f"Erro no rollback de pedidos: {rb_error}")
        return conn, False
    finally:
        if cursor: cursor.close()

def criar_tabela_itens_pedido(conn):
    cursor = None
    try:
        conn = garantir_conexao(conn)
        if conn is None: return conn, False
        cursor = conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS itens_pedido (
            id_item_pedido SERIAL PRIMARY KEY, id_pedido_tiny INT NOT NULL, id_produto_tiny INT,
            sku_produto VARCHAR(100), descricao_item VARCHAR(255), unidade_item VARCHAR(20),
            quantidade_item NUMERIC(10, 3), valor_unitario_item NUMERIC(12, 2), valor_total_item NUMERIC(12, 2),
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
        return conn, True
    except (Exception, psycopg2.Error) as error:
        print(f"Erro ao criar tabela 'itens_pedido': {error}")
        if conn and conn.closed == 0: 
            try: conn.rollback(); print("Rollback da transação da tabela itens_pedido realizado.")
            except psycopg2.Error as rb_error: print(f"Erro no rollback de itens_pedido: {rb_error}")
        return conn, False
    finally:
        if cursor: cursor.close()

def format_date_to_db(date_str):
    if not date_str: return None
    try: return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError: print(f"AVISO: Formato de data inválido: {date_str}."); return None

def obter_arvore_categorias_tiny_v2(api_token):
    endpoint = f"{TINY_API_V2_BASE_URL}/produtos.categorias.arvore.php"
    payload = {"token": api_token, "formato": "json"}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    print("Buscando árvore de categorias da API v2...")
    response = None
    try:
        response = requests.post(endpoint, data=payload, headers=headers, timeout=30)
        response.raise_for_status()
        response_data = response.json()

        if isinstance(response_data, list):
            print("API Tiny v2 (categorias): Resposta raiz é uma lista, processando diretamente.")
            return response_data
        elif isinstance(response_data, dict):
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
        else:
            print(f"Tipo de resposta raiz inesperado da API Tiny v2 (categorias): {type(response_data)}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Erro de requisição ao buscar árvore de categorias: {e}")
    except requests.exceptions.JSONDecodeError as e:
        print(f"Erro ao decodificar JSON da árvore de categorias: {e}. Conteúdo: {response.text if response else 'Sem resposta'}")
    except Exception as e:
        print(f"Erro inesperado ao buscar árvore de categorias: {e}")
    return None

def inserir_ou_atualizar_categoria(conn, categoria_node, id_pai=None):
    cursor = None
    try:
        conn = garantir_conexao(conn)
        if conn is None: return conn, False
        cursor = conn.cursor()
        
        categoria_id = categoria_node.get("id")
        categoria_nome = categoria_node.get("descricao")
        
        if not categoria_id or not categoria_nome:
            print(f"AVISO: Categoria com dados incompletos ignorada: {categoria_node}")
            return conn, True # Considera sucesso para não parar o processo

        # Tenta converter para int, se falhar, loga e ignora
        try:
            categoria_id_int = int(categoria_id)
        except ValueError:
            print(f"AVISO: ID de categoria inválido (não numérico) ignorado: {categoria_id}. Nome: {categoria_nome}")
            return conn, True

        id_pai_int = None
        if id_pai:
            try:
                id_pai_int = int(id_pai)
            except ValueError:
                print(f"AVISO: ID da categoria pai inválido (não numérico) ignorado: {id_pai} para categoria {categoria_nome}")
                # Não definimos id_pai_int, então será NULL no banco
        
        print(f"Processando categoria: ID={categoria_id_int}, Nome='{categoria_nome}', PaiID={id_pai_int if id_pai_int else 'N/A'}")

        upsert_query = """
        INSERT INTO categorias (id_categoria_tiny, nome_categoria, id_categoria_pai_tiny, data_atualizacao)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (id_categoria_tiny)
        DO UPDATE SET
            nome_categoria = EXCLUDED.nome_categoria,
            id_categoria_pai_tiny = EXCLUDED.id_categoria_pai_tiny,
            data_atualizacao = CURRENT_TIMESTAMP;
        """
        cursor.execute(upsert_query, (categoria_id_int, categoria_nome, id_pai_int))
        # conn.commit() # Commit será feito no final da função recursiva principal
        
        # Processar filhos recursivamente
        if "filhos" in categoria_node and isinstance(categoria_node["filhos"], list):
            for filho_node in categoria_node["filhos"]:
                if isinstance(filho_node, dict) and "categoria" in filho_node:
                    conn, success = inserir_ou_atualizar_categoria(conn, filho_node["categoria"], categoria_id_int)
                    if not success: return conn, False # Propaga o erro
        return conn, True

    except (Exception, psycopg2.Error) as error:
        print(f"Erro ao inserir/atualizar categoria ID {categoria_node.get('id') if categoria_node else 'N/A'}: {error}")
        # Rollback será feito na função chamadora principal se necessário
        return conn, False
    finally:
        if cursor: cursor.close()

def buscar_e_gravar_categorias(conn, api_token):
    print("\n--- Iniciando Sincronização de Categorias ---")
    conn = garantir_conexao(conn)
    if conn is None: return conn, False

    categorias_tree = obter_arvore_categorias_tiny_v2(api_token)
    if categorias_tree is None:
        print("Não foi possível obter a árvore de categorias. Abortando sincronização de categorias.")
        return conn, False

    all_success = True
    if isinstance(categorias_tree, list):
        for node_wrapper in categorias_tree: # A API v2 pode retornar uma lista de nós raiz encapsulados
            if isinstance(node_wrapper, dict) and "categoria" in node_wrapper:
                categoria_raiz = node_wrapper["categoria"]
                conn, success = inserir_ou_atualizar_categoria(conn, categoria_raiz, None)
                if not success:
                    all_success = False
                    break
            elif isinstance(node_wrapper, dict) and "id" in node_wrapper and "descricao" in node_wrapper: # Caso a raiz seja a própria categoria
                 conn, success = inserir_ou_atualizar_categoria(conn, node_wrapper, None)
                 if not success:
                    all_success = False
                    break
            else:
                print(f"AVISO: Nó raiz de categoria em formato inesperado ignorado: {node_wrapper}")
    else:
        print(f"Formato inesperado para a árvore de categorias: {type(categorias_tree)}. Esperado: lista.")
        all_success = False

    if all_success:
        try:
            conn.commit()
            print("Sincronização de categorias concluída com sucesso. Commit realizado.")
        except psycopg2.Error as e:
            print(f"Erro ao commitar transação de categorias: {e}")
            try: conn.rollback(); print("Rollback da transação de categorias realizado.")
            except psycopg2.Error as rb_error: print(f"Erro no rollback de categorias: {rb_error}")
            return conn, False
    else:
        print("Falha na sincronização de uma ou mais categorias. Rollback será realizado.")
        try: conn.rollback(); print("Rollback da transação de categorias realizado devido a erros.")
        except psycopg2.Error as rb_error: print(f"Erro no rollback de categorias: {rb_error}")
        return conn, False
    
    time.sleep(PAUSA_ENTRE_PAGINAS) # Pausa após processar todas as categorias
    return conn, True

def ler_progresso_paginacao_produtos():
    """Lê a última página de produtos processada do arquivo de progresso."""
    try:
        if os.path.exists(ARQUIVO_PROGRESSO_PAGINACAO_PRODUTOS):
            with open(ARQUIVO_PROGRESSO_PAGINACAO_PRODUTOS, "r") as f:
                pagina = int(f.read().strip())
                print(f"Retomando processamento de produtos da página: {pagina}")
                return pagina
    except Exception as e:
        print(f"Erro ao ler progresso de produtos: {e}. Iniciando da página 1.")
    return 1

def salvar_progresso_paginacao_produtos(pagina):
    """Salva a última página de produtos processada no arquivo de progresso."""
    try:
        with open(ARQUIVO_PROGRESSO_PAGINACAO_PRODUTOS, "w") as f:
            f.write(str(pagina))
        print(f"Progresso de produtos salvo: página {pagina}")
    except Exception as e:
        print(f"Erro ao salvar progresso de produtos: {e}")

def resetar_progresso_paginacao_produtos():
    if os.path.exists(ARQUIVO_PROGRESSO_PAGINACAO_PRODUTOS):
        try:
            os.remove(ARQUIVO_PROGRESSO_PAGINACAO_PRODUTOS)
            print(f"Arquivo de progresso de paginação de produtos ({ARQUIVO_PROGRESSO_PAGINACAO_PRODUTOS}) removido.")
        except Exception as e:
            print(f"Erro ao tentar remover o arquivo de progresso de paginação de produtos: {e}")
    else:
        print(f"Arquivo de progresso de paginação de produtos ({ARQUIVO_PROGRESSO_PAGINACAO_PRODUTOS}) não encontrado para remoção.")

def buscar_e_gravar_produtos(conn, api_token):
    print("\n--- Iniciando Sincronização de Produtos ---")
    conn = garantir_conexao(conn)
    if conn is None: return conn, False, False # conn, sucesso, produtos_restantes

    pagina_atual = ler_progresso_paginacao_produtos()
    produtos_processados_neste_lote = 0
    paginas_processadas_neste_lote = 0
    todos_produtos_processados_carga_completa = False # Flag para indicar se a carga completa terminou

    # Tenta a busca incremental primeiro
    ultimo_timestamp_produtos_str = ler_timestamp_progresso(ARQUIVO_TIMESTAMP_PRODUTOS)
    
    if ultimo_timestamp_produtos_str:
        print(f"Modo incremental para produtos ativado. Buscando alterações desde: {ultimo_timestamp_produtos_str}")
        endpoint_produtos = f"{TINY_API_V2_BASE_URL}/produtos.pesquisar.alteracoes.php"
        payload_base = {"token": api_token, "formato": "json", "dataAlteracao": ultimo_timestamp_produtos_str}
        # A API de alterações pode ou não ter paginação, verificar documentação ou testar.
        # Assumindo que não tem paginação complexa ou que retorna tudo de uma vez para este exemplo.
        # Se tiver paginação, a lógica precisará ser adaptada aqui.
        pagina_api_incremental = 1 # Começa da página 1 para a busca incremental
        
        while True: # Loop para paginação da API de alterações, se houver
            payload = payload_base.copy()
            if pagina_api_incremental > 1: # Adiciona página se não for a primeira
                 payload["pagina"] = pagina_api_incremental

            print(f"Buscando produtos alterados (página {pagina_api_incremental}) desde {ultimo_timestamp_produtos_str}...")
            response = None
            try:
                response = requests.post(endpoint_produtos, data=payload, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=60)
                response.raise_for_status()
                data = response.json()

                if data.get("retorno", {}).get("status") == "Erro":
                    erros = data.get("retorno", {}).get("erros", [])
                    codigo_erro = None
                    if isinstance(erros, list) and len(erros) > 0 and isinstance(erros[0], dict):
                        codigo_erro = erros[0].get("erro", {}).get("codigo_erro")
                    elif isinstance(erros, dict): # Formato antigo de erro único
                        codigo_erro = erros.get("erro", {}).get("codigo_erro")
                    elif isinstance(erros, str): # Erro como string simples
                        if "Codigo: 22" in erros or "pagina nao encontrada" in erros.lower():
                            codigo_erro = "22"
                    
                    if codigo_erro == "22": # Página não encontrada - Fim dos produtos alterados
                        print("API de alterações de produtos: Nenhuma página adicional encontrada (Erro 22).")
                        break # Sai do loop de paginação incremental
                    else:
                        print(f"Erro da API Tiny ao buscar produtos alterados: {erros}")
                        return conn, False, True # Sinaliza erro, mas podem haver mais produtos (de carga completa, se aplicável)

                produtos_alterados = data.get("retorno", {}).get("produtos", [])
                if not produtos_alterados and pagina_api_incremental == 1:
                    print("Nenhum produto alterado encontrado desde o último timestamp.")
                    # Atualiza o timestamp para o momento atual mesmo que nada tenha mudado, para a próxima busca partir daqui.
                    salvar_timestamp_progresso(ARQUIVO_TIMESTAMP_PRODUTOS, datetime.now(timezone.utc))
                    return conn, True, False # Sucesso, nenhum produto restante para processar
                if not produtos_alterados:
                    print("Fim da lista de produtos alterados (página vazia).")
                    break # Sai do loop de paginação incremental

                print(f"Encontrados {len(produtos_alterados)} produtos alterados na página {pagina_api_incremental}.")
                for produto_wrapper in produtos_alterados:
                    produto_data = produto_wrapper.get("produto")
                    if not produto_data or not produto_data.get("id"):
                        print(f"AVISO: Produto alterado com dados incompletos ou sem ID ignorado: {produto_data}")
                        continue
                    conn, success_detail = buscar_e_gravar_detalhe_produto(conn, api_token, produto_data["id"])
                    if not success_detail:
                        # Não retorna erro fatal aqui, apenas loga e continua, para não parar a sincronização por um produto
                        print(f"Falha ao processar detalhe do produto alterado ID {produto_data['id']}. Continuando...")
                    else:
                        produtos_processados_neste_lote += 1
                
                conn.commit() # Commit após processar os produtos da página incremental
                print(f"Commit realizado para produtos alterados da página {pagina_api_incremental}.")
                time.sleep(PAUSA_ENTRE_PAGINAS)
                pagina_api_incremental += 1

            except requests.exceptions.RequestException as e:
                print(f"Erro de requisição ao buscar produtos alterados: {e}")
                return conn, False, True
            except requests.exceptions.JSONDecodeError as e:
                print(f"Erro ao decodificar JSON de produtos alterados: {e}. Conteúdo: {response.text if response else 'Sem resposta'}")
                return conn, False, True
            except Exception as e:
                print(f"Erro inesperado ao buscar/gravar produtos alterados: {e}")
                if conn and conn.closed == 0: 
                    try: conn.rollback(); print("Rollback da transação de produtos alterados realizado.")
                    except psycopg2.Error as rb_error: print(f"Erro no rollback de produtos alterados: {rb_error}")
                return conn, False, True
        
        # Se chegou aqui, a busca incremental (potencialmente paginada) terminou.
        salvar_timestamp_progresso(ARQUIVO_TIMESTAMP_PRODUTOS, datetime.now(timezone.utc))
        print("Sincronização incremental de produtos concluída.")
        resetar_progresso_paginacao_produtos() # Reseta progresso de carga completa, pois a incremental está em dia.
        return conn, True, False # Sucesso, nenhum produto restante para processar (incremental cobriu)

    # Se não há timestamp, executa a carga completa em lotes
    print(f"Modo de carga completa para produtos ativado. Iniciando da página {pagina_atual}.")
    endpoint_produtos_lista = f"{TINY_API_V2_BASE_URL}/produtos.pesquisar.php"
    
    while paginas_processadas_neste_lote < PAGINAS_POR_LOTE_PRODUTOS:
        print(f"Buscando lista de produtos (página {pagina_atual})...")
        payload = {"token": api_token, "formato": "json", "pagina": pagina_atual}
        response = None
        try:
            response = requests.post(endpoint_produtos_lista, data=payload, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=60)
            response.raise_for_status()
            data = response.json()

            if data.get("retorno", {}).get("status") == "Erro":
                erros = data.get("retorno", {}).get("erros", [])
                codigo_erro = None
                mensagem_erro_completa = str(erros)

                if isinstance(erros, list) and len(erros) > 0 and isinstance(erros[0], dict):
                    erro_obj = erros[0].get("erro", {}) if isinstance(erros[0], dict) else {}
                    codigo_erro = erro_obj.get("codigo_erro")
                elif isinstance(erros, dict): # Formato antigo de erro único
                    codigo_erro = erros.get("erro", {}).get("codigo_erro")
                elif isinstance(erros, str): # Erro como string simples
                    if "Codigo: 22" in erros or "pagina nao encontrada" in erros.lower() or "página não encontrada" in erros.lower():
                        codigo_erro = "22"
                
                # Tratamento mais robusto para erro 22 com base na mensagem
                if not codigo_erro and ("pagina nao encontrada" in mensagem_erro_completa.lower() or "página não encontrada" in mensagem_erro_completa.lower()):
                    match = re.search(r"Codigo:\s*(\d+)", mensagem_erro_completa)
                    if match and match.group(1) == "22":
                        codigo_erro = "22"
                    elif "página não encontrada" in mensagem_erro_completa.lower() or "pagina nao encontrada" in mensagem_erro_completa.lower(): # Caso não tenha o código mas tenha a msg
                        codigo_erro = "22"

                if codigo_erro == "22":
                    print(f"API de produtos (carga completa): Nenhuma página adicional encontrada (Erro 22 na página {pagina_atual}). Todos os produtos foram listados.")
                    todos_produtos_processados_carga_completa = True
                    resetar_progresso_paginacao_produtos()
                    # Salva o timestamp aqui, pois a carga completa terminou
                    salvar_timestamp_progresso(ARQUIVO_TIMESTAMP_PRODUTOS, datetime.now(timezone.utc))
                    print("Carga completa de produtos finalizada. Timestamp salvo.")
                    break # Sai do loop de paginação da carga completa
                else:
                    print(f"Erro da API Tiny ao listar produtos (página {pagina_atual}): {erros}")
                    salvar_progresso_paginacao_produtos(pagina_atual) # Salva a página atual para tentar de novo
                    return conn, False, True # Erro, mas podem haver mais produtos

            produtos_da_pagina = data.get("retorno", {}).get("produtos", [])
            if not produtos_da_pagina:
                print(f"Fim da lista de produtos (página {pagina_atual} vazia). Todos os produtos foram listados.")
                todos_produtos_processados_carga_completa = True
                resetar_progresso_paginacao_produtos()
                salvar_timestamp_progresso(ARQUIVO_TIMESTAMP_PRODUTOS, datetime.now(timezone.utc))
                print("Carga completa de produtos finalizada (página vazia). Timestamp salvo.")
                break # Sai do loop de paginação da carga completa

            print(f"Encontrados {len(produtos_da_pagina)} produtos na página {pagina_atual}.")
            for produto_wrapper in produtos_da_pagina:
                produto_lista = produto_wrapper.get("produto")
                if not produto_lista or not produto_lista.get("id"):
                    print(f"AVISO: Produto da lista com dados incompletos ou sem ID ignorado: {produto_lista}")
                    continue
                
                conn, success_detail = buscar_e_gravar_detalhe_produto(conn, api_token, produto_lista["id"])
                if not success_detail:
                    print(f"Falha ao processar detalhe do produto ID {produto_lista['id']} da lista. Continuando...")
                else:
                    produtos_processados_neste_lote += 1
            
            conn.commit()
            print(f"Commit realizado para produtos da página {pagina_atual}.")
            
            paginas_processadas_neste_lote += 1
            pagina_atual += 1
            salvar_progresso_paginacao_produtos(pagina_atual) # Salva a PRÓXIMA página a ser processada
            time.sleep(PAUSA_ENTRE_PAGINAS)

        except requests.exceptions.RequestException as e:
            print(f"Erro de requisição ao listar produtos (página {pagina_atual}): {e}")
            salvar_progresso_paginacao_produtos(pagina_atual)
            return conn, False, True
        except requests.exceptions.JSONDecodeError as e:
            print(f"Erro ao decodificar JSON de produtos (página {pagina_atual}): {e}. Conteúdo: {response.text if response else 'Sem resposta'}")
            salvar_progresso_paginacao_produtos(pagina_atual)
            return conn, False, True
        except Exception as e:
            print(f"Erro inesperado ao buscar/gravar produtos (página {pagina_atual}): {e}")
            if conn and conn.closed == 0: 
                try: conn.rollback(); print("Rollback da transação de produtos realizado.")
                except psycopg2.Error as rb_error: print(f"Erro no rollback de produtos: {rb_error}")
            salvar_progresso_paginacao_produtos(pagina_atual)
            return conn, False, True

    if todos_produtos_processados_carga_completa:
        print("Todos os produtos da carga completa foram processados.")
        return conn, True, False # Sucesso, nenhum produto restante
    elif paginas_processadas_neste_lote >= PAGINAS_POR_LOTE_PRODUTOS:
        print(f"Lote de {PAGINAS_POR_LOTE_PRODUTOS} páginas de produtos processado. Ainda podem existir mais produtos.")
        return conn, True, True # Sucesso neste lote, mas há mais produtos
    else: # Saiu do while por outro motivo (ex: erro antes de completar o lote, mas não erro 22)
        print("Processamento de produtos (carga completa) interrompido antes de completar o lote ou finalizar todas as páginas.")
        return conn, True, True # Sucesso parcial, mas podem haver mais produtos

def buscar_e_gravar_detalhe_produto(conn, api_token, id_produto_tiny):
    """Busca detalhes de um produto específico e insere/atualiza no banco."""
    print(f"Buscando detalhes do produto ID: {id_produto_tiny}...")
    endpoint_detalhe = f"{TINY_API_V2_BASE_URL}/produto.obter.php"
    payload = {"token": api_token, "formato": "json", "id": id_produto_tiny}
    response = None
    try:
        response = requests.post(endpoint_detalhe, data=payload, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=30)
        response.raise_for_status()
        data = response.json()

        if data.get("retorno", {}).get("status") == "Erro":
            # Tratamento para quando a API retorna um erro mas com status 200 OK HTTP
            # Ex: produto não encontrado, retorna erro na estrutura JSON
            erros = data.get("retorno", {}).get("erros", [])
            print(f"Erro da API Tiny ao obter detalhes do produto ID {id_produto_tiny}: {erros}")
            # Se o produto não existe mais no Tiny, não é um erro fatal para o script, apenas logamos.
            # Poderíamos adicionar uma lógica para marcar o produto como inativo no nosso banco se necessário.
            return conn, True # Considera sucesso para não parar a sincronização por um produto que pode ter sido excluído no Tiny

        produto_detalhe = data.get("retorno", {}).get("produto")
        if not produto_detalhe:
            # Resposta inesperada, sem o nó 'produto'
            print(f"AVISO: Detalhes do produto ID {id_produto_tiny} não encontrados na resposta da API ou formato inesperado: {data}")
            # Isso pode acontecer se a API retornar um texto simples de erro em vez de JSON estruturado
            # Ex: "Token invalido ou nao informado" (mas isso deveria ser pego antes)
            # Ou se o produto foi excluído e a API retorna algo diferente de um erro JSON estruturado.
            if response and response.text and "token invalido" in response.text.lower():
                 print("ERRO FATAL: Token da API Tiny inválido ou não informado. Verifique a configuração.")
                 return conn, False # Erro fatal, token inválido
            return conn, True # Considera sucesso para não parar por um produto problemático

        # Extração dos campos do produto
        id_prod = produto_detalhe.get("id")
        nome = produto_detalhe.get("nome")
        sku = produto_detalhe.get("codigo") # SKU é 'codigo' na API
        preco_venda = produto_detalhe.get("preco")
        unidade = produto_detalhe.get("unidade")
        situacao = produto_detalhe.get("situacao")
        preco_custo = produto_detalhe.get("preco_custo")
        tipo_produto = produto_detalhe.get("tipo") # 'P' para Produto, 'S' para Serviço, etc.
        estoque_atual = produto_detalhe.get("saldo") # Estoque é 'saldo'
        id_categoria_api = produto_detalhe.get("id_categoria")
        id_categoria_db = None
        if id_categoria_api:
            try: id_categoria_db = int(id_categoria_api)
            except ValueError: print(f"AVISO: id_categoria '{id_categoria_api}' do produto {id_prod} não é um inteiro válido.")

        if not id_prod or not nome:
            print(f"AVISO: Produto com ID ou Nome ausente nos detalhes ignorado: {produto_detalhe}")
            return conn, True
        
        print(f"Inserindo/Atualizando produto detalhado ID: {id_prod}, Nome: {nome[:30]}...")
        cursor = None
        try:
            conn = garantir_conexao(conn)
            if conn is None: return conn, False
            cursor = conn.cursor()
            upsert_query = """
            INSERT INTO produtos (id_produto_tiny, nome_produto, sku, preco_venda, unidade, situacao, 
                                preco_custo, tipo_produto, estoque_atual, id_categoria_produto_tiny, data_atualizacao)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (id_produto_tiny)
            DO UPDATE SET
                nome_produto = EXCLUDED.nome_produto,
                sku = EXCLUDED.sku,
                preco_venda = EXCLUDED.preco_venda,
                unidade = EXCLUDED.unidade,
                situacao = EXCLUDED.situacao,
                preco_custo = EXCLUDED.preco_custo,
                tipo_produto = EXCLUDED.tipo_produto,
                estoque_atual = EXCLUDED.estoque_atual,
                id_categoria_produto_tiny = EXCLUDED.id_categoria_produto_tiny,
                data_atualizacao = CURRENT_TIMESTAMP;
            """
            cursor.execute(upsert_query, (id_prod, nome, sku, preco_venda, unidade, situacao, 
                                        preco_custo, tipo_produto, estoque_atual, id_categoria_db))
            # conn.commit() # Commit será feito após um lote de produtos ou ao final da página
            time.sleep(PAUSA_ENTRE_CHAMADAS_DETALHE)
            return conn, True
        except (Exception, psycopg2.Error) as error:
            print(f"Erro ao inserir/atualizar produto ID {id_prod}: {error}")
            # Não faz rollback aqui, deixa para a função chamadora (buscar_e_gravar_produtos)
            return conn, False
        finally:
            if cursor: cursor.close()

    except requests.exceptions.HTTPError as http_err:
        # Erros HTTP como 401, 403, 404, 500 etc.
        print(f"Erro HTTP ao obter detalhes do produto ID {id_produto_tiny}: {http_err}")
        if response is not None and response.status_code == 404:
            print(f"Produto ID {id_produto_tiny} não encontrado na API Tiny (404). Pode ter sido excluído.")
            return conn, True # Considera sucesso para não parar a sincronização
        return conn, False # Outros erros HTTP são considerados falha
    except requests.exceptions.RequestException as e:
        print(f"Erro de requisição ao obter detalhes do produto ID {id_produto_tiny}: {e}")
        return conn, False
    except requests.exceptions.JSONDecodeError as e:
        print(f"Erro ao decodificar JSON de detalhes do produto ID {id_produto_tiny}: {e}. Conteúdo: {response.text if response else 'Sem resposta'}")
        # Se o conteúdo for um texto simples de erro da API, tentar identificar
        if response and response.text and ("produto nao encontrado" in response.text.lower() or "produto não encontrado" in response.text.lower()):
            print(f"Produto ID {id_produto_tiny} não encontrado (texto na resposta). Pode ter sido excluído.")
            return conn, True # Sucesso, pois o produto não existe mais
        return conn, False
    except Exception as e:
        print(f"Erro inesperado ao obter detalhes do produto ID {id_produto_tiny}: {e}")
        return conn, False

def buscar_e_gravar_vendedores(conn, api_token):
    print("\n--- Iniciando Sincronização de Vendedores (Contatos) ---")
    conn = garantir_conexao(conn)
    if conn is None: return conn, False

    pagina_atual = 1
    todos_vendedores_processados = False

    while not todos_vendedores_processados:
        print(f"Buscando lista de vendedores (página {pagina_atual})...")
        # Na API v2, vendedores são contatos do tipo "V" (Vendedor)
        # O endpoint contato.pesquisar.php parece não estar funcionando (404), 
        # vamos usar o genérico pesquisar.contatos.php e filtrar pelo tipo.
        # Se o endpoint específico voltar a funcionar, ajustar aqui.
        endpoint_vendedores = f"{TINY_API_V2_BASE_URL}/contatos.pesquisar.php" 
        payload = {"token": api_token, "formato": "json", "pagina": pagina_atual, "tipoPessoa": "V"} # Filtrando por tipo Vendedor
        response = None
        try:
            response = requests.post(endpoint_vendedores, data=payload, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=60)
            response.raise_for_status()
            data = response.json()

            if data.get("retorno", {}).get("status") == "Erro":
                erros = data.get("retorno", {}).get("erros", [])
                codigo_erro = None
                mensagem_erro_completa = str(erros)
                if isinstance(erros, list) and len(erros) > 0 and isinstance(erros[0], dict):
                    codigo_erro = erros[0].get("erro", {}).get("codigo_erro")
                elif isinstance(erros, dict):
                    codigo_erro = erros.get("erro", {}).get("codigo_erro")
                elif isinstance(erros, str):
                     if "Codigo: 22" in erros or "pagina nao encontrada" in erros.lower() or "página não encontrada" in erros.lower():
                        codigo_erro = "22"
                
                if not codigo_erro and ("pagina nao encontrada" in mensagem_erro_completa.lower() or "página não encontrada" in mensagem_erro_completa.lower()):
                    match = re.search(r"Codigo:\s*(\d+)", mensagem_erro_completa)
                    if match and match.group(1) == "22":
                        codigo_erro = "22"
                    elif "página não encontrada" in mensagem_erro_completa.lower() or "pagina nao encontrada" in mensagem_erro_completa.lower():
                        codigo_erro = "22"

                if codigo_erro == "22":
                    print(f"API de vendedores: Nenhuma página adicional encontrada (Erro 22 na página {pagina_atual}).")
                    todos_vendedores_processados = True
                    break
                else:
                    print(f"Erro da API Tiny ao listar vendedores (página {pagina_atual}): {erros}")
                    return conn, False # Erro na API

            vendedores_da_pagina = data.get("retorno", {}).get("contatos", [])
            if not vendedores_da_pagina:
                print(f"Fim da lista de vendedores (página {pagina_atual} vazia).")
                todos_vendedores_processados = True
                break

            print(f"Encontrados {len(vendedores_da_pagina)} contatos (potenciais vendedores) na página {pagina_atual}.")
            cursor = None
            try:
                conn = garantir_conexao(conn)
                if conn is None: return conn, False
                cursor = conn.cursor()
                for contato_wrapper in vendedores_da_pagina:
                    contato = contato_wrapper.get("contato")
                    if not contato or contato.get("tipo_pessoa") != "V": # Confirma se é vendedor
                        # print(f"Contato ignorado (não é vendedor ou dados ausentes): {contato.get('id') if contato else 'N/A'}")
                        continue

                    id_vendedor = contato.get("id")
                    nome_vendedor = contato.get("nome")
                    situacao_vendedor = contato.get("situacao_vendedor", "Ativo") # Assumir Ativo se não informado
                    
                    if not id_vendedor or not nome_vendedor:
                        print(f"AVISO: Vendedor com ID ou Nome ausente ignorado: {contato}")
                        continue

                    print(f"Inserindo/Atualizando vendedor ID: {id_vendedor}, Nome: {nome_vendedor}")
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
                    time.sleep(PAUSA_ENTRE_CHAMADAS_DETALHE) # Pausa após cada detalhe/UPSERT
                
                conn.commit()
                print(f"Commit realizado para vendedores da página {pagina_atual}.")

            except (Exception, psycopg2.Error) as error:
                print(f"Erro ao inserir/atualizar vendedores da página {pagina_atual}: {error}")
                if conn and conn.closed == 0: 
                    try: conn.rollback(); print("Rollback da transação de vendedores realizado.")
                    except psycopg2.Error as rb_error: print(f"Erro no rollback de vendedores: {rb_error}")
                return conn, False
            finally:
                if cursor: cursor.close()
            
            pagina_atual += 1
            time.sleep(PAUSA_ENTRE_PAGINAS)

        except requests.exceptions.RequestException as e:
            print(f"Erro de requisição ao listar vendedores (página {pagina_atual}): {e}")
            return conn, False
        except requests.exceptions.JSONDecodeError as e:
            print(f"Erro ao decodificar JSON de vendedores (página {pagina_atual}): {e}. Conteúdo: {response.text if response else 'Sem resposta'}")
            return conn, False
        except Exception as e:
            print(f"Erro inesperado ao buscar/gravar vendedores (página {pagina_atual}): {e}")
            if conn and conn.closed == 0: 
                try: conn.rollback(); print("Rollback da transação de vendedores realizado.")
                except psycopg2.Error as rb_error: print(f"Erro no rollback de vendedores: {rb_error}")
            return conn, False

    print("Sincronização de vendedores concluída.")
    return conn, True

def buscar_e_gravar_pedidos_e_itens(conn, api_token):
    print("\n--- Iniciando Sincronização de Pedidos e Itens de Pedido ---")
    conn = garantir_conexao(conn)
    if conn is None: return conn, False

    pagina_atual = 1
    todos_pedidos_processados = False
    pedidos_processados_total = 0
    itens_processados_total = 0

    # Tenta a busca incremental primeiro
    ultimo_timestamp_pedidos_str = ler_timestamp_progresso(ARQUIVO_TIMESTAMP_PEDIDOS)

    if ultimo_timestamp_pedidos_str:
        print(f"Modo incremental para pedidos ativado. Buscando alterações desde: {ultimo_timestamp_pedidos_str}")
        endpoint_pedidos_lista = f"{TINY_API_V2_BASE_URL}/pedidos.pesquisar.php"
        payload_base = {"token": api_token, "formato": "json", "data_alteracao_inicial": ultimo_timestamp_pedidos_str}
        # A API de pedidos.pesquisar.php usa data_alteracao_inicial e data_alteracao_final.
        # Para buscar "desde", podemos omitir data_alteracao_final ou usar uma data futura distante.
        # Por simplicidade, vamos omitir por enquanto e ver como a API se comporta.
        # Se necessário, adicionar data_alteracao_final = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        pagina_api_incremental = 1

        while True: # Loop para paginação da busca incremental de pedidos
            payload = payload_base.copy()
            if pagina_api_incremental > 1:
                payload["pagina"] = pagina_api_incremental
            
            print(f"Buscando pedidos alterados (página {pagina_api_incremental}) desde {ultimo_timestamp_pedidos_str}...")
            response = None
            try:
                response = requests.post(endpoint_pedidos_lista, data=payload, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=120) # Timeout maior para pedidos
                response.raise_for_status()
                data = response.json()

                if data.get("retorno", {}).get("status") == "Erro":
                    erros = data.get("retorno", {}).get("erros", [])
                    codigo_erro = None
                    mensagem_erro_completa = str(erros)
                    if isinstance(erros, list) and len(erros) > 0 and isinstance(erros[0], dict):
                        codigo_erro = erros[0].get("erro", {}).get("codigo_erro")
                    elif isinstance(erros, dict):
                        codigo_erro = erros.get("erro", {}).get("codigo_erro")
                    elif isinstance(erros, str):
                        if "Codigo: 22" in erros or "pagina nao encontrada" in erros.lower() or "página não encontrada" in erros.lower():
                            codigo_erro = "22"
                    
                    if not codigo_erro and ("pagina nao encontrada" in mensagem_erro_completa.lower() or "página não encontrada" in mensagem_erro_completa.lower()):
                        match = re.search(r"Codigo:\s*(\d+)", mensagem_erro_completa)
                        if match and match.group(1) == "22":
                            codigo_erro = "22"
                        elif "página não encontrada" in mensagem_erro_completa.lower() or "pagina nao encontrada" in mensagem_erro_completa.lower():
                             codigo_erro = "22"

                    if codigo_erro == "22":
                        print(f"API de pedidos alterados: Nenhuma página adicional encontrada (Erro 22 na página {pagina_api_incremental}).")
                        break # Sai do loop de paginação incremental de pedidos
                    else:
                        print(f"Erro da API Tiny ao listar pedidos alterados (página {pagina_api_incremental}): {erros}")
                        return conn, False # Erro na API

                pedidos_da_pagina = data.get("retorno", {}).get("pedidos", [])
                if not pedidos_da_pagina and pagina_api_incremental == 1:
                    print("Nenhum pedido alterado encontrado desde o último timestamp.")
                    salvar_timestamp_progresso(ARQUIVO_TIMESTAMP_PEDIDOS, datetime.now(timezone.utc))
                    return conn, True
                if not pedidos_da_pagina:
                    print(f"Fim da lista de pedidos alterados (página {pagina_api_incremental} vazia).")
                    break # Sai do loop de paginação incremental de pedidos

                print(f"Encontrados {len(pedidos_da_pagina)} pedidos alterados na página {pagina_api_incremental}.")
                conn, success_page, p_proc, i_proc = processar_pagina_de_pedidos(conn, api_token, pedidos_da_pagina, "incremental")
                pedidos_processados_total += p_proc
                itens_processados_total += i_proc
                if not success_page:
                    print(f"Falha ao processar página {pagina_api_incremental} de pedidos alterados. Abortando sincronização de pedidos.")
                    return conn, False
                
                conn.commit()
                print(f"Commit realizado para pedidos alterados da página {pagina_api_incremental}.")
                time.sleep(PAUSA_ENTRE_PAGINAS)
                pagina_api_incremental += 1

            except requests.exceptions.RequestException as e:
                print(f"Erro de requisição ao listar pedidos alterados (página {pagina_api_incremental}): {e}")
                return conn, False
            except requests.exceptions.JSONDecodeError as e:
                print(f"Erro ao decodificar JSON de pedidos alterados (página {pagina_api_incremental}): {e}. Conteúdo: {response.text if response else 'Sem resposta'}")
                return conn, False
            except Exception as e:
                print(f"Erro inesperado ao buscar/gravar pedidos alterados (página {pagina_api_incremental}): {e}")
                if conn and conn.closed == 0: 
                    try: conn.rollback(); print("Rollback da transação de pedidos alterados realizado.")
                    except psycopg2.Error as rb_error: print(f"Erro no rollback de pedidos alterados: {rb_error}")
                return conn, False
        
        salvar_timestamp_progresso(ARQUIVO_TIMESTAMP_PEDIDOS, datetime.now(timezone.utc))
        print(f"Sincronização incremental de pedidos concluída. Total de {pedidos_processados_total} pedidos e {itens_processados_total} itens processados.")
        return conn, True

    # Se não há timestamp, executa a carga completa
    print(f"Modo de carga completa para pedidos ativado. Iniciando da página {pagina_atual}.")
    endpoint_pedidos_lista_completa = f"{TINY_API_V2_BASE_URL}/pedidos.pesquisar.php"

    while not todos_pedidos_processados:
        print(f"Buscando lista de pedidos (carga completa, página {pagina_atual})...")
        payload = {"token": api_token, "formato": "json", "pagina": pagina_atual}
        response = None
        try:
            response = requests.post(endpoint_pedidos_lista_completa, data=payload, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=120)
            response.raise_for_status()
            data = response.json()

            if data.get("retorno", {}).get("status") == "Erro":
                erros = data.get("retorno", {}).get("erros", [])
                codigo_erro = None
                mensagem_erro_completa = str(erros)
                if isinstance(erros, list) and len(erros) > 0 and isinstance(erros[0], dict):
                    codigo_erro = erros[0].get("erro", {}).get("codigo_erro")
                elif isinstance(erros, dict):
                    codigo_erro = erros.get("erro", {}).get("codigo_erro")
                elif isinstance(erros, str):
                    if "Codigo: 22" in erros or "pagina nao encontrada" in erros.lower() or "página não encontrada" in erros.lower():
                        codigo_erro = "22"
                
                if not codigo_erro and ("pagina nao encontrada" in mensagem_erro_completa.lower() or "página não encontrada" in mensagem_erro_completa.lower()):
                    match = re.search(r"Codigo:\s*(\d+)", mensagem_erro_completa)
                    if match and match.group(1) == "22":
                        codigo_erro = "22"
                    elif "página não encontrada" in mensagem_erro_completa.lower() or "pagina nao encontrada" in mensagem_erro_completa.lower():
                        codigo_erro = "22"

                if codigo_erro == "22":
                    print(f"API de pedidos (carga completa): Nenhuma página adicional encontrada (Erro 22 na página {pagina_atual}).")
                    todos_pedidos_processados = True
                    break
                else:
                    print(f"Erro da API Tiny ao listar pedidos (carga completa, página {pagina_atual}): {erros}")
                    return conn, False # Erro na API

            pedidos_da_pagina = data.get("retorno", {}).get("pedidos", [])
            if not pedidos_da_pagina:
                print(f"Fim da lista de pedidos (carga completa, página {pagina_atual} vazia).")
                todos_pedidos_processados = True
                break

            print(f"Encontrados {len(pedidos_da_pagina)} pedidos na carga completa (página {pagina_atual}).")
            conn, success_page, p_proc, i_proc = processar_pagina_de_pedidos(conn, api_token, pedidos_da_pagina, "carga completa")
            pedidos_processados_total += p_proc
            itens_processados_total += i_proc
            if not success_page:
                print(f"Falha ao processar página {pagina_atual} de pedidos (carga completa). Abortando sincronização.")
                return conn, False
            
            conn.commit()
            print(f"Commit realizado para pedidos da carga completa (página {pagina_atual}).")
            pagina_atual += 1
            time.sleep(PAUSA_ENTRE_PAGINAS)

        except requests.exceptions.RequestException as e:
            print(f"Erro de requisição ao listar pedidos (carga completa, página {pagina_atual}): {e}")
            return conn, False
        except requests.exceptions.JSONDecodeError as e:
            print(f"Erro ao decodificar JSON de pedidos (carga completa, página {pagina_atual}): {e}. Conteúdo: {response.text if response else 'Sem resposta'}")
            return conn, False
        except Exception as e:
            print(f"Erro inesperado ao buscar/gravar pedidos (carga completa, página {pagina_atual}): {e}")
            if conn and conn.closed == 0: 
                try: conn.rollback(); print("Rollback da transação de pedidos (carga completa) realizado.")
                except psycopg2.Error as rb_error: print(f"Erro no rollback de pedidos (carga completa): {rb_error}")
            return conn, False

    if todos_pedidos_processados:
        salvar_timestamp_progresso(ARQUIVO_TIMESTAMP_PEDIDOS, datetime.now(timezone.utc))
        print(f"Carga completa de pedidos finalizada. Timestamp salvo. Total de {pedidos_processados_total} pedidos e {itens_processados_total} itens processados.")
    else:
        print(f"Sincronização de pedidos (carga completa) interrompida. Total de {pedidos_processados_total} pedidos e {itens_processados_total} itens processados até o momento.")
    
    return conn, True

def processar_pagina_de_pedidos(conn, api_token, pedidos_da_pagina, tipo_carga="desconhecido"):
    pedidos_processados_na_pagina = 0
    itens_processados_na_pagina = 0
    for pedido_wrapper in pedidos_da_pagina:
        pedido_lista = pedido_wrapper.get("pedido")
        if not pedido_lista or not pedido_lista.get("id"):
            print(f"AVISO (Pedidos - {tipo_carga}): Pedido da lista com dados incompletos ou sem ID ignorado: {pedido_lista}")
            continue
        
        id_pedido_tiny = pedido_lista.get("id")
        conn, success_det, itens_proc_det = buscar_e_gravar_detalhe_pedido_e_itens(conn, api_token, id_pedido_tiny, tipo_carga)
        if not success_det:
            print(f"Falha ao processar detalhes do pedido ID {id_pedido_tiny} ({tipo_carga}). Continuando com os próximos, mas a página será marcada como falha parcial.")
            # Não retorna False imediatamente para tentar outros pedidos da página, mas a função principal deve tratar isso.
            # No entanto, para simplificar, vamos considerar um erro no detalhe como erro da página por enquanto.
            return conn, False, pedidos_processados_na_pagina, itens_processados_na_pagina
        else:
            pedidos_processados_na_pagina += 1
            itens_processados_na_pagina += itens_proc_det
    return conn, True, pedidos_processados_na_pagina, itens_processados_na_pagina

def buscar_e_gravar_detalhe_pedido_e_itens(conn, api_token, id_pedido_tiny, tipo_carga="desconhecido"):
    print(f"Buscando detalhes do pedido ID: {id_pedido_tiny} ({tipo_carga})...")
    endpoint_detalhe = f"{TINY_API_V2_BASE_URL}/pedido.obter.php"
    payload = {"token": api_token, "formato": "json", "id": id_pedido_tiny}
    response = None
    itens_processados_neste_pedido = 0
    try:
        response = requests.post(endpoint_detalhe, data=payload, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=60)
        response.raise_for_status()
        data = response.json()

        if data.get("retorno", {}).get("status") == "Erro":
            erros = data.get("retorno", {}).get("erros", [])
            print(f"Erro da API Tiny ao obter detalhes do pedido ID {id_pedido_tiny} ({tipo_carga}): {erros}")
            return conn, True, 0 # Considera sucesso para não parar por um pedido que pode não existir mais

        pedido_detalhe = data.get("retorno", {}).get("pedido")
        if not pedido_detalhe:
            print(f"AVISO: Detalhes do pedido ID {id_pedido_tiny} ({tipo_carga}) não encontrados na resposta da API: {data}")
            return conn, True, 0

        # Extração dos campos do pedido
        id_pedido = pedido_detalhe.get("id")
        numero_pedido = pedido_detalhe.get("numero")
        data_pedido_str = pedido_detalhe.get("data_pedido")
        data_pedido_db = format_date_to_db(data_pedido_str)
        
        id_lista_preco = pedido_detalhe.get("id_lista_preco")
        desc_lista_preco = pedido_detalhe.get("descricao_lista_preco")
        nome_cliente = pedido_detalhe.get("cliente", {}).get("nome")
        cpf_cnpj_cliente = pedido_detalhe.get("cliente", {}).get("cpf_cnpj")
        email_cliente = pedido_detalhe.get("cliente", {}).get("email")
        fone_cliente = pedido_detalhe.get("cliente", {}).get("fone")
        
        id_vendedor_api = pedido_detalhe.get("id_vendedor")
        id_vendedor_db = None
        if id_vendedor_api:
            try: id_vendedor_db = int(id_vendedor_api)
            except ValueError: print(f"AVISO (Pedido {id_pedido}): id_vendedor '{id_vendedor_api}' não é um inteiro válido.")
        nome_vendedor_pedido = pedido_detalhe.get("nome_vendedor") # Campo novo

        situacao_pedido = pedido_detalhe.get("situacao")
        valor_total_pedido = pedido_detalhe.get("total_pedido")
        valor_desconto_pedido = pedido_detalhe.get("valor_desconto")
        condicao_pagamento = pedido_detalhe.get("condicao_pagamento")
        forma_pagamento_api = pedido_detalhe.get("forma_pagamento") # Pode ser um dict
        forma_pagamento_db = forma_pagamento_api
        if isinstance(forma_pagamento_api, dict):
            forma_pagamento_db = forma_pagamento_api.get("descricao_forma_pagamento", str(forma_pagamento_api))

        if not id_pedido:
            print(f"AVISO (Pedido - {tipo_carga}): Pedido com ID ausente nos detalhes ignorado: {pedido_detalhe}")
            return conn, True, 0

        print(f"Inserindo/Atualizando pedido detalhado ID: {id_pedido}, Numero: {numero_pedido} ({tipo_carga})")
        cursor = None
        try:
            conn = garantir_conexao(conn)
            if conn is None: return conn, False, 0
            cursor = conn.cursor()
            upsert_pedido_query = """
            INSERT INTO pedidos (id_pedido_tiny, numero_pedido_tiny, data_pedido, id_lista_preco, 
                                descricao_lista_preco, nome_cliente, cpf_cnpj_cliente, email_cliente, 
                                fone_cliente, id_vendedor, nome_vendedor_pedido, situacao_pedido, valor_total_pedido, 
                                valor_desconto_pedido, condicao_pagamento, forma_pagamento, data_atualizacao)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (id_pedido_tiny)
            DO UPDATE SET
                numero_pedido_tiny = EXCLUDED.numero_pedido_tiny, data_pedido = EXCLUDED.data_pedido,
                id_lista_preco = EXCLUDED.id_lista_preco, descricao_lista_preco = EXCLUDED.descricao_lista_preco,
                nome_cliente = EXCLUDED.nome_cliente, cpf_cnpj_cliente = EXCLUDED.cpf_cnpj_cliente,
                email_cliente = EXCLUDED.email_cliente, fone_cliente = EXCLUDED.fone_cliente,
                id_vendedor = EXCLUDED.id_vendedor, nome_vendedor_pedido = EXCLUDED.nome_vendedor_pedido,
                situacao_pedido = EXCLUDED.situacao_pedido, valor_total_pedido = EXCLUDED.valor_total_pedido,
                valor_desconto_pedido = EXCLUDED.valor_desconto_pedido, condicao_pagamento = EXCLUDED.condicao_pagamento,
                forma_pagamento = EXCLUDED.forma_pagamento, data_atualizacao = CURRENT_TIMESTAMP;
            """
            cursor.execute(upsert_pedido_query, (
                id_pedido, numero_pedido, data_pedido_db, id_lista_preco, desc_lista_preco, nome_cliente,
                cpf_cnpj_cliente, email_cliente, fone_cliente, id_vendedor_db, nome_vendedor_pedido, situacao_pedido,
                valor_total_pedido, valor_desconto_pedido, condicao_pagamento, forma_pagamento_db
            ))

            # Processar itens do pedido
            itens_do_pedido = pedido_detalhe.get("itens", [])
            if itens_do_pedido:
                # Primeiro, removemos itens antigos para este pedido para garantir consistência
                # Isso é importante se um item foi removido do pedido no Tiny
                delete_itens_query = "DELETE FROM itens_pedido WHERE id_pedido_tiny = %s;"
                cursor.execute(delete_itens_query, (id_pedido,))
                print(f"Itens antigos do pedido ID {id_pedido} removidos antes de inserir os novos.")

                for item_wrapper in itens_do_pedido:
                    item = item_wrapper.get("item")
                    if not item:
                        print(f"AVISO (Pedido {id_pedido} - {tipo_carga}): Item de pedido em formato inválido ignorado: {item_wrapper}")
                        continue
                    
                    id_produto_item_api = item.get("id_produto")
                    id_produto_item_db = None
                    if id_produto_item_api:
                        try: id_produto_item_db = int(id_produto_item_api)
                        except ValueError: print(f"AVISO (Pedido {id_pedido}, Item SKU {item.get('codigo')}): id_produto '{id_produto_item_api}' não é um inteiro válido.")

                    sku_produto_item = item.get("codigo") # SKU
                    descricao_item = item.get("descricao")
                    unidade_item = item.get("unidade")
                    quantidade_item = item.get("quantidade")
                    valor_unitario_item = item.get("valor_unitario")
                    # valor_total_item = item.get("valor_total") # API não parece fornecer, calculamos se necessário
                    valor_total_calculado = None
                    if quantidade_item is not None and valor_unitario_item is not None:
                        try: valor_total_calculado = float(quantidade_item) * float(valor_unitario_item)
                        except ValueError: print(f"AVISO (Pedido {id_pedido}, Item {descricao_item}): Não foi possível calcular valor total do item.")
                    
                    if not descricao_item: # SKU pode ser nulo para itens manuais
                        print(f"AVISO (Pedido {id_pedido} - {tipo_carga}): Item de pedido sem descrição ignorado: {item}")
                        continue

                    insert_item_query = """
                    INSERT INTO itens_pedido (id_pedido_tiny, id_produto_tiny, sku_produto, descricao_item, 
                                            unidade_item, quantidade_item, valor_unitario_item, valor_total_item, 
                                            data_atualizacao)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP);
                    """
                    cursor.execute(insert_item_query, (
                        id_pedido, id_produto_item_db, sku_produto_item, descricao_item, unidade_item,
                        quantidade_item, valor_unitario_item, valor_total_calculado
                    ))
                    itens_processados_neste_pedido += 1
                print(f"{itens_processados_neste_pedido} itens processados para o pedido ID {id_pedido} ({tipo_carga}).")
            
            # conn.commit() # Commit será feito pela função chamadora (processar_pagina_de_pedidos)
            time.sleep(PAUSA_ENTRE_CHAMADAS_DETALHE)
            return conn, True, itens_processados_neste_pedido

        except (Exception, psycopg2.Error) as error:
            print(f"Erro ao inserir/atualizar pedido ID {id_pedido} ou seus itens ({tipo_carga}): {error}")
            # Rollback será feito pela função principal se necessário
            return conn, False, itens_processados_neste_pedido # Retorna o que foi processado até o erro
        finally:
            if cursor: cursor.close()

    except requests.exceptions.HTTPError as http_err:
        print(f"Erro HTTP ao obter detalhes do pedido ID {id_pedido_tiny} ({tipo_carga}): {http_err}")
        if response is not None and response.status_code == 404:
            print(f"Pedido ID {id_pedido_tiny} ({tipo_carga}) não encontrado na API Tiny (404). Pode ter sido excluído.")
            return conn, True, 0
        return conn, False, 0
    except requests.exceptions.RequestException as e:
        print(f"Erro de requisição ao obter detalhes do pedido ID {id_pedido_tiny} ({tipo_carga}): {e}")
        return conn, False, 0
    except requests.exceptions.JSONDecodeError as e:
        print(f"Erro ao decodificar JSON de detalhes do pedido ID {id_pedido_tiny} ({tipo_carga}): {e}. Conteúdo: {response.text if response else 'Sem resposta'}")
        if response and response.text and ("pedido nao encontrado" in response.text.lower() or "pedido não encontrado" in response.text.lower()):
            print(f"Pedido ID {id_pedido_tiny} ({tipo_carga}) não encontrado (texto na resposta). Pode ter sido excluído.")
            return conn, True, 0
        return conn, False, 0
    except Exception as e:
        print(f"Erro inesperado ao obter detalhes do pedido ID {id_pedido_tiny} ({tipo_carga}): {e}")
        return conn, False, 0

def main():
    print("Iniciando script de integração Tiny ERP -> PostgreSQL...")
    start_time = time.time()

    conn = conectar_db()
    if conn is None:
        print("Falha ao conectar ao banco de dados. Abortando script.")
        return

    # Criar/Verificar tabelas
    conn, success_cat_tbl = criar_tabela_categorias(conn)
    conn, success_prod_tbl = criar_tabela_produtos(conn)
    conn, success_vend_tbl = criar_tabela_vendedores(conn)
    conn, success_ped_tbl = criar_tabela_pedidos(conn)
    conn, success_item_ped_tbl = criar_tabela_itens_pedido(conn)

    if not (success_cat_tbl and success_prod_tbl and success_vend_tbl and success_ped_tbl and success_item_ped_tbl):
        print("Falha ao criar uma ou mais tabelas. Verifique os logs. Abortando script.")
        if conn and conn.closed == 0: conn.close()
        return
    
    # Sincronizar Categorias
    conn, success_sync_cat = buscar_e_gravar_categorias(conn, TINY_API_V2_TOKEN)
    if not success_sync_cat:
        print("Sincronização de categorias falhou. Verifique os logs.")
        # Continuar mesmo se categorias falharem? Decidir a estratégia.
        # Por ora, vamos continuar para tentar sincronizar o resto.

    # Sincronizar Produtos (com lógica de lote e retomada para carga completa, e incremental)
    conn, success_sync_prod, produtos_ainda_restantes = buscar_e_gravar_produtos(conn, TINY_API_V2_TOKEN)
    if not success_sync_prod:
        print("Sincronização de produtos falhou ou foi parcial neste lote. Verifique os logs.")
    if produtos_ainda_restantes:
        print("Ainda existem produtos a serem processados na carga completa. Execute o script novamente para continuar.")
        # Se há produtos restantes na carga completa, não processa o resto para não sobrecarregar
        # e para garantir que os produtos estejam minimamente atualizados antes dos pedidos.
        if conn and conn.closed == 0: conn.close()
        end_time = time.time()
        print(f"Script concluído (parcialmente devido a produtos restantes). Tempo total: {end_time - start_time:.2f} segundos.")
        return 
    # Se success_sync_prod é True e produtos_ainda_restantes é False, então todos os produtos (carga completa ou incremental) foram processados.

    # Sincronizar Vendedores
    conn, success_sync_vend = buscar_e_gravar_vendedores(conn, TINY_API_V2_TOKEN)
    if not success_sync_vend:
        print("Sincronização de vendedores falhou. Verifique os logs.")

    # Sincronizar Pedidos e Itens de Pedido (com lógica incremental)
    conn, success_sync_ped = buscar_e_gravar_pedidos_e_itens(conn, TINY_API_V2_TOKEN)
    if not success_sync_ped:
        print("Sincronização de pedidos e itens falhou. Verifique os logs.")

    # Fechar conexão com o banco
    if conn and conn.closed == 0:
        conn.close()
        print("Conexão com o PostgreSQL fechada.")

    end_time = time.time()
    print(f"Script de integração concluído. Tempo total: {end_time - start_time:.2f} segundos.")

if __name__ == "__main__":
    main()

