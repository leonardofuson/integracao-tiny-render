#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script Python para integrar o ERP Tiny com um banco de dados PostgreSQL no Render.
Versão Robusta - Desenvolvida para a Fusion (www.usefusion.com.br)

Funcionalidades:
- Sincronização de Categorias, Produtos (com detalhes), Vendedores, Pedidos e Itens de Pedido
- Persistência de progresso e timestamps no banco de dados PostgreSQL
- Tratamento robusto de erros e reconexão automática
- Processamento em lotes para cargas iniciais
- Sincronização incremental para produtos e pedidos

Autor: Manus AI
Data: Maio 2025
"""

import requests
import psycopg2
import json
import os
import time
import logging
from datetime import datetime, timezone
import re
import sys

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('tiny_integration')

# --- Configurações Globais ---
TINY_API_V2_BASE_URL = "https://api.tiny.com.br/api2"
TINY_API_V2_TOKEN = os.getenv("TINY_API_V2_TOKEN", "14c9d28dc8d8dcb2d7229efb1fafcf0392059b2579414c17d02eee4c821f4677")

DB_HOST = os.getenv("DB_HOST", "dpg-d0h4vjgdl3ps73cihsv0-a.oregon-postgres.render.com")
DB_NAME = os.getenv("DB_NAME", "banco_tiny_fusion")
DB_USER = os.getenv("DB_USER", "banco_tiny_fusion_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "9JFbOUjBU1f3tM10EZygXgtOp8vKoUyb")
DB_PORT = os.getenv("DB_PORT", "5432")

# Configurações de reconexão e pausas
MAX_DB_RECONNECT_ATTEMPTS = 3
DB_RECONNECT_DELAY_SECONDS = 10
PAUSA_ENTRE_CHAMADAS_API = 0.6  # Pausa geral entre chamadas à API Tiny
PAUSA_ENTRE_PAGINAS = 1.0       # Pausa adicional entre páginas

# Chaves para a tabela de controle_progresso
CHAVE_PAGINA_PRODUTOS = "ultima_pagina_produto_processada"
CHAVE_TIMESTAMP_PRODUTOS = "timestamp_sincronizacao_produtos"
CHAVE_PAGINA_PEDIDOS = "ultima_pagina_pedido_processada"
CHAVE_TIMESTAMP_PEDIDOS = "timestamp_sincronizacao_pedidos"

# Configurações de Lote
PAGINAS_POR_LOTE_PRODUTOS = 3
MAX_LOTES_PRODUTOS_POR_EXECUCAO = 10  # Para carga completa de produtos
TERMO_PESQUISA_PRODUTOS_CARGA_COMPLETA = "a"  # Termo genérico para listar produtos

PAGINAS_POR_LOTE_PEDIDOS = 3
MAX_LOTES_PEDIDOS_POR_EXECUCAO = 10  # Para carga completa de pedidos

# --- Funções de Conexão e Controle de Banco de Dados ---

def conectar_db(attempt=1):
    """Estabelece conexão com o banco de dados PostgreSQL com tentativas de reconexão."""
    conn = None
    try:
        logger.info(f"Tentando conectar ao PostgreSQL (tentativa {attempt})...")
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            connect_timeout=10
        )
        conn.autocommit = False  # Controlar transações manualmente
        logger.info("Conexão com o PostgreSQL bem-sucedida!")
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Erro ao conectar ao PostgreSQL (tentativa {attempt}): {error}")
        if attempt < MAX_DB_RECONNECT_ATTEMPTS:
            logger.info(f"Aguardando {DB_RECONNECT_DELAY_SECONDS}s para nova tentativa...")
            time.sleep(DB_RECONNECT_DELAY_SECONDS)
            return conectar_db(attempt + 1)
        else:
            logger.error("Número máximo de tentativas de reconexão ao DB atingido.")
    return conn

def garantir_conexao(conn):
    """Garante que a conexão com o banco esteja ativa, reconectando se necessário."""
    if conn is None or conn.closed != 0:
        logger.warning("Conexão com DB está fechada ou nula. Tentando reconectar...")
        conn = conectar_db()
    return conn

def criar_tabela_controle_progresso(conn):
    """Cria a tabela de controle_progresso se ela não existir."""
    cursor = None
    try:
        conn = garantir_conexao(conn)
        if conn is None: 
            logger.error("Falha ao garantir conexão para criar tabela de controle_progresso")
            return conn, False
        
        cursor = conn.cursor()
        create_query = """
        CREATE TABLE IF NOT EXISTS controle_progresso (
            chave VARCHAR(255) PRIMARY KEY,
            valor TEXT,
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );"""
        cursor.execute(create_query)
        conn.commit()
        
        # Verificar se a tabela foi realmente criada
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'controle_progresso');")
        if cursor.fetchone()[0]:
            logger.info("Tabela 'controle_progresso' verificada/criada com sucesso.")
            return conn, True
        else:
            logger.error("ERRO CRÍTICO: Tabela 'controle_progresso' não foi criada após o comando CREATE.")
            return conn, False
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Erro ao criar tabela 'controle_progresso': {error}")
        if conn and conn.closed == 0:
            try:
                conn.rollback()
                logger.info("Rollback da transação da tabela controle_progresso realizado.")
            except Exception as e:
                logger.error(f"Erro no rollback de controle_progresso: {e}")
        return conn, False
    finally:
        if cursor: 
            cursor.close()

def ler_progresso_db(conn, chave, valor_padrao=None):
    """Lê um valor da tabela controle_progresso."""
    cursor = None
    try:
        conn = garantir_conexao(conn)
        if conn is None: 
            logger.error(f"Falha ao garantir conexão para ler progresso da chave '{chave}'")
            return valor_padrao
        
        cursor = conn.cursor()
        cursor.execute("SELECT valor FROM controle_progresso WHERE chave = %s", (chave,))
        resultado = cursor.fetchone()
        if resultado and resultado[0] is not None:
            valor = resultado[0]
            logger.info(f"Valor '{valor}' lido do banco para a chave '{chave}'.")
            return valor
        else:
            logger.info(f"Chave '{chave}' não encontrada ou valor nulo no banco. Usando valor padrão: {valor_padrao}")
            return valor_padrao
    except (Exception, psycopg2.Error) as e:
        logger.error(f"Erro ao ler progresso do banco para chave '{chave}': {e}. Usando valor padrão: {valor_padrao}")
        if conn and conn.closed == 0:
            try:
                conn.rollback()
            except Exception as rb_error:
                logger.error(f"Erro ao tentar rollback em ler_progresso_db: {rb_error}")
        return valor_padrao
    finally:
        if cursor: 
            cursor.close()

def salvar_progresso_db(conn, chave, valor):
    """Salva (insere ou atualiza) um valor na tabela controle_progresso."""
    cursor = None
    try:
        conn = garantir_conexao(conn)
        if conn is None: 
            logger.error(f"Falha ao garantir conexão para salvar progresso da chave '{chave}'")
            return False
        
        cursor = conn.cursor()
        upsert_query = """
        INSERT INTO controle_progresso (chave, valor, data_atualizacao) 
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (chave) 
        DO UPDATE SET valor = EXCLUDED.valor, data_atualizacao = CURRENT_TIMESTAMP;"""
        cursor.execute(upsert_query, (chave, str(valor)))
        conn.commit()
        logger.info(f"Progresso salvo no banco: chave='{chave}', valor='{valor}'.")
        return True
    except (Exception, psycopg2.Error) as e:
        logger.error(f"Erro ao salvar progresso no banco para chave '{chave}': {e}")
        if conn and conn.closed == 0:
            try:
                conn.rollback()
            except Exception as rb_error:
                logger.error(f"Erro ao tentar rollback em salvar_progresso_db: {rb_error}")
        return False
    finally:
        if cursor: 
            cursor.close()

# --- Funções de Limpeza e Verificação de Tabelas ---

def verificar_e_limpar_constraints(conn):
    """Verifica e limpa constraints conflitantes antes de criar as tabelas."""
    cursor = None
    try:
        conn = garantir_conexao(conn)
        if conn is None: 
            logger.error("Falha ao garantir conexão para verificar constraints")
            return conn, False
        
        cursor = conn.cursor()
        
        # Verificar constraints existentes
        logger.info("Verificando constraints existentes...")
        cursor.execute("""
        SELECT tc.constraint_name, tc.table_name, kcu.column_name, 
               ccu.table_name AS foreign_table_name,
               ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
        LEFT JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY';
        """)
        
        constraints = cursor.fetchall()
        constraints_problematicas = []
        
        # Identificar constraints problemáticas (com nomes de coluna inconsistentes)
        for constraint in constraints:
            constraint_name, table_name, column_name, foreign_table, foreign_column = constraint
            
            # Verificar se há inconsistência entre id_vendedor e id_vendedor_tiny
            if (table_name == 'pedidos' and column_name == 'id_vendedor' and 
                foreign_table == 'vendedores' and foreign_column == 'id_vendedor_tiny'):
                constraints_problematicas.append(constraint_name)
                logger.warning(f"Constraint inconsistente encontrada: {constraint_name} ({table_name}.{column_name} -> {foreign_table}.{foreign_column})")
        
        # Remover constraints problemáticas
        if constraints_problematicas:
            logger.info(f"Removendo {len(constraints_problematicas)} constraints inconsistentes...")
            for constraint_name in constraints_problematicas:
                logger.info(f"Removendo constraint: {constraint_name}")
                cursor.execute(f"ALTER TABLE pedidos DROP CONSTRAINT IF EXISTS {constraint_name};")
            
            conn.commit()
            logger.info("Constraints inconsistentes removidas com sucesso.")
        else:
            logger.info("Nenhuma constraint inconsistente encontrada.")
        
        return conn, True
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Erro ao verificar/limpar constraints: {error}")
        if conn and conn.closed == 0:
            try:
                conn.rollback()
                logger.info("Rollback da transação de verificação de constraints realizado.")
            except Exception as e:
                logger.error(f"Erro ao tentar rollback em verificar_e_limpar_constraints: {e}")
        return conn, False
    finally:
        if cursor: 
            cursor.close()

def verificar_e_corrigir_colunas_pedidos(conn):
    """Verifica e corrige a coluna id_vendedor para id_vendedor_tiny na tabela pedidos."""
    cursor = None
    try:
        conn = garantir_conexao(conn)
        if conn is None: 
            logger.error("Falha ao garantir conexão para verificar colunas da tabela pedidos")
            return conn, False
        
        cursor = conn.cursor()
        
        # Verificar se a tabela pedidos existe
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'pedidos');")
        tabela_existe = cursor.fetchone()[0]
        
        if not tabela_existe:
            logger.info("Tabela 'pedidos' não existe. Nenhuma correção necessária.")
            return conn, True
        
        # Verificar se a coluna id_vendedor existe e id_vendedor_tiny não existe
        cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'pedidos' AND column_name IN ('id_vendedor', 'id_vendedor_tiny');
        """)
        
        colunas = [col[0] for col in cursor.fetchall()]
        
        if 'id_vendedor' in colunas and 'id_vendedor_tiny' not in colunas:
            logger.warning("Coluna inconsistente encontrada: 'id_vendedor' em vez de 'id_vendedor_tiny'")
            logger.info("Renomeando coluna 'id_vendedor' para 'id_vendedor_tiny'...")
            
            # Remover constraints primeiro
            cursor.execute("""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_name = 'pedidos' AND constraint_type = 'FOREIGN KEY';
            """)
            
            constraints = [con[0] for con in cursor.fetchall()]
            for constraint in constraints:
                cursor.execute(f"ALTER TABLE pedidos DROP CONSTRAINT IF EXISTS {constraint};")
            
            # Renomear a coluna
            cursor.execute("ALTER TABLE pedidos RENAME COLUMN id_vendedor TO id_vendedor_tiny;")
            
            conn.commit()
            logger.info("Coluna renomeada com sucesso.")
        elif 'id_vendedor_tiny' in colunas:
            logger.info("Coluna 'id_vendedor_tiny' já existe na tabela 'pedidos'. Nenhuma correção necessária.")
        else:
            logger.info("Nenhuma das colunas 'id_vendedor' ou 'id_vendedor_tiny' encontrada. A tabela será recriada.")
        
        return conn, True
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Erro ao verificar/corrigir colunas da tabela pedidos: {error}")
        if conn and conn.closed == 0:
            try:
                conn.rollback()
                logger.info("Rollback da transação de verificação de colunas realizado.")
            except Exception as e:
                logger.error(f"Erro ao tentar rollback em verificar_e_corrigir_colunas_pedidos: {e}")
        return conn, False
    finally:
        if cursor: 
            cursor.close()

# --- Funções de Criação de Tabelas de Dados ---

def criar_tabelas_dados(conn):
    """Cria todas as tabelas de dados (categorias, produtos, etc.) se não existirem."""
    # Primeiro, verificar e limpar constraints problemáticas
    conn, sucesso_limpeza = verificar_e_limpar_constraints(conn)
    if not sucesso_limpeza:
        logger.error("Falha ao limpar constraints problemáticas. Continuando mesmo assim...")
    
    # Verificar e corrigir colunas da tabela pedidos
    conn, sucesso_correcao = verificar_e_corrigir_colunas_pedidos(conn)
    if not sucesso_correcao:
        logger.error("Falha ao corrigir colunas da tabela pedidos. Continuando mesmo assim...")
    
    # Criar as tabelas na ordem correta
    sucesso_total = True
    tabelas_a_criar = [
        criar_tabela_categorias,
        criar_tabela_produtos,
        criar_tabela_vendedores,
        criar_tabela_pedidos,
        criar_tabela_itens_pedido
    ]
    for funcao_criar_tabela in tabelas_a_criar:
        conn, sucesso = funcao_criar_tabela(conn)
        if not sucesso:
            sucesso_total = False
            # Não adianta continuar se uma tabela base falhar
            logger.error(f"Falha crítica ao criar tabela via {funcao_criar_tabela.__name__}. Abortando criação de tabelas de dados.")
            break 
    return conn, sucesso_total

def criar_tabela_categorias(conn):
    """Cria a tabela de categorias se não existir."""
    cursor = None
    try:
        conn = garantir_conexao(conn)
        if conn is None: 
            return conn, False
        
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS categorias (
            id_categoria_tiny INT PRIMARY KEY,
            nome_categoria VARCHAR(255) NOT NULL,
            id_categoria_pai_tiny INT,
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (id_categoria_pai_tiny) REFERENCES categorias (id_categoria_tiny) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS idx_categorias_id_categoria_pai_tiny ON categorias (id_categoria_pai_tiny);
        """)
        conn.commit()
        logger.info("Tabela 'categorias' verificada/criada com sucesso.")
        return conn, True
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Erro ao criar tabela 'categorias': {error}")
        if conn and conn.closed == 0:
            try:
                conn.rollback()
                logger.info("Rollback da transação em criar_tabela_categorias realizado.")
            except Exception as e:
                logger.error(f"Erro ao tentar rollback em criar_tabela_categorias: {e}")
        return conn, False
    finally:
        if cursor: 
            cursor.close()

def criar_tabela_produtos(conn):
    """Cria a tabela de produtos se não existir."""
    cursor = None
    try:
        conn = garantir_conexao(conn)
        if conn is None: 
            return conn, False
        
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS produtos (
            id_produto_tiny INT PRIMARY KEY,
            nome_produto VARCHAR(255) NOT NULL,
            sku VARCHAR(100) UNIQUE,
            preco_venda NUMERIC(12, 2),
            preco_custo NUMERIC(12, 2),
            unidade VARCHAR(50),
            tipo_produto VARCHAR(50), 
            estoque_atual NUMERIC(10, 3),
            situacao VARCHAR(50),
            id_categoria_produto_tiny INT,
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (id_categoria_produto_tiny) REFERENCES categorias (id_categoria_tiny) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS idx_produtos_id_categoria_produto_tiny ON produtos (id_categoria_produto_tiny);
        CREATE INDEX IF NOT EXISTS idx_produtos_sku ON produtos (sku);
        """)
        conn.commit()
        logger.info("Tabela 'produtos' verificada/criada com sucesso.")
        return conn, True
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Erro ao criar tabela 'produtos': {error}")
        if conn and conn.closed == 0:
            try:
                conn.rollback()
                logger.info("Rollback da transação em criar_tabela_produtos realizado.")
            except Exception as e:
                logger.error(f"Erro ao tentar rollback em criar_tabela_produtos: {e}")
        return conn, False
    finally:
        if cursor: 
            cursor.close()

def criar_tabela_vendedores(conn):
    """Cria a tabela de vendedores se não existir."""
    cursor = None
    try:
        conn = garantir_conexao(conn)
        if conn is None: 
            return conn, False
        
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS vendedores (
            id_vendedor_tiny INT PRIMARY KEY,
            nome_vendedor VARCHAR(255) NOT NULL,
            situacao_vendedor VARCHAR(50),
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        conn.commit()
        logger.info("Tabela 'vendedores' verificada/criada com sucesso.")
        return conn, True
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Erro ao criar tabela 'vendedores': {error}")
        if conn and conn.closed == 0:
            try:
                conn.rollback()
                logger.info("Rollback da transação em criar_tabela_vendedores realizado.")
            except Exception as e:
                logger.error(f"Erro ao tentar rollback em criar_tabela_vendedores: {e}")
        return conn, False
    finally:
        if cursor: 
            cursor.close()

def criar_tabela_pedidos(conn):
    """Cria a tabela de pedidos se não existir."""
    cursor = None
    try:
        conn = garantir_conexao(conn)
        if conn is None: 
            return conn, False
        
        cursor = conn.cursor()
        cursor.execute("""
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
            id_vendedor_tiny INT,
            nome_vendedor_pedido VARCHAR(255),
            situacao_pedido VARCHAR(50),
            valor_total_pedido NUMERIC(12, 2),
            valor_desconto_pedido NUMERIC(12, 2),
            condicao_pagamento VARCHAR(255),
            forma_pagamento VARCHAR(100),
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (id_vendedor_tiny) REFERENCES vendedores (id_vendedor_tiny) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS idx_pedidos_id_vendedor_tiny ON pedidos (id_vendedor_tiny);
        CREATE INDEX IF NOT EXISTS idx_pedidos_data_pedido ON pedidos (data_pedido);
        """)
        conn.commit()
        logger.info("Tabela 'pedidos' verificada/criada com sucesso.")
        return conn, True
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Erro ao criar tabela 'pedidos': {error}")
        if conn and conn.closed == 0:
            try:
                conn.rollback()
                logger.info("Rollback da transação em criar_tabela_pedidos realizado.")
            except Exception as e:
                logger.error(f"Erro ao tentar rollback em criar_tabela_pedidos: {e}")
        return conn, False
    finally:
        if cursor: 
            cursor.close()

def criar_tabela_itens_pedido(conn):
    """Cria a tabela de itens de pedido se não existir."""
    cursor = None
    try:
        conn = garantir_conexao(conn)
        if conn is None: 
            return conn, False
        
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS itens_pedido (
            id_item_pedido_tiny VARCHAR(255) PRIMARY KEY, 
            id_pedido_tiny INT NOT NULL,
            id_produto_tiny INT,
            sku_produto VARCHAR(100),
            descricao_item VARCHAR(255),
            unidade_item VARCHAR(50),
            quantidade_item NUMERIC(10, 3),
            valor_unitario_item NUMERIC(12, 2),
            valor_total_item NUMERIC(12, 2),
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (id_pedido_tiny) REFERENCES pedidos (id_pedido_tiny) ON DELETE CASCADE,
            FOREIGN KEY (id_produto_tiny) REFERENCES produtos (id_produto_tiny) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS idx_itens_pedido_id_pedido_tiny ON itens_pedido (id_pedido_tiny);
        CREATE INDEX IF NOT EXISTS idx_itens_pedido_id_produto_tiny ON itens_pedido (id_produto_tiny);
        """)
        conn.commit()
        logger.info("Tabela 'itens_pedido' verificada/criada com sucesso.")
        return conn, True
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Erro ao criar tabela 'itens_pedido': {error}")
        if conn and conn.closed == 0:
            try:
                conn.rollback()
                logger.info("Rollback da transação em criar_tabela_itens_pedido realizado.")
            except Exception as e:
                logger.error(f"Erro ao tentar rollback em criar_tabela_itens_pedido: {e}")
        return conn, False
    finally:
        if cursor: 
            cursor.close()

# --- Funções Auxiliares da API Tiny ---

def fazer_requisicao_tiny(endpoint, params=None):
    """Faz uma requisição para a API do Tiny e retorna o JSON da resposta."""
    if params is None:
        params = {}
    params["token"] = TINY_API_V2_TOKEN
    params["formato"] = "json"
    
    url = f"{TINY_API_V2_BASE_URL}/{endpoint}"
    tentativas = 0
    max_tentativas = 3
    delay_tentativa = 5  # segundos

    while tentativas < max_tentativas:
        try:
            logger.debug(f"Fazendo requisição para {url} com parâmetros: {params}")
            response = requests.post(url, data=params)
            response.raise_for_status()  # Levanta exceção para códigos de erro HTTP
            
            # Pausa para evitar sobrecarga da API
            time.sleep(PAUSA_ENTRE_CHAMADAS_API)
            
            try:
                return response.json()
            except json.JSONDecodeError:
                logger.error(f"Erro ao decodificar JSON da resposta: {response.text}")
                return {"erro": "Erro ao decodificar JSON da resposta"}
                
        except requests.exceptions.RequestException as e:
            tentativas += 1
            logger.warning(f"Tentativa {tentativas}/{max_tentativas} falhou: {e}")
            if tentativas < max_tentativas:
                logger.info(f"Aguardando {delay_tentativa}s antes de tentar novamente...")
                time.sleep(delay_tentativa)
            else:
                logger.error(f"Todas as tentativas de requisição para {url} falharam.")
                return {"erro": f"Falha na requisição após {max_tentativas} tentativas: {str(e)}"}

def extrair_codigo_erro_api(resposta):
    """Extrai o código de erro da resposta da API do Tiny."""
    codigo_erro = None
    
    # Verificar se a resposta tem a estrutura esperada
    if isinstance(resposta, dict) and 'retorno' in resposta:
        retorno = resposta['retorno']
        
        # Verificar se há código de erro diretamente no retorno
        if isinstance(retorno, dict) and 'codigo_erro' in retorno:
            try:
                codigo_erro = retorno['codigo_erro']
                if isinstance(codigo_erro, str) and codigo_erro.isdigit():
                    codigo_erro = int(codigo_erro)
            except (ValueError, TypeError):
                logger.warning(f"Erro ao converter código de erro: {retorno['codigo_erro']}")
    
    # Se não encontrou o código de erro na estrutura padrão, tenta extrair de mensagens de erro
    if codigo_erro is None:
        mensagem_erro_completa = str(resposta)
        match_codigo = re.search(r"(?:Codigo:\s*|codigo_erro['\"]?:\s*['\"]?)(\d+)", mensagem_erro_completa, re.IGNORECASE)
        if match_codigo:
            try:
                codigo_erro = int(match_codigo.group(1))
            except (ValueError, TypeError):
                logger.warning(f"Erro ao converter código de erro extraído via regex: {match_codigo.group(1)}")
    
    return codigo_erro

def verificar_fim_paginacao(resposta):
    """Verifica se a resposta da API indica o fim da paginação (códigos 22 ou 23)."""
    codigo_erro = extrair_codigo_erro_api(resposta)
    
    # Códigos 22 e 23 indicam fim da paginação na API do Tiny
    if codigo_erro in [22, 23]:
        logger.info(f"Fim da paginação detectado (código {codigo_erro}).")
        return True
    
    return False

# --- Funções de Sincronização de Categorias ---

def sincronizar_categorias(conn):
    """Sincroniza as categorias do Tiny com o banco de dados."""
    logger.info("--- Iniciando Sincronização de Categorias ---")
    try:
        # Buscar árvore de categorias da API
        logger.info("Buscando árvore de categorias da API v2...")
        resposta = fazer_requisicao_tiny("categoria.obter.arvore.php")
        
        if "erro" in resposta:
            logger.error(f"Erro ao buscar árvore de categorias: {resposta['erro']}")
            return conn, False
        
        # Verificar se a resposta tem a estrutura esperada
        if 'retorno' not in resposta:
            logger.error(f"Resposta da API não contém 'retorno': {resposta}")
            return conn, False
        
        retorno = resposta['retorno']
        
        # Verificar se o retorno é uma lista (formato antigo) ou um dicionário com status e categorias (formato novo)
        categorias = None
        if isinstance(retorno, list):
            logger.info("API Tiny v2 (categorias): 'retorno' é uma lista, processando diretamente.")
            categorias = retorno
        elif isinstance(retorno, dict) and 'categorias' in retorno:
            logger.info("API Tiny v2 (categorias): 'retorno' contém 'categorias', processando.")
            categorias = retorno['categorias']
        else:
            logger.error(f"Formato de resposta da API para categorias não reconhecido: {retorno}")
            return conn, False
        
        # Processar categorias
        if not categorias:
            logger.warning("Nenhuma categoria encontrada na resposta da API.")
            return conn, True  # Não é um erro, apenas não há categorias
        
        # Iniciar transação para inserção/atualização de categorias
        cursor = conn.cursor()
        
        # Processar cada categoria
        for categoria in categorias:
            if not isinstance(categoria, dict):
                logger.warning(f"Categoria não é um dicionário, ignorando: {categoria}")
                continue
                
            id_categoria = categoria.get('id')
            nome_categoria = categoria.get('nome')
            id_categoria_pai = categoria.get('idPai')
            
            if not id_categoria or not nome_categoria:
                logger.warning(f"Categoria sem ID ou nome, ignorando: {categoria}")
                continue
            
            # Converter para None se for string vazia ou "0"
            if not id_categoria_pai or id_categoria_pai == "0":
                id_categoria_pai = None
                pai_info = "N/A"
            else:
                pai_info = id_categoria_pai
            
            logger.info(f"Processando categoria: ID={id_categoria}, Nome='{nome_categoria}', PaiID={pai_info}")
            
            # Inserir ou atualizar categoria
            try:
                cursor.execute("""
                INSERT INTO categorias (id_categoria_tiny, nome_categoria, id_categoria_pai_tiny, data_atualizacao)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (id_categoria_tiny) 
                DO UPDATE SET 
                    nome_categoria = EXCLUDED.nome_categoria,
                    id_categoria_pai_tiny = EXCLUDED.id_categoria_pai_tiny,
                    data_atualizacao = CURRENT_TIMESTAMP;
                """, (id_categoria, nome_categoria, id_categoria_pai))
            except (Exception, psycopg2.Error) as error:
                logger.error(f"Erro ao inserir/atualizar categoria ID {id_categoria}: {error}")
                conn.rollback()
                return conn, False
        
        # Commit da transação
        conn.commit()
        logger.info("Sincronização de categorias concluída com sucesso. Commit realizado.")
        return conn, True
        
    except Exception as e:
        logger.error(f"Erro inesperado na sincronização de categorias: {e}")
        if conn and conn.closed == 0:
            try:
                conn.rollback()
                logger.info("Rollback da transação de categorias realizado.")
            except Exception as rb_error:
                logger.error(f"Erro ao tentar rollback em sincronizar_categorias: {rb_error}")
        return conn, False

# --- Funções de Sincronização de Produtos ---

def buscar_e_gravar_produtos(conn, lote_atual=1, max_lotes=MAX_LOTES_PRODUTOS_POR_EXECUCAO):
    """Busca produtos da API do Tiny e grava no banco de dados."""
    logger.info("--- Iniciando Sincronização de Produtos ---")
    
    # Verificar se existe timestamp de última sincronização
    timestamp_produtos = ler_progresso_db(conn, CHAVE_TIMESTAMP_PRODUTOS)
    
    # Se tiver timestamp, usar modo incremental, senão usar carga completa
    if timestamp_produtos:
        logger.info(f"Timestamp de última sincronização de produtos encontrado: {timestamp_produtos}")
        return buscar_e_gravar_produtos_incremental(conn, timestamp_produtos)
    else:
        logger.info("Nenhum timestamp de produtos encontrado. Iniciando carga completa.")
        return buscar_e_gravar_produtos_carga_completa(conn, lote_atual, max_lotes)

def buscar_e_gravar_produtos_incremental(conn, timestamp_ultimo):
    """Busca e grava apenas produtos alterados desde o último timestamp."""
    logger.info(f"Modo incremental para produtos ativado. Buscando alterações desde {timestamp_ultimo}.")
    
    try:
        # Parâmetros para a API de alterações
        params = {
            "dataAlteracao": timestamp_ultimo
        }
        
        # Buscar produtos alterados
        resposta = fazer_requisicao_tiny("produto.pesquisar.alteracoes.php", params)
        
        if "erro" in resposta:
            logger.error(f"Erro ao buscar produtos alterados: {resposta['erro']}")
            return conn, False, True  # Terceiro parâmetro indica se há mais produtos a processar
        
        # Verificar se a resposta tem a estrutura esperada
        if 'retorno' not in resposta:
            logger.error(f"Resposta da API não contém 'retorno': {resposta}")
            return conn, False, True
        
        retorno = resposta['retorno']
        
        # Verificar status da resposta
        if isinstance(retorno, dict) and 'status' in retorno and retorno['status'] == 'Erro':
            codigo_erro = extrair_codigo_erro_api(resposta)
            
            # Se for erro de "nenhum registro encontrado", não é um problema
            if codigo_erro in [14, 34]:
                logger.info("Nenhum produto alterado encontrado desde o último timestamp.")
                
                # Atualizar o timestamp para a data/hora atual
                novo_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                salvar_progresso_db(conn, CHAVE_TIMESTAMP_PRODUTOS, novo_timestamp)
                logger.info(f"Timestamp de produtos atualizado para: {novo_timestamp}")
                
                return conn, True, False  # Não há mais produtos a processar
            else:
                logger.error(f"Erro ao buscar produtos alterados: {retorno.get('erros', 'Erro desconhecido')}")
                return conn, False, True
        
        # Extrair produtos alterados
        produtos_alterados = []
        if isinstance(retorno, dict) and 'produtos' in retorno:
            produtos_alterados = retorno['produtos']
        elif isinstance(retorno, list):
            produtos_alterados = retorno
        
        if not produtos_alterados:
            logger.info("Nenhum produto alterado encontrado na resposta.")
            
            # Atualizar o timestamp para a data/hora atual
            novo_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            salvar_progresso_db(conn, CHAVE_TIMESTAMP_PRODUTOS, novo_timestamp)
            logger.info(f"Timestamp de produtos atualizado para: {novo_timestamp}")
            
            return conn, True, False  # Não há mais produtos a processar
        
        logger.info(f"Encontrados {len(produtos_alterados)} produtos alterados desde {timestamp_ultimo}.")
        
        # Processar cada produto alterado
        produtos_processados = 0
        produtos_com_erro = 0
        
        for produto in produtos_alterados:
            if not isinstance(produto, dict):
                logger.warning(f"Produto não é um dicionário, ignorando: {produto}")
                continue
            
            id_produto = produto.get('id')
            if not id_produto:
                logger.warning(f"Produto sem ID, ignorando: {produto}")
                continue
            
            # Buscar detalhes do produto
            logger.info(f"Buscando detalhes do produto ID: {id_produto}...")
            sucesso = buscar_e_gravar_detalhes_produto(conn, id_produto)
            
            if sucesso:
                produtos_processados += 1
            else:
                produtos_com_erro += 1
                logger.warning(f"Falha ao processar detalhe do produto ID {id_produto} da lista de alterações. Continuando...")
        
        logger.info(f"Processamento incremental concluído: {produtos_processados} produtos processados, {produtos_com_erro} com erro.")
        
        # Atualizar o timestamp para a data/hora atual
        novo_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        salvar_progresso_db(conn, CHAVE_TIMESTAMP_PRODUTOS, novo_timestamp)
        logger.info(f"Timestamp de produtos atualizado para: {novo_timestamp}")
        
        return conn, True, False  # Não há mais produtos a processar (modo incremental concluído)
        
    except Exception as e:
        logger.error(f"Erro inesperado na sincronização incremental de produtos: {e}")
        if conn and conn.closed == 0:
            try:
                conn.rollback()
                logger.info("Rollback da transação de produtos incremental realizado.")
            except Exception as rb_error:
                logger.error(f"Erro ao tentar rollback em buscar_e_gravar_produtos_incremental: {rb_error}")
        return conn, False, True

def buscar_e_gravar_produtos_carga_completa(conn, lote_atual=1, max_lotes=MAX_LOTES_PRODUTOS_POR_EXECUCAO):
    """Busca e grava todos os produtos em lotes."""
    logger.info(f"Processando lote {lote_atual} de produtos...")
    
    # Verificar se existe progresso de paginação
    pagina_atual_str = ler_progresso_db(conn, CHAVE_PAGINA_PRODUTOS)
    
    if pagina_atual_str and pagina_atual_str.isdigit():
        pagina_atual = int(pagina_atual_str)
        logger.info(f"Retomando processamento de produtos da página: {pagina_atual}")
    else:
        pagina_atual = 1
        logger.info("Nenhum progresso de paginação de produtos encontrado no banco. Iniciando da página 1.")
    
    logger.info(f"Modo de carga completa para produtos ativado. Iniciando da página {pagina_atual} com termo '{TERMO_PESQUISA_PRODUTOS_CARGA_COMPLETA}'.")
    
    lotes_processados = 0
    paginas_por_lote = PAGINAS_POR_LOTE_PRODUTOS
    todos_produtos_processados = False
    
    try:
        while lotes_processados < max_lotes and not todos_produtos_processados:
            paginas_processadas = 0
            
            # Processar um lote de páginas
            while paginas_processadas < paginas_por_lote and not todos_produtos_processados:
                logger.info(f"Buscando lista de produtos (página {pagina_atual})...")
                
                # Parâmetros para a API de pesquisa de produtos
                params = {
                    "pesquisa": TERMO_PESQUISA_PRODUTOS_CARGA_COMPLETA,
                    "pagina": pagina_atual
                }
                
                # Buscar produtos da página atual
                resposta = fazer_requisicao_tiny("produtos.pesquisa.php", params)
                
                if "erro" in resposta:
                    logger.error(f"Erro de requisição ao listar produtos (página {pagina_atual}): {resposta['erro']}")
                    # Salvar progresso para retomar da mesma página
                    salvar_progresso_db(conn, CHAVE_PAGINA_PRODUTOS, pagina_atual)
                    logger.info(f"Progresso de produtos salvo: página {pagina_atual}")
                    return conn, False, True  # Ainda há produtos a processar
                
                # Verificar se a resposta indica fim da paginação
                if verificar_fim_paginacao(resposta):
                    logger.info(f"Fim da paginação de produtos detectado na página {pagina_atual}.")
                    todos_produtos_processados = True
                    
                    # Remover o progresso de paginação, pois a carga completa foi concluída
                    # E salvar o timestamp atual para futuras sincronizações incrementais
                    novo_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    salvar_progresso_db(conn, CHAVE_TIMESTAMP_PRODUTOS, novo_timestamp)
                    logger.info(f"Timestamp inicial de produtos salvo: {novo_timestamp}")
                    
                    # Limpar o progresso de paginação
                    salvar_progresso_db(conn, CHAVE_PAGINA_PRODUTOS, "")
                    logger.info("Progresso de paginação de produtos removido (carga completa concluída).")
                    
                    break
                
                # Verificar se a resposta tem a estrutura esperada
                if 'retorno' not in resposta:
                    logger.error(f"Resposta da API não contém 'retorno': {resposta}")
                    # Salvar progresso para retomar da mesma página
                    salvar_progresso_db(conn, CHAVE_PAGINA_PRODUTOS, pagina_atual)
                    logger.info(f"Progresso de produtos salvo: página {pagina_atual}")
                    return conn, False, True  # Ainda há produtos a processar
                
                retorno = resposta['retorno']
                
                # Extrair produtos da página
                produtos_pagina = []
                if isinstance(retorno, dict) and 'produtos' in retorno:
                    produtos_pagina = retorno['produtos']
                elif isinstance(retorno, list):
                    produtos_pagina = retorno
                
                if not produtos_pagina:
                    logger.warning(f"Nenhum produto encontrado na página {pagina_atual}.")
                    pagina_atual += 1
                    paginas_processadas += 1
                    
                    # Salvar progresso
                    salvar_progresso_db(conn, CHAVE_PAGINA_PRODUTOS, pagina_atual)
                    logger.info(f"Progresso de produtos salvo: página {pagina_atual}")
                    
                    # Pausa entre páginas
                    time.sleep(PAUSA_ENTRE_PAGINAS)
                    continue
                
                logger.info(f"Encontrados {len(produtos_pagina)} produtos na página {pagina_atual}.")
                
                # Iniciar transação para esta página
                cursor = conn.cursor()
                
                # Processar cada produto da página
                produtos_processados = 0
                produtos_com_erro = 0
                
                for produto in produtos_pagina:
                    if not isinstance(produto, dict):
                        logger.warning(f"Produto não é um dicionário, ignorando: {produto}")
                        continue
                    
                    id_produto = produto.get('id')
                    if not id_produto:
                        logger.warning(f"Produto sem ID, ignorando: {produto}")
                        continue
                    
                    # Buscar detalhes do produto
                    logger.info(f"Buscando detalhes do produto ID: {id_produto}...")
                    sucesso = buscar_e_gravar_detalhes_produto(conn, id_produto)
                    
                    if sucesso:
                        produtos_processados += 1
                    else:
                        produtos_com_erro += 1
                        logger.warning(f"Falha ao processar detalhe do produto ID {id_produto} da lista. Continuando...")
                
                # Commit da transação da página
                try:
                    conn.commit()
                    logger.info(f"Commit realizado para produtos da página {pagina_atual}.")
                except (Exception, psycopg2.Error) as error:
                    logger.error(f"Erro ao realizar commit dos produtos da página {pagina_atual}: {error}")
                    try:
                        conn.rollback()
                        logger.info(f"Rollback da transação de produtos da página {pagina_atual} realizado.")
                    except Exception as rb_error:
                        logger.error(f"Erro ao tentar rollback: {rb_error}")
                
                # Avançar para a próxima página
                pagina_atual += 1
                paginas_processadas += 1
                
                # Salvar progresso
                salvar_progresso_db(conn, CHAVE_PAGINA_PRODUTOS, pagina_atual)
                logger.info(f"Progresso de produtos salvo: página {pagina_atual}")
                
                # Pausa entre páginas
                time.sleep(PAUSA_ENTRE_PAGINAS)
            
            # Incrementar contador de lotes
            lotes_processados += 1
            
            if not todos_produtos_processados:
                logger.info(f"Lote {lotes_processados} concluído. Ainda existem produtos a serem processados.")
                logger.info(f"Conteúdo do progresso após lote {lotes_processados}: '{pagina_atual}'")
            
            # Se atingiu o limite de lotes e ainda há produtos, informar
            if lotes_processados >= max_lotes and not todos_produtos_processados:
                logger.info(f"Atingido o limite de {max_lotes} lotes por execução. Execute o script novamente para continuar.")
                return conn, True, True  # Ainda há produtos a processar
        
        # Se chegou aqui, todos os produtos foram processados ou o limite de lotes foi atingido
        if todos_produtos_processados:
            logger.info(f"Todos os produtos foram processados após {lotes_processados} lote(s).")
            return conn, True, False  # Não há mais produtos a processar
        else:
            logger.info(f"Processados {lotes_processados} lotes de produtos. Ainda podem existir mais produtos.")
            return conn, True, True  # Ainda há produtos a processar
            
    except Exception as e:
        logger.error(f"Erro inesperado ao buscar/gravar produtos (página {pagina_atual}): {e}")
        if conn and conn.closed == 0:
            try:
                conn.rollback()
                logger.info("Rollback da transação de produtos realizado.")
            except Exception as rb_error:
                logger.error(f"Erro ao tentar rollback: {rb_error}")
        
        # Salvar progresso para retomar da mesma página
        salvar_progresso_db(conn, CHAVE_PAGINA_PRODUTOS, pagina_atual)
        logger.info(f"Progresso de produtos salvo: página {pagina_atual}")
        
        return conn, False, True  # Ainda há produtos a processar

def buscar_e_gravar_detalhes_produto(conn, id_produto):
    """Busca detalhes de um produto específico e grava no banco."""
    try:
        # Buscar detalhes do produto
        resposta = fazer_requisicao_tiny("produto.obter.php", {"id": id_produto})
        
        if "erro" in resposta:
            logger.error(f"Erro ao buscar detalhes do produto ID {id_produto}: {resposta['erro']}")
            return False
        
        # Verificar se a resposta tem a estrutura esperada
        if 'retorno' not in resposta:
            logger.error(f"Resposta da API não contém 'retorno' para produto ID {id_produto}: {resposta}")
            return False
        
        retorno = resposta['retorno']
        
        # Verificar status da resposta
        if isinstance(retorno, dict) and 'status' in retorno and retorno['status'] == 'Erro':
            logger.error(f"Erro ao buscar detalhes do produto ID {id_produto}: {retorno.get('erros', 'Erro desconhecido')}")
            return False
        
        # Extrair dados do produto
        produto = None
        if isinstance(retorno, dict) and 'produto' in retorno:
            produto = retorno['produto']
        elif isinstance(retorno, list) and len(retorno) > 0:
            produto = retorno[0]
        
        if not produto or not isinstance(produto, dict):
            logger.error(f"Dados do produto ID {id_produto} não encontrados ou inválidos: {retorno}")
            return False
        
        # Extrair campos relevantes
        nome_produto = produto.get('nome', '')
        sku = produto.get('codigo', '')
        preco_venda = produto.get('preco', 0)
        preco_custo = produto.get('precoCusto', 0)
        unidade = produto.get('unidade', '')
        tipo_produto = produto.get('tipo', '')
        estoque_atual = produto.get('saldo', 0)
        situacao = produto.get('situacao', '')
        id_categoria = produto.get('idCategoria', None)
        
        # Converter valores vazios para None
        if sku == '':
            sku = None
        
        # Converter strings numéricas para float
        try:
            preco_venda = float(preco_venda) if preco_venda else 0
            preco_custo = float(preco_custo) if preco_custo else 0
            estoque_atual = float(estoque_atual) if estoque_atual else 0
        except (ValueError, TypeError):
            logger.warning(f"Erro ao converter valores numéricos do produto ID {id_produto}. Usando valores padrão.")
            preco_venda = 0
            preco_custo = 0
            estoque_atual = 0
        
        # Converter id_categoria para None se for string vazia ou "0"
        if not id_categoria or id_categoria == "0":
            id_categoria = None
        
        logger.info(f"Inserindo/Atualizando produto detalhado ID: {id_produto}, Nome: {nome_produto[:30]}..., SKU: {sku}")
        
        # Iniciar transação para este produto
        cursor = conn.cursor()
        
        try:
            # Inserir ou atualizar produto
            cursor.execute("""
            INSERT INTO produtos (
                id_produto_tiny, nome_produto, sku, preco_venda, preco_custo, 
                unidade, tipo_produto, estoque_atual, situacao, id_categoria_produto_tiny, 
                data_atualizacao
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (id_produto_tiny) 
            DO UPDATE SET 
                nome_produto = EXCLUDED.nome_produto,
                sku = EXCLUDED.sku,
                preco_venda = EXCLUDED.preco_venda,
                preco_custo = EXCLUDED.preco_custo,
                unidade = EXCLUDED.unidade,
                tipo_produto = EXCLUDED.tipo_produto,
                estoque_atual = EXCLUDED.estoque_atual,
                situacao = EXCLUDED.situacao,
                id_categoria_produto_tiny = EXCLUDED.id_categoria_produto_tiny,
                data_atualizacao = CURRENT_TIMESTAMP;
            """, (
                id_produto, nome_produto, sku, preco_venda, preco_custo, 
                unidade, tipo_produto, estoque_atual, situacao, id_categoria
            ))
            
            # Commit da transação deste produto
            conn.commit()
            return True
            
        except (Exception, psycopg2.Error) as error:
            logger.error(f"Erro ao inserir/atualizar produto ID {id_produto}: {error}")
            try:
                conn.rollback()
                logger.info(f"Rollback realizado para o produto ID {id_produto} devido a erro.")
            except Exception as rb_error:
                logger.error(f"Erro ao tentar rollback para produto ID {id_produto}: {rb_error}")
            return False
            
    except Exception as e:
        logger.error(f"Erro inesperado ao processar detalhes do produto ID {id_produto}: {e}")
        return False

# --- Funções de Sincronização de Vendedores ---

def sincronizar_vendedores(conn):
    """Sincroniza os vendedores (contatos) do Tiny com o banco de dados."""
    logger.info("--- Iniciando Sincronização de Vendedores ---")
    try:
        # Buscar lista de contatos (vendedores) da API
        logger.info("Buscando lista de contatos (vendedores) da API...")
        resposta = fazer_requisicao_tiny("contatos.pesquisa.php", {"pesquisa": ""})
        
        if "erro" in resposta:
            logger.error(f"Erro ao buscar contatos: {resposta['erro']}")
            return conn, False
        
        # Verificar se a resposta tem a estrutura esperada
        if 'retorno' not in resposta:
            logger.error(f"Resposta da API não contém 'retorno': {resposta}")
            return conn, False
        
        retorno = resposta['retorno']
        
        # Verificar status da resposta
        if isinstance(retorno, dict) and 'status' in retorno and retorno['status'] == 'Erro':
            logger.error(f"Erro ao buscar contatos: {retorno.get('erros', 'Erro desconhecido')}")
            return conn, False
        
        # Extrair contatos
        contatos = []
        if isinstance(retorno, dict) and 'contatos' in retorno:
            contatos = retorno['contatos']
        elif isinstance(retorno, list):
            contatos = retorno
        
        if not contatos:
            logger.warning("Nenhum contato encontrado na resposta.")
            return conn, True  # Não é um erro, apenas não há contatos
        
        logger.info(f"Encontrados {len(contatos)} contatos.")
        
        # Filtrar apenas vendedores (tipo 'V')
        vendedores = []
        for contato in contatos:
            if not isinstance(contato, dict):
                logger.warning(f"Contato não é um dicionário, ignorando: {contato}")
                continue
            
            tipo_contato = contato.get('tipo')
            if tipo_contato == 'V':  # 'V' indica vendedor
                vendedores.append(contato)
        
        logger.info(f"Filtrados {len(vendedores)} vendedores.")
        
        if not vendedores:
            logger.warning("Nenhum vendedor encontrado entre os contatos.")
            return conn, True  # Não é um erro, apenas não há vendedores
        
        # Iniciar transação para inserção/atualização de vendedores
        cursor = conn.cursor()
        
        # Processar cada vendedor
        for vendedor in vendedores:
            id_vendedor = vendedor.get('id')
            nome_vendedor = vendedor.get('nome')
            situacao_vendedor = vendedor.get('situacao', 'A')  # 'A' = Ativo (padrão)
            
            if not id_vendedor or not nome_vendedor:
                logger.warning(f"Vendedor sem ID ou nome, ignorando: {vendedor}")
                continue
            
            logger.info(f"Processando vendedor: ID={id_vendedor}, Nome='{nome_vendedor}'")
            
            # Inserir ou atualizar vendedor
            try:
                cursor.execute("""
                INSERT INTO vendedores (id_vendedor_tiny, nome_vendedor, situacao_vendedor, data_atualizacao)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (id_vendedor_tiny) 
                DO UPDATE SET 
                    nome_vendedor = EXCLUDED.nome_vendedor,
                    situacao_vendedor = EXCLUDED.situacao_vendedor,
                    data_atualizacao = CURRENT_TIMESTAMP;
                """, (id_vendedor, nome_vendedor, situacao_vendedor))
            except (Exception, psycopg2.Error) as error:
                logger.error(f"Erro ao inserir/atualizar vendedor ID {id_vendedor}: {error}")
                conn.rollback()
                return conn, False
        
        # Commit da transação
        conn.commit()
        logger.info("Sincronização de vendedores concluída com sucesso. Commit realizado.")
        return conn, True
        
    except Exception as e:
        logger.error(f"Erro inesperado na sincronização de vendedores: {e}")
        if conn and conn.closed == 0:
            try:
                conn.rollback()
                logger.info("Rollback da transação de vendedores realizado.")
            except Exception as rb_error:
                logger.error(f"Erro ao tentar rollback em sincronizar_vendedores: {rb_error}")
        return conn, False

# --- Funções de Sincronização de Pedidos e Itens de Pedido ---

def buscar_e_gravar_pedidos_e_itens(conn):
    """Busca pedidos e seus itens da API do Tiny e grava no banco de dados."""
    logger.info("--- Iniciando Sincronização de Pedidos e Itens ---")
    
    # Verificar se existe timestamp de última sincronização
    timestamp_pedidos = ler_progresso_db(conn, CHAVE_TIMESTAMP_PEDIDOS)
    
    # Se tiver timestamp, usar modo incremental, senão usar carga completa
    if timestamp_pedidos:
        logger.info(f"Timestamp de última sincronização de pedidos encontrado: {timestamp_pedidos}")
        return buscar_e_gravar_pedidos_incremental(conn, timestamp_pedidos)
    else:
        logger.info("Nenhum timestamp de pedidos encontrado. Iniciando carga completa.")
        return buscar_e_gravar_pedidos_carga_completa(conn)

def buscar_e_gravar_pedidos_incremental(conn, timestamp_ultimo):
    """Busca e grava apenas pedidos alterados desde o último timestamp."""
    logger.info(f"Modo incremental para pedidos ativado. Buscando alterações desde {timestamp_ultimo}.")
    
    try:
        # Converter timestamp para formato da API (YYYY-MM-DD)
        data_alteracao = timestamp_ultimo.split()[0]  # Pegar apenas a parte da data
        
        # Parâmetros para a API de pedidos
        params = {
            "data_alteracao_inicial": data_alteracao,
            "formato_data_alteracao": "Y-m-d"
        }
        
        # Buscar pedidos alterados
        resposta = fazer_requisicao_tiny("pedidos.pesquisa.php", params)
        
        if "erro" in resposta:
            logger.error(f"Erro ao buscar pedidos alterados: {resposta['erro']}")
            return conn, False
        
        # Verificar se a resposta tem a estrutura esperada
        if 'retorno' not in resposta:
            logger.error(f"Resposta da API não contém 'retorno': {resposta}")
            return conn, False
        
        retorno = resposta['retorno']
        
        # Verificar status da resposta
        if isinstance(retorno, dict) and 'status' in retorno and retorno['status'] == 'Erro':
            codigo_erro = extrair_codigo_erro_api(resposta)
            
            # Se for erro de "nenhum registro encontrado", não é um problema
            if codigo_erro in [14, 34]:
                logger.info("Nenhum pedido alterado encontrado desde o último timestamp.")
                
                # Atualizar o timestamp para a data/hora atual
                novo_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                salvar_progresso_db(conn, CHAVE_TIMESTAMP_PEDIDOS, novo_timestamp)
                logger.info(f"Timestamp de pedidos atualizado para: {novo_timestamp}")
                
                return conn, True
            else:
                logger.error(f"Erro ao buscar pedidos alterados: {retorno.get('erros', 'Erro desconhecido')}")
                return conn, False
        
        # Extrair pedidos alterados
        pedidos_alterados = []
        if isinstance(retorno, dict) and 'pedidos' in retorno:
            pedidos_alterados = retorno['pedidos']
        elif isinstance(retorno, list):
            pedidos_alterados = retorno
        
        if not pedidos_alterados:
            logger.info("Nenhum pedido alterado encontrado na resposta.")
            
            # Atualizar o timestamp para a data/hora atual
            novo_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            salvar_progresso_db(conn, CHAVE_TIMESTAMP_PEDIDOS, novo_timestamp)
            logger.info(f"Timestamp de pedidos atualizado para: {novo_timestamp}")
            
            return conn, True
        
        logger.info(f"Encontrados {len(pedidos_alterados)} pedidos alterados desde {timestamp_ultimo}.")
        
        # Processar cada pedido alterado
        pedidos_processados = 0
        pedidos_com_erro = 0
        
        for pedido in pedidos_alterados:
            if not isinstance(pedido, dict):
                logger.warning(f"Pedido não é um dicionário, ignorando: {pedido}")
                continue
            
            id_pedido = pedido.get('id')
            if not id_pedido:
                logger.warning(f"Pedido sem ID, ignorando: {pedido}")
                continue
            
            # Buscar detalhes do pedido
            logger.info(f"Buscando detalhes do pedido ID: {id_pedido}...")
            sucesso = buscar_e_gravar_detalhes_pedido(conn, id_pedido)
            
            if sucesso:
                pedidos_processados += 1
            else:
                pedidos_com_erro += 1
                logger.warning(f"Falha ao processar detalhe do pedido ID {id_pedido} da lista de alterações. Continuando...")
        
        logger.info(f"Processamento incremental concluído: {pedidos_processados} pedidos processados, {pedidos_com_erro} com erro.")
        
        # Atualizar o timestamp para a data/hora atual
        novo_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        salvar_progresso_db(conn, CHAVE_TIMESTAMP_PEDIDOS, novo_timestamp)
        logger.info(f"Timestamp de pedidos atualizado para: {novo_timestamp}")
        
        return conn, True
        
    except Exception as e:
        logger.error(f"Erro inesperado na sincronização incremental de pedidos: {e}")
        if conn and conn.closed == 0:
            try:
                conn.rollback()
                logger.info("Rollback da transação de pedidos incremental realizado.")
            except Exception as rb_error:
                logger.error(f"Erro ao tentar rollback em buscar_e_gravar_pedidos_incremental: {rb_error}")
        return conn, False

def buscar_e_gravar_pedidos_carga_completa(conn):
    """Busca e grava todos os pedidos em lotes."""
    logger.info("Modo de carga completa para pedidos ativado.")
    
    # Verificar se existe progresso de paginação
    pagina_atual_str = ler_progresso_db(conn, CHAVE_PAGINA_PEDIDOS)
    
    if pagina_atual_str and pagina_atual_str.isdigit():
        pagina_atual = int(pagina_atual_str)
        logger.info(f"Retomando processamento de pedidos da página: {pagina_atual}")
    else:
        pagina_atual = 1
        logger.info("Nenhum progresso de paginação de pedidos encontrado no banco. Iniciando da página 1.")
    
    lotes_processados = 0
    paginas_por_lote = PAGINAS_POR_LOTE_PEDIDOS
    max_lotes = MAX_LOTES_PEDIDOS_POR_EXECUCAO
    todos_pedidos_processados = False
    
    try:
        while lotes_processados < max_lotes and not todos_pedidos_processados:
            paginas_processadas = 0
            
            # Processar um lote de páginas
            while paginas_processadas < paginas_por_lote and not todos_pedidos_processados:
                logger.info(f"Buscando lista de pedidos (página {pagina_atual})...")
                
                # Parâmetros para a API de pesquisa de pedidos
                params = {
                    "pagina": pagina_atual
                }
                
                # Buscar pedidos da página atual
                resposta = fazer_requisicao_tiny("pedidos.pesquisa.php", params)
                
                if "erro" in resposta:
                    logger.error(f"Erro de requisição ao listar pedidos (página {pagina_atual}): {resposta['erro']}")
                    # Salvar progresso para retomar da mesma página
                    salvar_progresso_db(conn, CHAVE_PAGINA_PEDIDOS, pagina_atual)
                    logger.info(f"Progresso de pedidos salvo: página {pagina_atual}")
                    return conn, False
                
                # Verificar se a resposta indica fim da paginação
                if verificar_fim_paginacao(resposta):
                    logger.info(f"Fim da paginação de pedidos detectado na página {pagina_atual}.")
                    todos_pedidos_processados = True
                    
                    # Remover o progresso de paginação, pois a carga completa foi concluída
                    # E salvar o timestamp atual para futuras sincronizações incrementais
                    novo_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    salvar_progresso_db(conn, CHAVE_TIMESTAMP_PEDIDOS, novo_timestamp)
                    logger.info(f"Timestamp inicial de pedidos salvo: {novo_timestamp}")
                    
                    # Limpar o progresso de paginação
                    salvar_progresso_db(conn, CHAVE_PAGINA_PEDIDOS, "")
                    logger.info("Progresso de paginação de pedidos removido (carga completa concluída).")
                    
                    break
                
                # Verificar se a resposta tem a estrutura esperada
                if 'retorno' not in resposta:
                    logger.error(f"Resposta da API não contém 'retorno': {resposta}")
                    # Salvar progresso para retomar da mesma página
                    salvar_progresso_db(conn, CHAVE_PAGINA_PEDIDOS, pagina_atual)
                    logger.info(f"Progresso de pedidos salvo: página {pagina_atual}")
                    return conn, False
                
                retorno = resposta['retorno']
                
                # Extrair pedidos da página
                pedidos_pagina = []
                if isinstance(retorno, dict) and 'pedidos' in retorno:
                    pedidos_pagina = retorno['pedidos']
                elif isinstance(retorno, list):
                    pedidos_pagina = retorno
                
                if not pedidos_pagina:
                    logger.warning(f"Nenhum pedido encontrado na página {pagina_atual}.")
                    pagina_atual += 1
                    paginas_processadas += 1
                    
                    # Salvar progresso
                    salvar_progresso_db(conn, CHAVE_PAGINA_PEDIDOS, pagina_atual)
                    logger.info(f"Progresso de pedidos salvo: página {pagina_atual}")
                    
                    # Pausa entre páginas
                    time.sleep(PAUSA_ENTRE_PAGINAS)
                    continue
                
                logger.info(f"Encontrados {len(pedidos_pagina)} pedidos na página {pagina_atual}.")
                
                # Processar cada pedido da página
                pedidos_processados = 0
                pedidos_com_erro = 0
                
                for pedido in pedidos_pagina:
                    if not isinstance(pedido, dict):
                        logger.warning(f"Pedido não é um dicionário, ignorando: {pedido}")
                        continue
                    
                    id_pedido = pedido.get('id')
                    if not id_pedido:
                        logger.warning(f"Pedido sem ID, ignorando: {pedido}")
                        continue
                    
                    # Buscar detalhes do pedido
                    logger.info(f"Buscando detalhes do pedido ID: {id_pedido}...")
                    sucesso = buscar_e_gravar_detalhes_pedido(conn, id_pedido)
                    
                    if sucesso:
                        pedidos_processados += 1
                    else:
                        pedidos_com_erro += 1
                        logger.warning(f"Falha ao processar detalhe do pedido ID {id_pedido} da lista. Continuando...")
                
                # Commit da transação da página
                try:
                    conn.commit()
                    logger.info(f"Commit realizado para pedidos da página {pagina_atual}.")
                except (Exception, psycopg2.Error) as error:
                    logger.error(f"Erro ao realizar commit dos pedidos da página {pagina_atual}: {error}")
                    try:
                        conn.rollback()
                        logger.info(f"Rollback da transação de pedidos da página {pagina_atual} realizado.")
                    except Exception as rb_error:
                        logger.error(f"Erro ao tentar rollback: {rb_error}")
                
                # Avançar para a próxima página
                pagina_atual += 1
                paginas_processadas += 1
                
                # Salvar progresso
                salvar_progresso_db(conn, CHAVE_PAGINA_PEDIDOS, pagina_atual)
                logger.info(f"Progresso de pedidos salvo: página {pagina_atual}")
                
                # Pausa entre páginas
                time.sleep(PAUSA_ENTRE_PAGINAS)
            
            # Incrementar contador de lotes
            lotes_processados += 1
            
            if not todos_pedidos_processados:
                logger.info(f"Lote {lotes_processados} concluído. Ainda existem pedidos a serem processados.")
            
            # Se atingiu o limite de lotes e ainda há pedidos, informar
            if lotes_processados >= max_lotes and not todos_pedidos_processados:
                logger.info(f"Atingido o limite de {max_lotes} lotes por execução. Execute o script novamente para continuar.")
                return conn, True
        
        # Se chegou aqui, todos os pedidos foram processados ou o limite de lotes foi atingido
        if todos_pedidos_processados:
            logger.info(f"Todos os pedidos foram processados após {lotes_processados} lote(s).")
        else:
            logger.info(f"Processados {lotes_processados} lotes de pedidos. Ainda podem existir mais pedidos.")
        
        return conn, True
            
    except Exception as e:
        logger.error(f"Erro inesperado ao buscar/gravar pedidos (página {pagina_atual}): {e}")
        if conn and conn.closed == 0:
            try:
                conn.rollback()
                logger.info("Rollback da transação de pedidos realizado.")
            except Exception as rb_error:
                logger.error(f"Erro ao tentar rollback: {rb_error}")
        
        # Salvar progresso para retomar da mesma página
        salvar_progresso_db(conn, CHAVE_PAGINA_PEDIDOS, pagina_atual)
        logger.info(f"Progresso de pedidos salvo: página {pagina_atual}")
        
        return conn, False

def buscar_e_gravar_detalhes_pedido(conn, id_pedido):
    """Busca detalhes de um pedido específico e seus itens e grava no banco."""
    try:
        # Buscar detalhes do pedido
        resposta = fazer_requisicao_tiny("pedido.obter.php", {"id": id_pedido})
        
        if "erro" in resposta:
            logger.error(f"Erro ao buscar detalhes do pedido ID {id_pedido}: {resposta['erro']}")
            return False
        
        # Verificar se a resposta tem a estrutura esperada
        if 'retorno' not in resposta:
            logger.error(f"Resposta da API não contém 'retorno' para pedido ID {id_pedido}: {resposta}")
            return False
        
        retorno = resposta['retorno']
        
        # Verificar status da resposta
        if isinstance(retorno, dict) and 'status' in retorno and retorno['status'] == 'Erro':
            logger.error(f"Erro ao buscar detalhes do pedido ID {id_pedido}: {retorno.get('erros', 'Erro desconhecido')}")
            return False
        
        # Extrair dados do pedido
        pedido = None
        if isinstance(retorno, dict) and 'pedido' in retorno:
            pedido = retorno['pedido']
        elif isinstance(retorno, list) and len(retorno) > 0:
            pedido = retorno[0]
        
        if not pedido or not isinstance(pedido, dict):
            logger.error(f"Dados do pedido ID {id_pedido} não encontrados ou inválidos: {retorno}")
            return False
        
        # Extrair campos relevantes do pedido
        numero_pedido = pedido.get('numero', '')
        data_pedido_str = pedido.get('data_pedido', '')
        id_lista_preco = pedido.get('id_lista_preco', '')
        descricao_lista_preco = pedido.get('descricao_lista_preco', '')
        
        # Dados do cliente
        cliente = pedido.get('cliente', {})
        if not isinstance(cliente, dict):
            cliente = {}
        
        nome_cliente = cliente.get('nome', '')
        cpf_cnpj_cliente = cliente.get('cpf_cnpj', '')
        email_cliente = cliente.get('email', '')
        fone_cliente = cliente.get('fone', '')
        
        # Dados do vendedor
        id_vendedor = pedido.get('id_vendedor', None)
        nome_vendedor_pedido = pedido.get('nome_vendedor', '')
        
        # Outros dados do pedido
        situacao_pedido = pedido.get('situacao', '')
        valor_total_pedido = pedido.get('valor_total', 0)
        valor_desconto_pedido = pedido.get('valor_desconto', 0)
        condicao_pagamento = pedido.get('forma_pagamento', '')  # Pode ser "À Vista", "A Prazo", etc.
        forma_pagamento = pedido.get('meio_pagamento', '')  # Pode ser "Dinheiro", "Cartão", etc.
        
        # Converter data_pedido para formato SQL (YYYY-MM-DD)
        data_pedido_sql = None
        if data_pedido_str:
            try:
                # Tentar converter do formato DD/MM/YYYY para YYYY-MM-DD
                partes_data = data_pedido_str.split('/')
                if len(partes_data) == 3:
                    data_pedido_sql = f"{partes_data[2]}-{partes_data[1]}-{partes_data[0]}"
            except Exception as e:
                logger.warning(f"Erro ao converter data do pedido ID {id_pedido}: {e}. Usando NULL.")
        
        # Converter valores vazios para None
        if id_vendedor == '' or id_vendedor == '0':
            id_vendedor = None
        
        # Converter strings numéricas para float
        try:
            valor_total_pedido = float(valor_total_pedido) if valor_total_pedido else 0
            valor_desconto_pedido = float(valor_desconto_pedido) if valor_desconto_pedido else 0
        except (ValueError, TypeError):
            logger.warning(f"Erro ao converter valores numéricos do pedido ID {id_pedido}. Usando valores padrão.")
            valor_total_pedido = 0
            valor_desconto_pedido = 0
        
        logger.info(f"Inserindo/Atualizando pedido ID: {id_pedido}, Número: {numero_pedido}, Cliente: {nome_cliente}")
        
        # Iniciar transação para este pedido e seus itens
        cursor = conn.cursor()
        
        try:
            # Inserir ou atualizar pedido
            cursor.execute("""
            INSERT INTO pedidos (
                id_pedido_tiny, numero_pedido_tiny, data_pedido, id_lista_preco, descricao_lista_preco,
                nome_cliente, cpf_cnpj_cliente, email_cliente, fone_cliente,
                id_vendedor_tiny, nome_vendedor_pedido, situacao_pedido,
                valor_total_pedido, valor_desconto_pedido, condicao_pagamento, forma_pagamento,
                data_atualizacao
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
                id_vendedor_tiny = EXCLUDED.id_vendedor_tiny,
                nome_vendedor_pedido = EXCLUDED.nome_vendedor_pedido,
                situacao_pedido = EXCLUDED.situacao_pedido,
                valor_total_pedido = EXCLUDED.valor_total_pedido,
                valor_desconto_pedido = EXCLUDED.valor_desconto_pedido,
                condicao_pagamento = EXCLUDED.condicao_pagamento,
                forma_pagamento = EXCLUDED.forma_pagamento,
                data_atualizacao = CURRENT_TIMESTAMP;
            """, (
                id_pedido, numero_pedido, data_pedido_sql, id_lista_preco, descricao_lista_preco,
                nome_cliente, cpf_cnpj_cliente, email_cliente, fone_cliente,
                id_vendedor, nome_vendedor_pedido, situacao_pedido,
                valor_total_pedido, valor_desconto_pedido, condicao_pagamento, forma_pagamento
            ))
            
            # Processar itens do pedido
            itens = pedido.get('itens', [])
            if not isinstance(itens, list):
                itens = []
            
            if itens:
                logger.info(f"Processando {len(itens)} itens do pedido ID {id_pedido}...")
                
                # Remover itens antigos deste pedido (para evitar itens órfãos se algum foi removido)
                cursor.execute("DELETE FROM itens_pedido WHERE id_pedido_tiny = %s", (id_pedido,))
                
                # Inserir novos itens
                for item in itens:
                    if not isinstance(item, dict):
                        logger.warning(f"Item não é um dicionário, ignorando: {item}")
                        continue
                    
                    # Gerar um ID único para o item (Tiny não fornece um ID específico para itens)
                    id_item = f"{id_pedido}_{item.get('sequencia', '')}"
                    
                    id_produto = item.get('id_produto', None)
                    if id_produto == '' or id_produto == '0':
                        id_produto = None
                    
                    sku_produto = item.get('codigo', '')
                    if sku_produto == '':
                        sku_produto = None
                    
                    descricao_item = item.get('descricao', '')
                    unidade_item = item.get('unidade', '')
                    quantidade_item = item.get('quantidade', 0)
                    valor_unitario_item = item.get('valor_unitario', 0)
                    valor_total_item = item.get('valor_total', 0)
                    
                    # Converter strings numéricas para float
                    try:
                        quantidade_item = float(quantidade_item) if quantidade_item else 0
                        valor_unitario_item = float(valor_unitario_item) if valor_unitario_item else 0
                        valor_total_item = float(valor_total_item) if valor_total_item else 0
                    except (ValueError, TypeError):
                        logger.warning(f"Erro ao converter valores numéricos do item {id_item}. Usando valores padrão.")
                        quantidade_item = 0
                        valor_unitario_item = 0
                        valor_total_item = 0
                    
                    # Inserir item
                    cursor.execute("""
                    INSERT INTO itens_pedido (
                        id_item_pedido_tiny, id_pedido_tiny, id_produto_tiny, sku_produto,
                        descricao_item, unidade_item, quantidade_item,
                        valor_unitario_item, valor_total_item, data_atualizacao
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP);
                    """, (
                        id_item, id_pedido, id_produto, sku_produto,
                        descricao_item, unidade_item, quantidade_item,
                        valor_unitario_item, valor_total_item
                    ))
            else:
                logger.info(f"Pedido ID {id_pedido} não possui itens.")
            
            # Commit da transação deste pedido e seus itens
            conn.commit()
            return True
            
        except (Exception, psycopg2.Error) as error:
            logger.error(f"Erro ao inserir/atualizar pedido ID {id_pedido} ou seus itens: {error}")
            try:
                conn.rollback()
                logger.info(f"Rollback realizado para o pedido ID {id_pedido} devido a erro.")
            except Exception as rb_error:
                logger.error(f"Erro ao tentar rollback para pedido ID {id_pedido}: {rb_error}")
            return False
            
    except Exception as e:
        logger.error(f"Erro inesperado ao processar detalhes do pedido ID {id_pedido}: {e}")
        return False

# --- Função Principal ---

def main():
    """Função principal do script."""
    logger.info("Iniciando script de integração Tiny ERP -> PostgreSQL...")
    start_time = time.time()
    
    conn = None
    try:
        # Conectar ao banco de dados
        conn = conectar_db()
        if conn is None:
            logger.error("Não foi possível estabelecer conexão com o banco de dados. Encerrando script.")
            return
        
        # Criar tabela de controle de progresso
        conn, sucesso_controle = criar_tabela_controle_progresso(conn)
        if not sucesso_controle:
            logger.error("Falha ao criar tabela de controle de progresso. Encerrando script.")
            return
        
        # Criar tabelas de dados
        conn, sucesso_tabelas = criar_tabelas_dados(conn)
        if not sucesso_tabelas:
            logger.error("Falha ao criar tabelas de dados. Encerrando script.")
            return
        
        # Sincronizar categorias
        conn, sucesso_categorias = sincronizar_categorias(conn)
        if not sucesso_categorias:
            logger.error("Sincronização de categorias falhou. Continuando com as demais entidades...")
        
        # Ler progresso inicial para debug
        logger.info("Lendo progresso inicial do banco...")
        pagina_produtos = ler_progresso_db(conn, CHAVE_PAGINA_PRODUTOS, "N/A")
        logger.info(f"Progresso de paginação de produtos no banco ANTES do processamento: {pagina_produtos}")
        
        timestamp_produtos = ler_progresso_db(conn, CHAVE_TIMESTAMP_PRODUTOS, "N/A")
        logger.info(f"Timestamp de produtos no banco ANTES do processamento: {timestamp_produtos}")
        
        # Sincronizar produtos (com controle de lotes)
        lote_atual = 1
        produtos_restantes = True
        
        while produtos_restantes:
            logger.info(f"Processando lote {lote_atual} de produtos...")
            conn, sucesso_produtos, produtos_restantes = buscar_e_gravar_produtos(conn, lote_atual)
            
            if not sucesso_produtos:
                logger.error("Sincronização de produtos falhou ou foi parcial neste lote. Verifique os logs.")
                break
            
            if produtos_restantes:
                logger.info(f"Lote {lote_atual} concluído. Ainda existem produtos a serem processados.")
                lote_atual += 1
                
                # Verificar se atingiu o limite de lotes
                if lote_atual > MAX_LOTES_PRODUTOS_POR_EXECUCAO:
                    logger.info(f"Atingido o limite de {MAX_LOTES_PRODUTOS_POR_EXECUCAO} lotes por execução. Execute o script novamente para continuar.")
                    logger.info("Script encerrando pois ainda há produtos restantes da carga completa para processar em uma próxima execução.")
                    return
            else:
                logger.info("Todos os produtos foram processados com sucesso.")
        
        # Se ainda há produtos a processar, encerrar o script para continuar na próxima execução
        if produtos_restantes:
            logger.info("Ainda existem produtos a serem processados na carga completa. O script foi projetado para processar em lotes.")
            logger.info("Execute o script novamente para continuar o processamento dos produtos restantes.")
            logger.info("Script encerrando pois ainda há produtos restantes da carga completa para processar em uma próxima execução.")
            return
        
        # Sincronizar vendedores
        conn, sucesso_vendedores = sincronizar_vendedores(conn)
        if not sucesso_vendedores:
            logger.error("Sincronização de vendedores falhou. Continuando com as demais entidades...")
        
        # Sincronizar pedidos e itens de pedido
        conn, sucesso_pedidos = buscar_e_gravar_pedidos_e_itens(conn)
        if not sucesso_pedidos:
            logger.error("Sincronização de pedidos e itens falhou. Verifique os logs.")
        
        # Ler progresso final para debug
        logger.info("Lendo progresso final do banco...")
        pagina_produtos = ler_progresso_db(conn, CHAVE_PAGINA_PRODUTOS, "N/A")
        logger.info(f"Progresso de paginação de produtos no banco APÓS o processamento: {pagina_produtos}")
        
        timestamp_produtos = ler_progresso_db(conn, CHAVE_TIMESTAMP_PRODUTOS, "N/A")
        logger.info(f"Timestamp de produtos no banco APÓS o processamento: {timestamp_produtos}")
        
        logger.info("Script de integração concluído com sucesso!")
        
    except Exception as e:
        logger.error(f"Erro inesperado na execução principal do script: {e}")
    finally:
        if conn and conn.closed == 0:
            logger.info("Fechando conexão com o PostgreSQL...")
            conn.close()
        
        end_time = time.time()
        logger.info(f"Script concluído. Tempo total: {end_time - start_time:.2f} segundos.")

if __name__ == "__main__":
    main()
