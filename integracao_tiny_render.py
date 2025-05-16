#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de Integração Tiny ERP -> PostgreSQL (API v3)
Desenvolvido para Fusion

Este script realiza a sincronização de dados entre o Tiny ERP e um banco de dados PostgreSQL,
utilizando a API v3 do Tiny. Ele suporta sincronização incremental para produtos e pedidos,
e carga completa para categorias e vendedores.

Todas as informações de progresso são armazenadas no banco de dados PostgreSQL para
garantir a continuidade mesmo em ambientes efêmeros como o Render.
"""

import os
import sys
import time
import json
import logging
import datetime
import re
import requests
import psycopg2
from psycopg2 import sql
from urllib.parse import quote

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()

# Configurações da API Tiny
TINY_TOKEN = os.environ.get('TINY_TOKEN', 'seu_token_aqui')
TINY_API_BASE_URL = 'https://api.tiny.com.br/public-api/v3'

# Configurações do PostgreSQL
DB_HOST = os.environ.get('DB_HOST', 'dpg-d0h4vjgdl3ps73cihsv0-a.oregon-postgres.render.com')
DB_NAME = os.environ.get('DB_NAME', 'banco_tiny_fusion')
DB_USER = os.environ.get('DB_USER', 'banco_tiny_fusion_user')
DB_PASSWORD = os.environ.get('DB_PASSWORD', '9JFbOUjBU1f3tM10EZygXgtOp8vKoUyb')
DB_PORT = os.environ.get('DB_PORT', '5432')

# Chaves para a tabela de controle_progresso
CHAVE_PAGINA_PRODUTOS = 'ultima_pagina_produto_processada'
CHAVE_TIMESTAMP_PRODUTOS = 'timestamp_sincronizacao_produtos'
CHAVE_PAGINA_PEDIDOS = 'ultima_pagina_pedido_processada'
CHAVE_TIMESTAMP_PEDIDOS = 'timestamp_sincronizacao_pedidos'

# Configurações de paginação e limites
LIMITE_REGISTROS_POR_PAGINA = 100
MAX_TENTATIVAS_API = 3
TEMPO_ESPERA_ENTRE_TENTATIVAS = 5  # segundos
MAX_LOTES_POR_EXECUCAO = 10  # Limita o número de lotes processados por execução

# Funções de conexão com o banco de dados
def conectar_db(max_tentativas=3, tempo_espera=5):
    """
    Estabelece conexão com o banco de dados PostgreSQL com retry.
    
    Args:
        max_tentativas: Número máximo de tentativas de conexão
        tempo_espera: Tempo de espera entre tentativas em segundos
        
    Returns:
        Conexão com o banco de dados ou None em caso de falha
    """
    tentativa = 1
    while tentativa <= max_tentativas:
        try:
            logger.info(f"Tentando conectar ao PostgreSQL (tentativa {tentativa})...")
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT,
                connect_timeout=10
            )
            conn.autocommit = False
            logger.info("Conexão com o PostgreSQL bem-sucedida!")
            return conn
        except Exception as e:
            logger.error(f"Erro ao conectar ao PostgreSQL (tentativa {tentativa}): {e}")
            if tentativa < max_tentativas:
                logger.info(f"Aguardando {tempo_espera}s antes de tentar novamente...")
                time.sleep(tempo_espera)
                tentativa += 1
            else:
                logger.error(f"Falha ao conectar ao PostgreSQL após {max_tentativas} tentativas.")
                return None

# Funções de controle de progresso no banco de dados
def criar_tabela_controle_progresso(conn):
    """
    Cria a tabela de controle de progresso se não existir.
    
    Args:
        conn: Conexão com o banco de dados
        
    Returns:
        Tupla (conn, sucesso) com a conexão e indicador de sucesso
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS controle_progresso (
            chave VARCHAR(100) PRIMARY KEY,
            valor TEXT,
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        conn.commit()
        logger.info("Tabela 'controle_progresso' verificada/criada com sucesso.")
        return conn, True
    except Exception as e:
        logger.error(f"Erro ao criar tabela de controle de progresso: {e}")
        if conn and conn.closed == 0:
            conn.rollback()
            logger.info("Rollback da transação em criar_tabela_controle_progresso realizado.")
        return conn, False

def ler_valor_do_banco(conn, chave, valor_padrao=""):
    """
    Lê um valor da tabela de controle de progresso.
    
    Args:
        conn: Conexão com o banco de dados
        chave: Chave do valor a ser lido
        valor_padrao: Valor padrão caso a chave não exista
        
    Returns:
        Valor lido ou valor padrão
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT valor FROM controle_progresso WHERE chave = %s", (chave,))
        resultado = cursor.fetchone()
        
        if resultado:
            valor = resultado[0]
            logger.info(f"Valor '{valor}' lido do banco para a chave '{chave}'.")
            return valor
        else:
            logger.info(f"Chave '{chave}' não encontrada no banco. Usando valor padrão: '{valor_padrao}'")
            return valor_padrao
    except Exception as e:
        logger.error(f"Erro ao ler valor do banco para a chave '{chave}': {e}")
        return valor_padrao

def salvar_valor_no_banco(conn, chave, valor):
    """
    Salva um valor na tabela de controle de progresso.
    
    Args:
        conn: Conexão com o banco de dados
        chave: Chave do valor a ser salvo
        valor: Valor a ser salvo
        
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO controle_progresso (chave, valor, data_atualizacao) 
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (chave) 
        DO UPDATE SET valor = EXCLUDED.valor, data_atualizacao = CURRENT_TIMESTAMP
        """, (chave, valor))
        conn.commit()
        logger.info(f"Valor '{valor}' salvo no banco para a chave '{chave}'.")
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar valor no banco para a chave '{chave}': {e}")
        if conn and conn.closed == 0:
            conn.rollback()
        return False

# Funções de criação de tabelas de dados
def verificar_e_limpar_constraints_inconsistentes(conn):
    """
    Verifica e remove constraints que possam estar inconsistentes.
    
    Args:
        conn: Conexão com o banco de dados
        
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        logger.info("Verificando constraints existentes...")
        cursor = conn.cursor()
        
        # Verificar se existe constraint referenciando id_vendedor em vez de id_vendedor_tiny
        cursor.execute("""
        SELECT con.conname, pg_get_constraintdef(con.oid)
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
        WHERE nsp.nspname = 'public'
        AND pg_get_constraintdef(con.oid) LIKE '%id_vendedor%'
        AND rel.relname = 'pedidos'
        """)
        
        constraints_inconsistentes = cursor.fetchall()
        
        if constraints_inconsistentes:
            for constraint in constraints_inconsistentes:
                constraint_name = constraint[0]
                logger.warning(f"Constraint inconsistente encontrada: {constraint_name}")
                cursor.execute(f"ALTER TABLE pedidos DROP CONSTRAINT IF EXISTS {constraint_name}")
                logger.info(f"Constraint {constraint_name} removida com sucesso.")
            conn.commit()
        else:
            logger.info("Nenhuma constraint inconsistente encontrada.")
        
        # Verificar se a coluna id_vendedor existe em vez de id_vendedor_tiny
        cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'pedidos'
        AND column_name = 'id_vendedor'
        """)
        
        coluna_inconsistente = cursor.fetchone()
        
        if coluna_inconsistente:
            logger.warning("Coluna inconsistente encontrada: 'id_vendedor' em vez de 'id_vendedor_tiny'")
            
            # Verificar se id_vendedor_tiny já existe
            cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'pedidos'
            AND column_name = 'id_vendedor_tiny'
            """)
            
            coluna_correta_existe = cursor.fetchone()
            
            if coluna_correta_existe:
                logger.info("Coluna 'id_vendedor_tiny' já existe. Removendo coluna inconsistente 'id_vendedor'...")
                cursor.execute("ALTER TABLE pedidos DROP COLUMN IF EXISTS id_vendedor")
            else:
                logger.info("Renomeando coluna 'id_vendedor' para 'id_vendedor_tiny'...")
                cursor.execute("ALTER TABLE pedidos RENAME COLUMN id_vendedor TO id_vendedor_tiny")
                logger.info("Coluna renomeada com sucesso.")
            
            conn.commit()
        else:
            logger.info("Coluna 'id_vendedor_tiny' já existe na tabela 'pedidos'. Nenhuma correção necessária.")
        
        return True
    except Exception as e:
        logger.error(f"Erro ao verificar e limpar constraints inconsistentes: {e}")
        if conn and conn.closed == 0:
            conn.rollback()
        return False

def criar_tabela_categorias(conn):
    """
    Cria a tabela de categorias se não existir.
    
    Args:
        conn: Conexão com o banco de dados
        
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS categorias (
            id_categoria_tiny INTEGER PRIMARY KEY,
            nome VARCHAR(255) NOT NULL,
            pai INTEGER,
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        conn.commit()
        logger.info("Tabela 'categorias' verificada/criada com sucesso.")
        return True
    except Exception as e:
        logger.error(f"Erro ao criar tabela 'categorias': {e}")
        if conn and conn.closed == 0:
            conn.rollback()
            logger.info("Rollback da transação em criar_tabela_categorias realizado.")
        return False

def criar_tabela_produtos(conn):
    """
    Cria a tabela de produtos se não existir.
    
    Args:
        conn: Conexão com o banco de dados
        
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS produtos (
            id_produto_tiny INTEGER PRIMARY KEY,
            codigo VARCHAR(50),
            nome VARCHAR(255) NOT NULL,
            preco NUMERIC(15, 2),
            preco_promocional NUMERIC(15, 2),
            id_categoria_tiny INTEGER,
            situacao VARCHAR(50),
            tipo VARCHAR(50),
            unidade VARCHAR(10),
            ncm VARCHAR(20),
            estoque NUMERIC(15, 2),
            data_criacao TIMESTAMP,
            data_alteracao TIMESTAMP,
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (id_categoria_tiny) REFERENCES categorias(id_categoria_tiny) ON DELETE SET NULL
        )
        """)
        conn.commit()
        logger.info("Tabela 'produtos' verificada/criada com sucesso.")
        return True
    except Exception as e:
        logger.error(f"Erro ao criar tabela 'produtos': {e}")
        if conn and conn.closed == 0:
            conn.rollback()
            logger.info("Rollback da transação em criar_tabela_produtos realizado.")
        return False

def criar_tabela_vendedores(conn):
    """
    Cria a tabela de vendedores se não existir.
    
    Args:
        conn: Conexão com o banco de dados
        
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS vendedores (
            id_vendedor_tiny INTEGER PRIMARY KEY,
            nome VARCHAR(255) NOT NULL,
            email VARCHAR(255),
            cpf_cnpj VARCHAR(20),
            telefone VARCHAR(20),
            situacao VARCHAR(50),
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        conn.commit()
        logger.info("Tabela 'vendedores' verificada/criada com sucesso.")
        return True
    except Exception as e:
        logger.error(f"Erro ao criar tabela 'vendedores': {e}")
        if conn and conn.closed == 0:
            conn.rollback()
            logger.info("Rollback da transação em criar_tabela_vendedores realizado.")
        return False

def criar_tabela_pedidos(conn):
    """
    Cria a tabela de pedidos se não existir.
    
    Args:
        conn: Conexão com o banco de dados
        
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS pedidos (
            id_pedido_tiny INTEGER PRIMARY KEY,
            numero INTEGER,
            numero_ecommerce VARCHAR(50),
            data_pedido TIMESTAMP,
            data_alteracao TIMESTAMP,
            id_vendedor_tiny INTEGER,
            nome_cliente VARCHAR(255),
            cpf_cnpj VARCHAR(20),
            email VARCHAR(255),
            telefone VARCHAR(20),
            valor_frete NUMERIC(15, 2),
            valor_desconto NUMERIC(15, 2),
            valor_total NUMERIC(15, 2),
            situacao VARCHAR(50),
            obs VARCHAR(1000),
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (id_vendedor_tiny) REFERENCES vendedores(id_vendedor_tiny) ON DELETE SET NULL
        )
        """)
        conn.commit()
        logger.info("Tabela 'pedidos' verificada/criada com sucesso.")
        return True
    except Exception as e:
        logger.error(f"Erro ao criar tabela 'pedidos': {e}")
        if conn and conn.closed == 0:
            conn.rollback()
            logger.info("Rollback da transação em criar_tabela_pedidos realizado.")
        return False

def criar_tabela_itens_pedido(conn):
    """
    Cria a tabela de itens de pedido se não existir.
    
    Args:
        conn: Conexão com o banco de dados
        
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS itens_pedido (
            id_item_pedido SERIAL PRIMARY KEY,
            id_pedido_tiny INTEGER NOT NULL,
            id_produto_tiny INTEGER,
            descricao VARCHAR(255) NOT NULL,
            quantidade NUMERIC(15, 2),
            valor_unitario NUMERIC(15, 2),
            valor_total NUMERIC(15, 2),
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (id_pedido_tiny) REFERENCES pedidos(id_pedido_tiny) ON DELETE CASCADE,
            FOREIGN KEY (id_produto_tiny) REFERENCES produtos(id_produto_tiny) ON DELETE SET NULL
        )
        """)
        conn.commit()
        logger.info("Tabela 'itens_pedido' verificada/criada com sucesso.")
        return True
    except Exception as e:
        logger.error(f"Erro ao criar tabela 'itens_pedido': {e}")
        if conn and conn.closed == 0:
            conn.rollback()
            logger.info("Rollback da transação em criar_tabela_itens_pedido realizado.")
        return False

def criar_tabelas_dados(conn):
    """
    Cria todas as tabelas de dados necessárias.
    
    Args:
        conn: Conexão com o banco de dados
        
    Returns:
        Tupla (conn, sucesso) com a conexão e indicador de sucesso
    """
    try:
        # Verificar e limpar constraints inconsistentes
        if not verificar_e_limpar_constraints_inconsistentes(conn):
            logger.error("Falha ao verificar e limpar constraints inconsistentes.")
            return conn, False
        
        # Criar tabelas em ordem para respeitar as dependências de chaves estrangeiras
        if not criar_tabela_categorias(conn):
            logger.error("Falha crítica ao criar tabela via criar_tabela_categorias. Abortando criação de tabelas de dados.")
            return conn, False
            
        if not criar_tabela_produtos(conn):
            logger.error("Falha crítica ao criar tabela via criar_tabela_produtos. Abortando criação de tabelas de dados.")
            return conn, False
            
        if not criar_tabela_vendedores(conn):
            logger.error("Falha crítica ao criar tabela via criar_tabela_vendedores. Abortando criação de tabelas de dados.")
            return conn, False
            
        if not criar_tabela_pedidos(conn):
            logger.error("Falha crítica ao criar tabela via criar_tabela_pedidos. Abortando criação de tabelas de dados.")
            return conn, False
            
        if not criar_tabela_itens_pedido(conn):
            logger.error("Falha crítica ao criar tabela via criar_tabela_itens_pedido. Abortando criação de tabelas de dados.")
            return conn, False
        
        return conn, True
    except Exception as e:
        logger.error(f"Erro ao criar tabelas de dados: {e}")
        return conn, False

# Funções de comunicação com a API do Tiny
def fazer_requisicao_api(url, params=None, max_tentativas=MAX_TENTATIVAS_API, tempo_espera=TEMPO_ESPERA_ENTRE_TENTATIVAS):
    """
    Faz uma requisição à API do Tiny com retry em caso de falha.
    
    Args:
        url: URL da requisição
        params: Parâmetros da requisição
        max_tentativas: Número máximo de tentativas
        tempo_espera: Tempo de espera entre tentativas em segundos
        
    Returns:
        Resposta da API ou None em caso de falha
    """
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {TINY_TOKEN}'
    }
    
    params = params or {}
    
    for tentativa in range(1, max_tentativas + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            # Verificar se a resposta foi bem-sucedida
            response.raise_for_status()
            
            # Verificar se o conteúdo é JSON válido
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Tentativa {tentativa}/{max_tentativas} falhou: {e}")
            if tentativa < max_tentativas:
                logger.info(f"Aguardando {tempo_espera}s antes de tentar novamente...")
                time.sleep(tempo_espera)
            else:
                logger.error(f"Todas as tentativas de requisição para {url} falharam.")
                raise Exception(f"Falha na requisição após {max_tentativas} tentativas: {e}")
    
    return None

def extrair_codigo_erro(mensagem_erro_completa):
    """
    Extrai o código de erro de uma mensagem de erro da API do Tiny.
    
    Args:
        mensagem_erro_completa: Mensagem de erro completa
        
    Returns:
        Código de erro ou None se não encontrado
    """
    if not mensagem_erro_completa:
        return None
    
    # Tentar extrair o código de erro usando regex
    match_codigo = re.search(r"(?:Codigo:\s*|codigo_erro:\s*\"?)(\d+)", mensagem_erro_completa, re.IGNORECASE)
    
    # Se não encontrou o código, tentar outros padrões
    if not match_codigo:
        match_codigo = re.search(r"erro.*?(\d+)", mensagem_erro_completa, re.IGNORECASE)
    
    if match_codigo:
        return match_codigo.group(1)
    
    return None

# Funções de sincronização de categorias
def sincronizar_categorias(conn):
    """
    Sincroniza as categorias do Tiny com o banco de dados.
    
    Args:
        conn: Conexão com o banco de dados
        
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        logger.info("--- Iniciando Sincronização de Categorias ---")
        
        # Buscar árvore de categorias da API v3
        logger.info("Buscando árvore de categorias da API v3...")
        url = f"{TINY_API_BASE_URL}/categorias/todas"
        
        try:
            resposta = fazer_requisicao_api(url)
            
            if not resposta or 'data' not in resposta:
                logger.error(f"Resposta inválida da API: {resposta}")
                return False
            
            categorias = resposta.get('data', [])
            
            if not categorias:
                logger.warning("Nenhuma categoria encontrada na API.")
                return True
            
            # Processar categorias
            cursor = conn.cursor()
            contador = 0
            
            for categoria in categorias:
                id_categoria = categoria.get('id')
                nome = categoria.get('nome', '')
                pai = categoria.get('idPai')
                
                if not id_categoria or not nome:
                    logger.warning(f"Categoria com dados incompletos: {categoria}")
                    continue
                
                # Inserir ou atualizar categoria
                cursor.execute("""
                INSERT INTO categorias (id_categoria_tiny, nome, pai, data_atualizacao)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (id_categoria_tiny) 
                DO UPDATE SET 
                    nome = EXCLUDED.nome,
                    pai = EXCLUDED.pai,
                    data_atualizacao = CURRENT_TIMESTAMP
                """, (id_categoria, nome, pai))
                
                contador += 1
            
            conn.commit()
            logger.info(f"Sincronização de categorias concluída. {contador} categorias processadas.")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao buscar árvore de categorias: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Erro durante sincronização de categorias: {e}")
        if conn and conn.closed == 0:
            conn.rollback()
        return False

# Funções de sincronização de produtos
def sincronizar_produtos(conn, lote=1, max_lotes=MAX_LOTES_POR_EXECUCAO):
    """
    Sincroniza os produtos do Tiny com o banco de dados.
    
    Args:
        conn: Conexão com o banco de dados
        lote: Número do lote atual
        max_lotes: Número máximo de lotes por execução
        
    Returns:
        True se todos os produtos foram processados, False se ainda há mais para processar
    """
    try:
        logger.info(f"Processando lote {lote} de produtos...")
        logger.info("--- Iniciando Sincronização de Produtos ---")
        
        # Verificar se há timestamp de última sincronização
        timestamp = ler_valor_do_banco(conn, CHAVE_TIMESTAMP_PRODUTOS)
        pagina_atual = ler_valor_do_banco(conn, CHAVE_PAGINA_PRODUTOS)
        
        # Determinar modo de sincronização (incremental ou completo)
        if timestamp:
            logger.info(f"Timestamp de última sincronização de produtos encontrado: {timestamp}")
            logger.info("Modo incremental para produtos ativado. Buscando alterações desde " + timestamp)
            return sincronizar_produtos_incremental(conn, timestamp, lote, max_lotes)
        else:
            logger.info("Nenhum timestamp de sincronização de produtos encontrado. Iniciando carga completa.")
            offset = int(pagina_atual) * LIMITE_REGISTROS_POR_PAGINA if pagina_atual else 0
            return sincronizar_produtos_completo(conn, offset, lote, max_lotes)
            
    except Exception as e:
        logger.error(f"Erro durante sincronização de produtos: {e}")
        if conn and conn.closed == 0:
            conn.rollback()
        return False

def sincronizar_produtos_incremental(conn, timestamp, lote=1, max_lotes=MAX_LOTES_POR_EXECUCAO):
    """
    Sincroniza incrementalmente os produtos alterados desde o timestamp.
    
    Args:
        conn: Conexão com o banco de dados
        timestamp: Timestamp da última sincronização
        lote: Número do lote atual
        max_lotes: Número máximo de lotes por execução
        
    Returns:
        True se todos os produtos foram processados, False se ainda há mais para processar
    """
    try:
        # Buscar produtos alterados desde o timestamp
        url = f"{TINY_API_BASE_URL}/produtos"
        params = {
            'dataAlteracao': timestamp,
            'limit': LIMITE_REGISTROS_POR_PAGINA,
            'offset': 0
        }
        
        try:
            resposta = fazer_requisicao_api(url, params)
            
            if not resposta or 'data' not in resposta:
                logger.error(f"Resposta inválida da API: {resposta}")
                return False
            
            produtos = resposta.get('data', [])
            
            if not produtos:
                logger.info("Nenhum produto alterado desde a última sincronização.")
                # Atualizar timestamp para a data/hora atual
                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                salvar_valor_no_banco(conn, CHAVE_TIMESTAMP_PRODUTOS, now)
                return True
            
            # Processar produtos alterados
            processados = processar_produtos(conn, produtos)
            
            if processados:
                # Atualizar timestamp para a data/hora atual
                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                salvar_valor_no_banco(conn, CHAVE_TIMESTAMP_PRODUTOS, now)
                logger.info(f"Sincronização incremental de produtos concluída. {len(produtos)} produtos processados.")
                return True
            else:
                logger.error("Sincronização de produtos falhou ou foi parcial neste lote. Verifique os logs.")
                return False
                
        except Exception as e:
            logger.error(f"Erro ao buscar produtos alterados: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Erro durante sincronização incremental de produtos: {e}")
        if conn and conn.closed == 0:
            conn.rollback()
        return False

def sincronizar_produtos_completo(conn, offset=0, lote=1, max_lotes=MAX_LOTES_POR_EXECUCAO):
    """
    Sincroniza todos os produtos do Tiny com o banco de dados.
    
    Args:
        conn: Conexão com o banco de dados
        offset: Offset para paginação
        lote: Número do lote atual
        max_lotes: Número máximo de lotes por execução
        
    Returns:
        True se todos os produtos foram processados, False se ainda há mais para processar
    """
    try:
        # Buscar produtos paginados
        url = f"{TINY_API_BASE_URL}/produtos"
        params = {
            'limit': LIMITE_REGISTROS_POR_PAGINA,
            'offset': offset
        }
        
        try:
            resposta = fazer_requisicao_api(url, params)
            
            if not resposta or 'data' not in resposta:
                logger.error(f"Resposta inválida da API: {resposta}")
                return False
            
            produtos = resposta.get('data', [])
            
            if not produtos:
                logger.info("Nenhum produto encontrado ou fim da paginação atingido.")
                # Limpar página atual e definir timestamp para modo incremental
                salvar_valor_no_banco(conn, CHAVE_PAGINA_PRODUTOS, "")
                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                salvar_valor_no_banco(conn, CHAVE_TIMESTAMP_PRODUTOS, now)
                logger.info("Carga completa de produtos finalizada. Modo incremental ativado para próximas execuções.")
                return True
            
            # Processar produtos
            processados = processar_produtos(conn, produtos)
            
            if processados:
                # Calcular próxima página
                pagina_atual = offset // LIMITE_REGISTROS_POR_PAGINA
                proxima_pagina = pagina_atual + 1
                
                # Salvar progresso
                salvar_valor_no_banco(conn, CHAVE_PAGINA_PRODUTOS, str(proxima_pagina))
                
                logger.info(f"Lote {lote} processado. Produtos da página {pagina_atual} sincronizados.")
                
                # Verificar se atingiu o limite de lotes por execução
                if lote >= max_lotes:
                    logger.info("Limite de lotes por execução atingido. Continuará na próxima execução.")
                    return False
                
                # Verificar se há mais produtos para processar
                if len(produtos) < LIMITE_REGISTROS_POR_PAGINA:
                    # Fim da paginação
                    salvar_valor_no_banco(conn, CHAVE_PAGINA_PRODUTOS, "")
                    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    salvar_valor_no_banco(conn, CHAVE_TIMESTAMP_PRODUTOS, now)
                    logger.info("Carga completa de produtos finalizada. Modo incremental ativado para próximas execuções.")
                    return True
                else:
                    # Ainda há mais produtos
                    logger.info("Ainda existem produtos a serem processados na carga completa. O script foi projetado para processar em lotes.")
                    logger.info("Execute o script novamente para continuar o processamento dos produtos restantes.")
                    return False
            else:
                logger.error("Sincronização de produtos falhou ou foi parcial neste lote. Verifique os logs.")
                return False
                
        except Exception as e:
            logger.error(f"Erro ao buscar produtos: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Erro durante sincronização completa de produtos: {e}")
        if conn and conn.closed == 0:
            conn.rollback()
        return False

def processar_produtos(conn, produtos):
    """
    Processa uma lista de produtos e os insere/atualiza no banco de dados.
    
    Args:
        conn: Conexão com o banco de dados
        produtos: Lista de produtos a serem processados
        
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        cursor = conn.cursor()
        contador = 0
        
        for produto in produtos:
            try:
                # Extrair dados do produto
                id_produto = produto.get('id')
                codigo = produto.get('codigo', '')
                nome = produto.get('nome', '')
                preco = produto.get('preco', 0)
                preco_promocional = produto.get('precoPromocional', 0)
                id_categoria = produto.get('idCategoria')
                situacao = produto.get('situacao', '')
                tipo = produto.get('tipo', '')
                unidade = produto.get('unidade', '')
                ncm = produto.get('ncm', '')
                estoque = produto.get('saldo', 0)
                
                # Converter datas
                data_criacao_str = produto.get('dataCriacao', '')
                data_alteracao_str = produto.get('dataAlteracao', '')
                
                data_criacao = None
                data_alteracao = None
                
                if data_criacao_str:
                    try:
                        data_criacao = datetime.datetime.strptime(data_criacao_str, '%Y-%m-%d %H:%M:%S')
                    except:
                        pass
                
                if data_alteracao_str:
                    try:
                        data_alteracao = datetime.datetime.strptime(data_alteracao_str, '%Y-%m-%d %H:%M:%S')
                    except:
                        pass
                
                # Validar dados obrigatórios
                if not id_produto or not nome:
                    logger.warning(f"Produto com dados incompletos: {produto}")
                    continue
                
                # Inserir ou atualizar produto
                cursor.execute("""
                INSERT INTO produtos (
                    id_produto_tiny, codigo, nome, preco, preco_promocional, 
                    id_categoria_tiny, situacao, tipo, unidade, ncm, 
                    estoque, data_criacao, data_alteracao, data_atualizacao
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (id_produto_tiny) 
                DO UPDATE SET 
                    codigo = EXCLUDED.codigo,
                    nome = EXCLUDED.nome,
                    preco = EXCLUDED.preco,
                    preco_promocional = EXCLUDED.preco_promocional,
                    id_categoria_tiny = EXCLUDED.id_categoria_tiny,
                    situacao = EXCLUDED.situacao,
                    tipo = EXCLUDED.tipo,
                    unidade = EXCLUDED.unidade,
                    ncm = EXCLUDED.ncm,
                    estoque = EXCLUDED.estoque,
                    data_criacao = EXCLUDED.data_criacao,
                    data_alteracao = EXCLUDED.data_alteracao,
                    data_atualizacao = CURRENT_TIMESTAMP
                """, (
                    id_produto, codigo, nome, preco, preco_promocional,
                    id_categoria, situacao, tipo, unidade, ncm,
                    estoque, data_criacao, data_alteracao
                ))
                
                contador += 1
                
            except Exception as e:
                logger.error(f"Erro ao processar produto {produto.get('id')}: {e}")
                # Continuar com o próximo produto
                continue
        
        conn.commit()
        logger.info(f"{contador} produtos processados com sucesso.")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao processar produtos: {e}")
        if conn and conn.closed == 0:
            conn.rollback()
        return False

# Funções de sincronização de vendedores (contatos)
def sincronizar_vendedores(conn):
    """
    Sincroniza os vendedores (contatos) do Tiny com o banco de dados.
    
    Args:
        conn: Conexão com o banco de dados
        
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        logger.info("--- Iniciando Sincronização de Vendedores (Contatos) ---")
        
        # Buscar contatos paginados
        offset = 0
        limit = LIMITE_REGISTROS_POR_PAGINA
        total_processados = 0
        
        while True:
            url = f"{TINY_API_BASE_URL}/contatos"
            params = {
                'limit': limit,
                'offset': offset
            }
            
            try:
                resposta = fazer_requisicao_api(url, params)
                
                if not resposta or 'data' not in resposta:
                    logger.error(f"Resposta inválida da API: {resposta}")
                    return False
                
                contatos = resposta.get('data', [])
                
                if not contatos:
                    logger.info("Nenhum contato encontrado ou fim da paginação atingido.")
                    break
                
                # Processar contatos
                processados = processar_vendedores(conn, contatos)
                
                if processados:
                    total_processados += len(contatos)
                    
                    # Verificar se há mais contatos para processar
                    if len(contatos) < limit:
                        # Fim da paginação
                        break
                    else:
                        # Avançar para a próxima página
                        offset += limit
                else:
                    logger.error("Sincronização de vendedores falhou ou foi parcial. Verifique os logs.")
                    return False
                    
            except Exception as e:
                logger.error(f"Erro ao buscar contatos: {e}")
                return False
        
        logger.info(f"Sincronização de vendedores concluída. {total_processados} vendedores processados.")
        return True
        
    except Exception as e:
        logger.error(f"Erro durante sincronização de vendedores: {e}")
        if conn and conn.closed == 0:
            conn.rollback()
        return False

def processar_vendedores(conn, contatos):
    """
    Processa uma lista de contatos e os insere/atualiza no banco de dados como vendedores.
    
    Args:
        conn: Conexão com o banco de dados
        contatos: Lista de contatos a serem processados
        
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        cursor = conn.cursor()
        contador = 0
        
        for contato in contatos:
            try:
                # Extrair dados do contato
                id_contato = contato.get('id')
                nome = contato.get('nome', '')
                email = contato.get('email', '')
                cpf_cnpj = contato.get('cpfCnpj', '')
                telefone = contato.get('telefone', '')
                situacao = contato.get('situacao', '')
                
                # Validar dados obrigatórios
                if not id_contato or not nome:
                    logger.warning(f"Contato com dados incompletos: {contato}")
                    continue
                
                # Inserir ou atualizar vendedor
                cursor.execute("""
                INSERT INTO vendedores (
                    id_vendedor_tiny, nome, email, cpf_cnpj, 
                    telefone, situacao, data_atualizacao
                )
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (id_vendedor_tiny) 
                DO UPDATE SET 
                    nome = EXCLUDED.nome,
                    email = EXCLUDED.email,
                    cpf_cnpj = EXCLUDED.cpf_cnpj,
                    telefone = EXCLUDED.telefone,
                    situacao = EXCLUDED.situacao,
                    data_atualizacao = CURRENT_TIMESTAMP
                """, (
                    id_contato, nome, email, cpf_cnpj,
                    telefone, situacao
                ))
                
                contador += 1
                
            except Exception as e:
                logger.error(f"Erro ao processar contato {contato.get('id')}: {e}")
                # Continuar com o próximo contato
                continue
        
        conn.commit()
        logger.info(f"{contador} vendedores processados com sucesso.")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao processar vendedores: {e}")
        if conn and conn.closed == 0:
            conn.rollback()
        return False

# Funções de sincronização de pedidos
def sincronizar_pedidos(conn, lote=1, max_lotes=MAX_LOTES_POR_EXECUCAO):
    """
    Sincroniza os pedidos do Tiny com o banco de dados.
    
    Args:
        conn: Conexão com o banco de dados
        lote: Número do lote atual
        max_lotes: Número máximo de lotes por execução
        
    Returns:
        True se todos os pedidos foram processados, False se ainda há mais para processar
    """
    try:
        logger.info(f"Processando lote {lote} de pedidos...")
        logger.info("--- Iniciando Sincronização de Pedidos ---")
        
        # Verificar se há timestamp de última sincronização
        timestamp = ler_valor_do_banco(conn, CHAVE_TIMESTAMP_PEDIDOS)
        pagina_atual = ler_valor_do_banco(conn, CHAVE_PAGINA_PEDIDOS)
        
        # Determinar modo de sincronização (incremental ou completo)
        if timestamp:
            logger.info(f"Timestamp de última sincronização de pedidos encontrado: {timestamp}")
            logger.info("Modo incremental para pedidos ativado. Buscando alterações desde " + timestamp)
            return sincronizar_pedidos_incremental(conn, timestamp, lote, max_lotes)
        else:
            logger.info("Nenhum timestamp de sincronização de pedidos encontrado. Iniciando carga completa.")
            offset = int(pagina_atual) * LIMITE_REGISTROS_POR_PAGINA if pagina_atual else 0
            return sincronizar_pedidos_completo(conn, offset, lote, max_lotes)
            
    except Exception as e:
        logger.error(f"Erro durante sincronização de pedidos: {e}")
        if conn and conn.closed == 0:
            conn.rollback()
        return False

def sincronizar_pedidos_incremental(conn, timestamp, lote=1, max_lotes=MAX_LOTES_POR_EXECUCAO):
    """
    Sincroniza incrementalmente os pedidos alterados desde o timestamp.
    
    Args:
        conn: Conexão com o banco de dados
        timestamp: Timestamp da última sincronização
        lote: Número do lote atual
        max_lotes: Número máximo de lotes por execução
        
    Returns:
        True se todos os pedidos foram processados, False se ainda há mais para processar
    """
    try:
        # Buscar pedidos alterados desde o timestamp
        url = f"{TINY_API_BASE_URL}/pedidos"
        params = {
            'dataAtualizacao': timestamp,
            'limit': LIMITE_REGISTROS_POR_PAGINA,
            'offset': 0
        }
        
        try:
            resposta = fazer_requisicao_api(url, params)
            
            if not resposta or 'data' not in resposta:
                logger.error(f"Resposta inválida da API: {resposta}")
                return False
            
            pedidos = resposta.get('data', [])
            
            if not pedidos:
                logger.info("Nenhum pedido alterado desde a última sincronização.")
                # Atualizar timestamp para a data/hora atual
                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                salvar_valor_no_banco(conn, CHAVE_TIMESTAMP_PEDIDOS, now)
                return True
            
            # Processar pedidos alterados
            processados = processar_pedidos(conn, pedidos)
            
            if processados:
                # Atualizar timestamp para a data/hora atual
                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                salvar_valor_no_banco(conn, CHAVE_TIMESTAMP_PEDIDOS, now)
                logger.info(f"Sincronização incremental de pedidos concluída. {len(pedidos)} pedidos processados.")
                return True
            else:
                logger.error("Sincronização de pedidos falhou ou foi parcial neste lote. Verifique os logs.")
                return False
                
        except Exception as e:
            logger.error(f"Erro ao buscar pedidos alterados: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Erro durante sincronização incremental de pedidos: {e}")
        if conn and conn.closed == 0:
            conn.rollback()
        return False

def sincronizar_pedidos_completo(conn, offset=0, lote=1, max_lotes=MAX_LOTES_POR_EXECUCAO):
    """
    Sincroniza todos os pedidos do Tiny com o banco de dados.
    
    Args:
        conn: Conexão com o banco de dados
        offset: Offset para paginação
        lote: Número do lote atual
        max_lotes: Número máximo de lotes por execução
        
    Returns:
        True se todos os pedidos foram processados, False se ainda há mais para processar
    """
    try:
        # Buscar pedidos paginados
        url = f"{TINY_API_BASE_URL}/pedidos"
        params = {
            'limit': LIMITE_REGISTROS_POR_PAGINA,
            'offset': offset
        }
        
        try:
            resposta = fazer_requisicao_api(url, params)
            
            if not resposta or 'data' not in resposta:
                logger.error(f"Resposta inválida da API: {resposta}")
                return False
            
            pedidos = resposta.get('data', [])
            
            if not pedidos:
                logger.info("Nenhum pedido encontrado ou fim da paginação atingido.")
                # Limpar página atual e definir timestamp para modo incremental
                salvar_valor_no_banco(conn, CHAVE_PAGINA_PEDIDOS, "")
                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                salvar_valor_no_banco(conn, CHAVE_TIMESTAMP_PEDIDOS, now)
                logger.info("Carga completa de pedidos finalizada. Modo incremental ativado para próximas execuções.")
                return True
            
            # Processar pedidos
            processados = processar_pedidos(conn, pedidos)
            
            if processados:
                # Calcular próxima página
                pagina_atual = offset // LIMITE_REGISTROS_POR_PAGINA
                proxima_pagina = pagina_atual + 1
                
                # Salvar progresso
                salvar_valor_no_banco(conn, CHAVE_PAGINA_PEDIDOS, str(proxima_pagina))
                
                logger.info(f"Lote {lote} processado. Pedidos da página {pagina_atual} sincronizados.")
                
                # Verificar se atingiu o limite de lotes por execução
                if lote >= max_lotes:
                    logger.info("Limite de lotes por execução atingido. Continuará na próxima execução.")
                    return False
                
                # Verificar se há mais pedidos para processar
                if len(pedidos) < LIMITE_REGISTROS_POR_PAGINA:
                    # Fim da paginação
                    salvar_valor_no_banco(conn, CHAVE_PAGINA_PEDIDOS, "")
                    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    salvar_valor_no_banco(conn, CHAVE_TIMESTAMP_PEDIDOS, now)
                    logger.info("Carga completa de pedidos finalizada. Modo incremental ativado para próximas execuções.")
                    return True
                else:
                    # Ainda há mais pedidos
                    logger.info("Ainda existem pedidos a serem processados na carga completa. O script foi projetado para processar em lotes.")
                    logger.info("Execute o script novamente para continuar o processamento dos pedidos restantes.")
                    return False
            else:
                logger.error("Sincronização de pedidos falhou ou foi parcial neste lote. Verifique os logs.")
                return False
                
        except Exception as e:
            logger.error(f"Erro ao buscar pedidos: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Erro durante sincronização completa de pedidos: {e}")
        if conn and conn.closed == 0:
            conn.rollback()
        return False

def processar_pedidos(conn, pedidos):
    """
    Processa uma lista de pedidos e os insere/atualiza no banco de dados.
    
    Args:
        conn: Conexão com o banco de dados
        pedidos: Lista de pedidos a serem processados
        
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        cursor = conn.cursor()
        contador = 0
        
        for pedido_resumo in pedidos:
            try:
                # Obter detalhes completos do pedido
                id_pedido = pedido_resumo.get('id')
                
                if not id_pedido:
                    logger.warning(f"Pedido sem ID: {pedido_resumo}")
                    continue
                
                # Buscar detalhes completos do pedido
                url = f"{TINY_API_BASE_URL}/pedidos/{id_pedido}"
                
                try:
                    resposta_detalhes = fazer_requisicao_api(url)
                    
                    if not resposta_detalhes or 'data' not in resposta_detalhes:
                        logger.error(f"Resposta inválida da API para detalhes do pedido {id_pedido}: {resposta_detalhes}")
                        continue
                    
                    pedido = resposta_detalhes.get('data', {})
                    
                    if not pedido:
                        logger.warning(f"Nenhum detalhe encontrado para o pedido {id_pedido}")
                        continue
                    
                    # Extrair dados do pedido
                    numero = pedido.get('numero')
                    numero_ecommerce = pedido.get('numeroPedidoEcommerce', '')
                    id_vendedor = pedido.get('idVendedor')
                    
                    # Dados do cliente
                    cliente = pedido.get('cliente', {})
                    nome_cliente = cliente.get('nome', '')
                    cpf_cnpj = cliente.get('cpfCnpj', '')
                    email = cliente.get('email', '')
                    telefone = cliente.get('telefone', '')
                    
                    # Valores
                    valor_frete = pedido.get('valorFrete', 0)
                    valor_desconto = pedido.get('valorDesconto', 0)
                    valor_total = pedido.get('valorTotal', 0)
                    
                    # Outros dados
                    situacao = pedido.get('situacao', '')
                    obs = pedido.get('obs', '')
                    
                    # Converter datas
                    data_pedido_str = pedido.get('data', '')
                    data_alteracao_str = pedido.get('dataAlteracao', '')
                    
                    data_pedido = None
                    data_alteracao = None
                    
                    if data_pedido_str:
                        try:
                            data_pedido = datetime.datetime.strptime(data_pedido_str, '%Y-%m-%d %H:%M:%S')
                        except:
                            pass
                    
                    if data_alteracao_str:
                        try:
                            data_alteracao = datetime.datetime.strptime(data_alteracao_str, '%Y-%m-%d %H:%M:%S')
                        except:
                            pass
                    
                    # Validar dados obrigatórios
                    if not id_pedido or not numero:
                        logger.warning(f"Pedido com dados incompletos: {pedido}")
                        continue
                    
                    # Inserir ou atualizar pedido em uma transação isolada
                    cursor.execute("BEGIN")
                    
                    try:
                        # Inserir ou atualizar pedido
                        cursor.execute("""
                        INSERT INTO pedidos (
                            id_pedido_tiny, numero, numero_ecommerce, data_pedido, 
                            data_alteracao, id_vendedor_tiny, nome_cliente, cpf_cnpj, 
                            email, telefone, valor_frete, valor_desconto, 
                            valor_total, situacao, obs, data_atualizacao
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (id_pedido_tiny) 
                        DO UPDATE SET 
                            numero = EXCLUDED.numero,
                            numero_ecommerce = EXCLUDED.numero_ecommerce,
                            data_pedido = EXCLUDED.data_pedido,
                            data_alteracao = EXCLUDED.data_alteracao,
                            id_vendedor_tiny = EXCLUDED.id_vendedor_tiny,
                            nome_cliente = EXCLUDED.nome_cliente,
                            cpf_cnpj = EXCLUDED.cpf_cnpj,
                            email = EXCLUDED.email,
                            telefone = EXCLUDED.telefone,
                            valor_frete = EXCLUDED.valor_frete,
                            valor_desconto = EXCLUDED.valor_desconto,
                            valor_total = EXCLUDED.valor_total,
                            situacao = EXCLUDED.situacao,
                            obs = EXCLUDED.obs,
                            data_atualizacao = CURRENT_TIMESTAMP
                        """, (
                            id_pedido, numero, numero_ecommerce, data_pedido,
                            data_alteracao, id_vendedor, nome_cliente, cpf_cnpj,
                            email, telefone, valor_frete, valor_desconto,
                            valor_total, situacao, obs
                        ))
                        
                        # Processar itens do pedido
                        itens = pedido.get('itens', [])
                        
                        if itens:
                            # Remover itens antigos
                            cursor.execute("DELETE FROM itens_pedido WHERE id_pedido_tiny = %s", (id_pedido,))
                            
                            # Inserir novos itens
                            for item in itens:
                                id_produto = item.get('idProduto')
                                descricao = item.get('descricao', '')
                                quantidade = item.get('quantidade', 0)
                                valor_unitario = item.get('valorUnitario', 0)
                                valor_total_item = item.get('valorTotal', 0)
                                
                                cursor.execute("""
                                INSERT INTO itens_pedido (
                                    id_pedido_tiny, id_produto_tiny, descricao, 
                                    quantidade, valor_unitario, valor_total, data_atualizacao
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                                """, (
                                    id_pedido, id_produto, descricao,
                                    quantidade, valor_unitario, valor_total_item
                                ))
                        
                        cursor.execute("COMMIT")
                        contador += 1
                        
                    except Exception as e:
                        cursor.execute("ROLLBACK")
                        logger.error(f"Erro ao processar pedido {id_pedido} e seus itens: {e}")
                        continue
                    
                except Exception as e:
                    logger.error(f"Erro ao buscar detalhes do pedido {id_pedido}: {e}")
                    continue
                
            except Exception as e:
                logger.error(f"Erro ao processar pedido {pedido_resumo.get('id')}: {e}")
                # Continuar com o próximo pedido
                continue
        
        logger.info(f"{contador} pedidos processados com sucesso.")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao processar pedidos: {e}")
        if conn and conn.closed == 0:
            conn.rollback()
        return False

# Função principal
def main():
    """
    Função principal que coordena a sincronização de dados.
    """
    tempo_inicio = time.time()
    logger.info("Iniciando script de integração Tiny ERP -> PostgreSQL...")
    
    # Conectar ao banco de dados
    conn = conectar_db()
    if not conn:
        logger.error("Falha ao conectar ao banco de dados. Encerrando script.")
        return
    
    try:
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
        if not sincronizar_categorias(conn):
            logger.error("Sincronização de categorias falhou. Continuando com as demais entidades...")
        
        # Ler progresso inicial do banco
        logger.info("Lendo progresso inicial do banco...")
        pagina_produtos = ler_valor_do_banco(conn, CHAVE_PAGINA_PRODUTOS)
        logger.info(f"Progresso de paginação de produtos no banco ANTES do processamento: {pagina_produtos}")
        
        timestamp_produtos = ler_valor_do_banco(conn, CHAVE_TIMESTAMP_PRODUTOS)
        logger.info(f"Timestamp de produtos no banco ANTES do processamento: {timestamp_produtos}")
        
        # Sincronizar produtos
        produtos_completos = sincronizar_produtos(conn)
        
        # Sincronizar vendedores
        if not sincronizar_vendedores(conn):
            logger.error("Sincronização de vendedores falhou. Continuando com as demais entidades...")
        
        # Sincronizar pedidos apenas se produtos estiverem completos
        if produtos_completos:
            pedidos_completos = sincronizar_pedidos(conn)
            
            if not pedidos_completos:
                logger.info("Ainda existem pedidos a serem processados na carga completa. O script foi projetado para processar em lotes.")
                logger.info("Execute o script novamente para continuar o processamento dos pedidos restantes.")
        else:
            logger.info("Ainda existem produtos a serem processados na carga completa. O script foi projetado para processar em lotes.")
            logger.info("Execute o script novamente para continuar o processamento dos produtos restantes.")
            logger.info("Script encerrando pois ainda há produtos restantes da carga completa para processar em uma próxima execução.")
        
    except Exception as e:
        logger.error(f"Erro durante execução do script: {e}")
    finally:
        # Fechar conexão com o banco
        if conn and conn.closed == 0:
            conn.close()
            logger.info("Fechando conexão com o PostgreSQL...")
        
        tempo_total = time.time() - tempo_inicio
        logger.info(f"Script concluído. Tempo total: {tempo_total:.2f} segundos.")

if __name__ == "__main__":
    main()
