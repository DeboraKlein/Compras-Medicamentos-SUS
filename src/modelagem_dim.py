# src/modelagem_dim.py

import pandas as pd
import hashlib 
import re 
import logging
import numpy as np 

# Configuração de Log
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

# ==============================================================================
#  Função 1: CÁLCULO DE Z-SCORE (Usando TRANSFORM)
# ==============================================================================
def calcular_zscore_risco(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula o Z-Score para o Preço Médio Ponderado (PMP) de cada item de compra,
    agrupado por produto (codigo_br) e ano de compra.
    """
    logger.info("Aplicando etapa de Enriquecimento: Cálculo do Z-Score de Risco de Preço...")
    
    if not all(col in df.columns for col in ['compra', 'codigo_br', 'preco_total', 'qtd_itens_comprados']):
        logger.error("Colunas essenciais para Z-Score não encontradas. Pulando cálculo.")
        return df

    # 1. Cria uma coluna de Ano para o contexto (janela de tempo)
    df['ano_compra'] = df['compra'].dt.year

    # 2. Calcule o PMP Individual (Otimizado com np.where)
    df['pmp_individual'] = np.where(
        df['qtd_itens_comprados'] > 0,
        df['preco_total'] / df['qtd_itens_comprados'],
        0
    )
        
    # 3. Calcule a Média (mean) e o Desvio Padrão (std) por PRODUTO e ANO
    colunas_agrupamento = ['codigo_br', 'ano_compra']
    
    #  Uso do método TRANSFORM
    # 'transform' calcula o 'mean' do grupo e o aplica de volta a cada linha do 'df' original.
    # Isso evita TODOS os problemas de merge, join ou KeyError.
    
    logger.info("   Calculando Média (mean) por grupo...")
    df['pmp_medio'] = df.groupby(colunas_agrupamento)['pmp_individual'].transform('mean')
    
    logger.info("   Calculando Desvio Padrão (std) por grupo...")
    df['pmp_desvio_padrao'] = df.groupby(colunas_agrupamento)['pmp_individual'].transform('std')
    # Agora 'pmp_medio' e 'pmp_desvio_padrao' existem em todas as linhas do DataFrame original.
      
    # 5. Calcule o Z-Score
    
    # Trata Desvio Padrão NaN (coluna agora existe)
    df['pmp_desvio_padrao'] = df['pmp_desvio_padrao'].fillna(0) 

    # Calcula o Z-Score (Otimizado com np.where)
    df['score_z_risco'] = np.where(
        df['pmp_desvio_padrao'] > 0,
        (df['pmp_individual'] - df['pmp_medio']) / df['pmp_desvio_padrao'],
        0
    )
    
    df['score_z_risco'] = df['score_z_risco'].round(2)
    logger.info("Coluna 'score_z_risco' calculada e adicionada à Fato.")
    return df

# ==============================================================================
# FUNÇÃO 2: CÁLCULO DE INTERMITÊNCIA DA DEMANDA ---
# ==============================================================================

def calcular_risco_intermitencia(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula o Risco de Intermitência (Instabilidade) de um produto.
    Baseado na contagem de meses únicos em que o produto foi comprado.
    """
    logger.info("⏳ Calculando Risco de Intermitência (Gestão da Demanda)...")
    
    # *** CORREÇÃO APLICADA AQUI: USANDO 'data_compra' ***
    COLUNA_DATA = 'data_compra' 
    
    # 1. Preparação da Data
    if COLUNA_DATA not in df.columns:
         logger.error(f"❌ Coluna '{COLUNA_DATA}' para intermitência não encontrada. Pulando cálculo.")
         return df
         
    if not pd.api.types.is_datetime64_any_dtype(df[COLUNA_DATA]):
        logger.warning(f"⚠️ Coluna '{COLUNA_DATA}' não é datetime. Tentando conversão...")
        df[COLUNA_DATA] = pd.to_datetime(df[COLUNA_DATA], errors='coerce')
        
    df['mes_compra'] = df[COLUNA_DATA].dt.to_period('M')
    
    # 2. Contar meses únicos de compra para cada produto
    intermitencia = df.groupby('id_produto')['mes_compra'].nunique().reset_index()
    intermitencia.rename(columns={'mes_compra': 'Meses_Comprados_Historico'}, inplace=True)
    
    # 3. Definir o período total de meses
    mes_min = df['mes_compra'].min()
    mes_max = df['mes_compra'].max()
    if pd.isna(mes_min) or pd.isna(mes_max):
         logger.error("❌ Não foi possível determinar o período. Pulando Intermitência.")
         return df

    periodo_total_meses = (mes_max.year - mes_min.year) * 12 + mes_max.month - mes_min.month + 1
    
    # 4. Calcular o Risco (0 = Estável; 1 = Intermitente)
    intermitencia['Risco_Intermitencia'] = np.where(
        periodo_total_meses > 0,
        1 - (intermitencia['Meses_Comprados_Historico'] / periodo_total_meses),
        0
    )
    
    # 5. Merge de volta
    df = pd.merge(
        df, 
        intermitencia[['id_produto', 'Risco_Intermitencia', 'Meses_Comprados_Historico']], 
        on='id_produto', 
        how='left'
    )
    
    df.drop(columns=['mes_compra'], inplace=True, errors='ignore')
    logger.info("✅ Risco de Intermitência (Instabilidade de Demanda) calculado.")
    return df


# ==============================================================================
# FUNÇÃO 3: CÁLCULO DE CONCENTRAÇÃO DE FORNECEDOR (RISCO DE DEPENDÊNCIA) ---
# ==============================================================================

def calcular_concentracao_fornecedor(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula o percentual de gasto de um produto que está concentrado
    no seu principal fornecedor, sinalizando risco de dependência (Demanda).
    """
    logger.info("⏳ Calculando Risco de Concentração de Fornecedor...")
    
    gasto_col = 'preco_total'
    
    # 1. Gasto total por produto
    gasto_total_produto = df.groupby('id_produto')[gasto_col].sum().reset_index()
    gasto_total_produto.rename(columns={gasto_col: 'Gasto_Total_Produto_Global'}, inplace=True)
    
    # 2. Gasto por produto E fornecedor
    gasto_por_fornecedor = df.groupby(['id_produto', 'id_fornecedor'])[gasto_col].sum().reset_index()
    
    # 3. Encontra o fornecedor de MAIOR gasto para cada produto
    idx_max_gasto = gasto_por_fornecedor.groupby('id_produto')[gasto_col].idxmax()
    fornecedor_principal = gasto_por_fornecedor.loc[idx_max_gasto].rename(
        columns={gasto_col: 'Gasto_Forn_Principal'}
    )
    fornecedor_principal = fornecedor_principal[['id_produto', 'Gasto_Forn_Principal']]

    # 4. Faz o Merge e calcula o % de concentração
    df_concentracao = pd.merge(gasto_total_produto, fornecedor_principal, on='id_produto', how='left')
    
    # Calcula a % de concentração: 100% (ou 1.0) é alta dependência
    df_concentracao['%_Gasto_Unico_Forn'] = np.where(
        df_concentracao['Gasto_Total_Produto_Global'] > 0,
        df_concentracao['Gasto_Forn_Principal'] / df_concentracao['Gasto_Total_Produto_Global'],
        0
    )
    
    # 5. Merge de volta na Tabela Fato
    df = pd.merge(
        df, 
        df_concentracao[['id_produto', '%_Gasto_Unico_Forn']], 
        on='id_produto', 
        how='left'
    )
    
    logger.info("✅ Risco de Concentração de Fornecedor calculado.")
    return df



# ==============================================================================
#  FUNÇÃO 4: CÁLCULO DO ÍNDICE DE PRIORIZAÇÃO (Gestão da Demanda)
# ==============================================================================
def calcular_indice_priorizacao(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula e normaliza o Índice de Priorização para a Gestão de Demanda,
    combinando o Risco de Preço (Z-Score Absoluto) com o Valor Gasto (Demanda).
    """
    logger.info("⏳ Aplicando etapa de Priorização: Cálculo do Índice de Gestão da Demanda...")

    # id_produto (para agrupar), preco_total (para demanda), score_z_risco (para risco)
    if not all(col in df.columns for col in ['id_produto', 'preco_total', 'score_z_risco']):
        logger.error("❌ Colunas essenciais ('id_produto', 'preco_total', 'score_z_risco') para Priorização não encontradas. Pulando cálculo.")
        return df

    # 1. Agrupar os dados por Produto (id_produto)
    df_prioridade = df.groupby('id_produto').agg(
        # Risco: Usamos o valor absoluto do Z-Score para priorizar desvios para mais e para menos.
        risco_medio=('score_z_risco', lambda x: np.abs(x).mean()),
        # Demanda: Usamos a soma do preço total gasto (Demanda Financeira).
        demanda_valor=('preco_total', 'sum')
    ).reset_index()

    # 2. Normalizar os Indicadores (Escalonamento Min-Max)
    # Isso transforma os valores (R$ e Z-Score) em uma escala de 0 a 1.
    
    # Normaliza o Risco de 0 a 1
    risco_max = df_prioridade['risco_medio'].max()
    risco_min = df_prioridade['risco_medio'].min()
    # Evita divisão por zero se todos os Z-Scores forem iguais
    if (risco_max - risco_min) > 0:
        df_prioridade['risco_normalizado'] = (df_prioridade['risco_medio'] - risco_min) / (risco_max - risco_min)
    else:
        df_prioridade['risco_normalizado'] = 0
    
    # Normaliza a Demanda de 0 a 1
    demanda_max = df_prioridade['demanda_valor'].max()
    demanda_min = df_prioridade['demanda_valor'].min()
    # Evita divisão por zero se todos os valores forem iguais
    if (demanda_max - demanda_min) > 0:
        df_prioridade['demanda_normalizada'] = (df_prioridade['demanda_valor'] - demanda_min) / (demanda_max - demanda_min)
    else:
        df_prioridade['demanda_normalizada'] = 0

    # 3. Calcular o Índice Final (Ponderação 50/50: Risco e Demanda)
    df_prioridade['indice_priorizacao'] = (
        df_prioridade['risco_normalizado'] * 0.5 + 
        df_prioridade['demanda_normalizada'] * 0.5
    ).round(4)
    
    # 4. Selecionar colunas de interesse para merge e renomear
    df_prioridade = df_prioridade[['id_produto', 'indice_priorizacao', 'demanda_valor']]

    # 5. Juntar o Índice de volta ao DataFrame principal (df_fato)
    # Isso adiciona o índice de priorização (que é por produto) de volta em cada linha da fat
    df = df.merge(df_prioridade, on='id_produto', how='left')
    
    # Limpa as novas colunas que podem ter ficado NaN (se o merge falhar ou se os dados forem zero)
    df['indice_priorizacao'] = df['indice_priorizacao'].fillna(0)
    df['demanda_valor'] = df['demanda_valor'].fillna(0)
    
    logger.info("✅ Colunas 'indice_priorizacao' e 'demanda_valor' adicionadas à Fato.")
    return df

# ==============================================================================
# FUNÇÃO 5: GERAÇÃO DE ID_PEDIDO ---
# ==============================================================================
def gerar_id_pedido(df: pd.DataFrame) -> pd.DataFrame:
    
    logger.info("⏳ Aplicando etapa de Modelagem: Geração do id_pedido (Hash MD5) e Limpeza Final...")
    
    if df is None or df.empty:
        logger.warning("DataFrame recebido é None ou vazio. Retornando sem processamento.")
        return df

    colunas_hash = ['cnpj_instituicao', 'compra', 'codigo_br', 'cnpj_fornecedor', 'qtd_itens_comprados', 'preco_unitario', 'cnpj_fabricante', 'insercao', 'unidade_fornecimento_capacidade', 'capacidade', 'unidade_medida']
    
    if not all(col in df.columns for col in colunas_hash):
        logger.error(f"❌ Colunas essenciais ({', '.join(colunas_hash)}) para o HASH de ITEM não encontradas. Pulando hash.")
        pass 
    else:
        def calcular_hash_pedido(row):
            cnpj_completo = str(row['cnpj_instituicao']).strip()
            cnpj_raiz = re.sub(r'\D', '', cnpj_completo)[:8]
            data_compra = str(row['compra']).strip()
            codigo_br = str(row['codigo_br']).strip() 
            cnpj_fornecedor_completo = str(row['cnpj_fornecedor']).strip()
            cnpj_fornecedor_raiz = re.sub(r'\D', '', cnpj_fornecedor_completo)[:8]
            qtd_itens_comprados = str(row['qtd_itens_comprados']).strip()
            preco_unitario = str(row['preco_unitario']).strip()
            cnpj_fabricante_completo = str(row['cnpj_fabricante']).strip()
            cnpj_fabricante_raiz = re.sub(r'\D', '', cnpj_fabricante_completo)[:8]
            data_insercao = str(row['insercao']).strip()
            unidade_fornecimento_capacidade = str(row['unidade_fornecimento_capacidade']).strip()
            capacidade = str(row['capacidade']).strip()
            unidade_medida = str(row['unidade_medida']).strip()
            chave_concatenada = f"{cnpj_raiz}_{data_compra}_{codigo_br}_{cnpj_fornecedor_raiz}_{qtd_itens_comprados}_{preco_unitario}_{cnpj_fabricante_raiz}_{data_insercao}_{unidade_fornecimento_capacidade}_{capacidade}_{unidade_medida}"
            return hashlib.md5(chave_concatenada.encode('utf-8')).hexdigest()

        df['id_pedido'] = df.apply(calcular_hash_pedido, axis=1)

    colunas_para_dropar = [
        'unidade_fornecimento_capacidade', 
        'capacidade',                      
        'unidade_medida',                  
    ]
    colunas_removidas = 0
    for col in colunas_para_dropar:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)
            logger.info(f"🗑️ Coluna residual '{col}' removida.")
            colunas_removidas += 1
    if colunas_removidas == 0:
        logger.info("👍 Nenhuma coluna residual a ser dropada.")

    # 🚨 CHAMADA À FUNÇÃO DE CÁLCULO DE Z-SCORE
    df = calcular_zscore_risco(df)
    
    if 'compra' in df.columns and 'codigo_br' in df.columns:
        df = df.sort_values(by=['compra', 'codigo_br'], ascending=True).reset_index(drop=True)
        logger.info(f"✅ Registros ordenados por data ('compra') e código do item ('codigo_br').")
    elif 'compra' in df.columns:
        df = df.sort_values(by=['compra'], ascending=True).reset_index(drop=True)
        logger.info(f"✅ Registros ordenados por data ('compra').")

    if 'id_pedido' in df.columns:
        colunas = ['id_pedido'] + [col for col in df.columns if col != 'id_pedido']
        df = df[colunas]
        logger.info(f"✅ Coluna 'id_pedido' gerada e reordenada.")
    
    logger.info(f"✅ Modelagem concluída. Total de registros: {len(df):,}")
    
    return df

#  ==============================================================================
#  FUNÇÃO 6:RADAR DE OPORTUNIDADES (PMP MEDIANO DINÂMICO)
#  ==============================================================================

def gerar_mini_fato_radar_enriquecida(df_fato: pd.DataFrame) -> pd.DataFrame:
    """
    Cria uma Mini Tabela Fato Enriquecida com o PMP Mediano Dinâmico (Benchmark), 
    garantindo que o desvio seja real e o benchmark seja mais estável (Mediana, conforme sugestão).
    """
    logger.info("Aplicando etapa de Análise: Geração da 'Mini Fato Radar Enriquecida' com PMP Mediano...")
    
    # Contexto para o benchmark: O benchmark será por produto, instituição e período
    contexto_benchmark = ['id_produto', 'id_instituicao', 'id_tempo']
    required_cols = contexto_benchmark + ['id_pedido', 'id_fornecedor', 'id_fabricante', 'preco_unitario', 'preco_total', 'qtd_itens_comprados']
    
    if not all(col in df_fato.columns for col in required_cols):
        logger.error(f"Colunas essenciais para a Mini Fato Radar ({required_cols}) não encontradas. Pulando cálculo do Radar.")
        return pd.DataFrame()
        
    df_limpo = df_fato[
        (df_fato['preco_total'] > 0) & 
        (df_fato['qtd_itens_comprados'] > 0)
    ].copy()
    
    if df_limpo.empty:
        logger.warning("DataFrame limpo está vazio. Não há transações válidas para PMP. Retornando vazio.")
        return pd.DataFrame()

    # --- NOVO CÁLCULO DO BENCHMARK (MEDIANA) ---
    
    # 1. Calcular o PMP Mediano do CONTEXTO (agora é a Mediana, mais estável)
    df_benchmark = df_limpo.groupby(contexto_benchmark).agg(
        PMP_Mediano_Dinamico=('preco_unitario', 'median') 
    ).reset_index()

    # 2. Juntar o Benchmark de volta à Mini Fato
    df_mini_fato = pd.merge(
        df_limpo,
        df_benchmark,
        on=contexto_benchmark,
        how='left'
    )
    
    # 3. Criar a coluna Economia por Linha (para o total do potencial)
    df_mini_fato['Economia_por_Linha'] = (
        df_mini_fato['preco_unitario'] - df_mini_fato['PMP_Mediano_Dinamico']
    ) * df_mini_fato['qtd_itens_comprados']
    
    # 4. Calcular o Desvio % de Oportunidade
    # Agora usamos o PMP Mediano como referência
    df_mini_fato['Desvio_%_Oportunidade'] = np.where(
        df_mini_fato['PMP_Mediano_Dinamico'] > 0,
        (df_mini_fato['preco_unitario'] - df_mini_fato['PMP_Mediano_Dinamico']) / df_mini_fato['PMP_Mediano_Dinamico'],
        0
    )
    
    # --- FILTRAGEM E RENOMEAÇÃO FINAL ---
    radar_final = df_mini_fato[[
        'id_pedido', 
        'id_produto', 
        'id_instituicao',
        'id_fabricante', 
        'id_fornecedor', 
        'id_tempo', 
        'preco_unitario',
        'PMP_Mediano_Dinamico',       
        'Desvio_%_Oportunidade',
        'Economia_por_Linha'           
    ]].copy()
    
    radar_final.rename(columns={
        'preco_unitario': 'PMP_Pago_Linha',
        'PMP_Mediano_Dinamico': 'PMP_Benchmark_Referencia' 
    }, inplace=True)
    
    logger.info(f"✅ Mini Tabela Fato Enriquecida gerada com {len(radar_final):,} transações, usando PMP Mediano como benchmark.")
    
    return radar_final

