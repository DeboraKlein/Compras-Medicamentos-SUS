# src/dimensoes.py

import pandas as pd
import logging

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

def criar_e_integrar_dimensoes(df_fato: pd.DataFrame, pasta_outputs: str) -> pd.DataFrame:
    """
    Cria as tabelas dimensão (Instituição, Produto, Tempo, Fornecedor, Fabricante) a partir da Tabela Fato
    e integra suas chaves primárias (Surrogate Keys) de volta na Fato, usando prefixos intuitivos.
    """
    logger.info(" Iniciando a criação e integração das Tabelas Dimensão com prefixos...")
    
    # Lista para armazenar as chaves de dimensão criadas para reordenação final
    chaves_dimensao = ['id_pedido']

    # --- FUNÇÃO AUXILIAR PARA CRIAR DIMENSÃO (COM PREFIXO) ---
    def _criar_dimensao(df_base: pd.DataFrame, nome_dim: str, colunas_atributos: list, chave_primaria: str, prefixo: str):
        
        # 1. Extrair e Limpar Duplicatas
        df_dim = df_base[colunas_atributos].drop_duplicates(subset=colunas_atributos).reset_index(drop=True)
        
        # 2. Criar a Chave Primária (Surrogate Key - SK) com prefixo
        # O padrão {:05d} garante 5 dígitos, preenchendo com zeros à esquerda.
        #  CORREÇÃO: Converter (df_dim.index + 1) em uma Series para permitir o .apply()
        indices_sequenciais = pd.Series(df_dim.index + 1)

        df_dim[chave_primaria] = (
            indices_sequenciais
        ).apply(lambda x: f"{prefixo}{x:05d}")
        
        # 3. Reordenar e Exportar
        df_dim = df_dim[[chave_primaria] + colunas_atributos]
        df_dim.to_csv(f"{pasta_outputs}/dim_{nome_dim}.csv", index=False, sep=';', encoding='utf-8-sig')
        logger.info(f" Dimensão {nome_dim.capitalize()} criada ({len(df_dim):,} registros, Ex: {df_dim[chave_primaria].iloc[0]}) e exportada.")
        
        # 4. Integrar (Merge)
        df_fato_integrada = pd.merge(
            df_base,
            df_dim,
            on=colunas_atributos,
            how='left'
        )
        
        # 5. Remover colunas naturais da Fato
        df_fato_integrada.drop(columns=colunas_atributos, inplace=True, errors='ignore')
        
        # Adicionar a nova SK à lista de chaves
        chaves_dimensao.append(chave_primaria)
        
        return df_fato_integrada
    
    # --- PROCESSO DE CRIAÇÃO DAS DIMENSÕES ---
    
    # 1. DIMENSÃO INSTITUIÇÃO (Comprador)
    col_inst = ['cnpj_instituicao', 'nome_instituicao', 'municipio_instituicao', 'uf']
    df_fato = _criar_dimensao(df_fato, 'instituicao', col_inst, 'id_instituicao', 'Ins')

    # 2. DIMENSÃO PRODUTO/ITEM (Medicamento/CATMAT)
    col_prod = ['codigo_br', 'descricao_catmat', 'generico', 'unidade_fornecimento']
    df_fato = _criar_dimensao(df_fato, 'produto', col_prod, 'id_produto', 'Pro')

    # 3. DIMENSÃO FORNECEDOR
    col_forn = ['cnpj_fornecedor', 'fornecedor']
    if all(col in df_fato.columns for col in col_forn):
        df_fato = _criar_dimensao(df_fato, 'fornecedor', col_forn, 'id_fornecedor', 'For')
    else:
        logger.warning(f" Colunas de Fornecedor ({', '.join(col_forn)}) não encontradas. Dimensão pulada.")
        
    # 4. DIMENSÃO FABRICANTE
    col_fabr = ['cnpj_fabricante', 'fabricante']
    if all(col in df_fato.columns for col in col_fabr):
        df_fato = _criar_dimensao(df_fato, 'fabricante', col_fabr, 'id_fabricante', 'Fab')
    else:
        logger.warning(f" Colunas de Fabricante ({', '.join(col_fabr)}) não encontradas. Dimensão pulada.")

    # 5. DIMENSÃO TEMPO (Mantém o padrão AAAA/MM/DD, que já é descritivo e único)
    logger.info(" Criando Dimensão Tempo/Data...")
    col_tempo = ['compra'] 
    
    try:
        # Lógica da Dimensão Tempo... (mantida como a mais eficiente para datas)
        dim_tempo_natural = df_fato[['compra']].drop_duplicates().reset_index(drop=True)
        dim_tempo_natural['data_completa'] = pd.to_datetime(dim_tempo_natural['compra'], errors='coerce')
        dim_tempo_natural.dropna(subset=['data_completa'], inplace=True)
        
        dim_tempo_natural['id_tempo'] = (
            dim_tempo_natural['data_completa'].dt.strftime('%Y%m%d').astype(int)
        )
        
        dim_tempo_natural['ano'] = dim_tempo_natural['data_completa'].dt.year
        dim_tempo_natural['mes'] = dim_tempo_natural['data_completa'].dt.month
        dim_tempo_natural['dia'] = dim_tempo_natural['data_completa'].dt.day
        dim_tempo_natural['trimestre'] = dim_tempo_natural['data_completa'].dt.quarter
        
        dim_tempo = dim_tempo_natural[['id_tempo', 'data_completa', 'ano', 'mes', 'dia', 'trimestre']]
        
        df_fato = pd.merge(
            df_fato,
            dim_tempo_natural[['compra', 'id_tempo']], 
            on='compra',
            how='left'
        )
        
        df_fato.rename(columns={'compra': 'data_compra'}, inplace=True) 
        
        chaves_dimensao.append('id_tempo')
        
        logger.info(f" Dimensão Tempo criada ({len(dim_tempo):,} registros, Ex: {dim_tempo['id_tempo'].iloc[0]}) e integrada.")
        dim_tempo.to_csv(f"{pasta_outputs}/dim_tempo.csv", index=False, sep=';', encoding='utf-8-sig')
        
    except Exception as e:
        logger.error(f" Erro ao processar Dimensão Tempo: {e}")
        # Se falhar, renomeamos a coluna de volta para 'compra'
        df_fato.rename(columns={'data_compra': 'compra'}, inplace=True, errors='ignore')

    
    logger.info(" Criação de dimensões e integração à Fato concluída.")
    
    # 6. FINALIZAÇÃO DA FATO: Reordena as chaves de Dimensão e as colunas de contexto
    
    chaves_dimensao_finais = [c for c in chaves_dimensao if c in df_fato.columns]
    colunas_contexto = ['modalidade_compra', 'tipo_compra'] 
    
    colunas_finais_ordenadas = chaves_dimensao_finais
    colunas_finais_ordenadas += [col for col in colunas_contexto if col in df_fato.columns]
    colunas_finais_ordenadas += [col for col in df_fato.columns if col not in chaves_dimensao_finais + colunas_contexto]
    
    df_fato = df_fato[colunas_finais_ordenadas]

    return df_fato
