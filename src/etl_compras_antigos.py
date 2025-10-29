import os
import pandas as pd
import logging
import re
import traceback # Para registrar o erro completo

# Configuração de Log
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

class ETLComprasAntigos:
    """
    Pipeline de ETL para dados de compras públicas de anos antigos (2020-2022)
    VERSÃO COMPLETA E CORRIGIDA
    """    
    def __init__(self, pasta_base):
        self.pasta_base = pasta_base # Usa o caminho passado pelo main.py
        self.pasta_raw = os.path.join(self.pasta_base, "raw")
        self.pasta_processed = os.path.join(self.pasta_base, "processed")
        
        print(f"ETL Antigos - Usando caminho base: {self.pasta_base}")
        print(f"Raw: {self.pasta_raw}")
        print(f"Processed: {self.pasta_processed}")

        # Garante pastas
        os.makedirs(self.pasta_raw, exist_ok=True)
        os.makedirs(self.pasta_processed, exist_ok=True)
        
        self.logger = logging.getLogger(__name__)
        # Garante que o handler do logger está configurado
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    # --- MÉTODOS DE TRANSFORMAÇÃO ESPECÍFICOS PARA ANOS ANTIGOS ---
    
    # Padroniza nomes de colunas de ANOS ANTIGOS para snake_case do NOVO formato.    
    def _padronizar_colunas(self, df): 
        self.logger.info("🔧 Padronizando nomes de colunas...")
        
        # Lista de colunas a serem limpas (removendo espaços e caracteres extras)
        df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace('__', '_')

        map_colunas = {
        'Ano': 'ano_compra',
        'Código_BR': 'codigo_br',
        'Descrição CATMAT': 'descricao_catmat',
        
        
        # CORREÇÃO 1: Mapear para o nome final desejado 'unidade_fornecimento'
        ' Unidade_de_Fornecimento ': 'unidade_fornecimento', # Nome original com espaços
        'Unidade_de_Fornecimento': 'unidade_fornecimento',   # Proteção para nome sem espaços
            'Genérico': 'generico',
            'Anvisa': 'anvisa',
            'Compra': 'compra',
            'Modalidade_da_Compra': 'modalidade_compra',
            'Inserção': 'insercao',
            'Tipo_Compra': 'tipo_compra',
            
            # Inversões de CNPJ/Nome (mantidas como estão, pois você tem lógica para corrigir depois)
            'Fabricante': 'cnpj_fabricante_temp',
            'CNPJ_Fabricante': 'fabricante_temp',
            'Fornecedor': 'cnpj_fornecedor_temp',
            'CNPJ_Fornecedor': 'fornecedor_temp',
            'Nome_Instituição': 'cnpj_instituicao_temp',
            'CNPJ_Instituição': 'nome_instituicao_temp',
            
            'Município_Instituição': 'municipio_instituicao',
            'UF': 'uf',
            'Qtd_Itens_Comprados': 'qtd_itens_comprados',
            'Preço_Unitário': 'preco_unitario',
            
            # Se 'unidade_fornecimento_capacidade' já veio como coluna vazia, não a mapeie.
        }
        
        df.rename(columns=map_colunas, inplace=True)
        self.logger.info("Colunas padronizadas.")
        return df

    # Corrige a inversão de CNPJ e Nome que acontece em alguns anos antigos.
    def _corrigir_colunas_trocadas(self, df):
        
        self.logger.info("🔧 Corrigindo colunas CNPJ/Nome trocadas...")
        
        # Lógica de amostra para determinar se a coluna temp_cnpj realmente é o nome (se tem mais letras)
        def is_name(series):
            # Conta o número de caracteres alfabéticos na amostra
            amostra = series.head(100).astype(str).str.replace(r'\W', '', regex=True)
            letras = amostra.str.count(r'[a-zA-Z]').sum()
            digitos = amostra.str.count(r'[0-9]').sum()
            return letras > digitos * 0.5 # Se tiver mais letras do que metade dos dígitos, é nome

        # Fornecedor
        if 'cnpj_fornecedor_temp' in df.columns and 'fornecedor_temp' in df.columns:
            if is_name(df['cnpj_fornecedor_temp']):
                self.logger.info("   ⚠️ Inversão de Fornecedor detectada e corrigida.")
                df['fornecedor'] = df['cnpj_fornecedor_temp']
                df['cnpj_fornecedor'] = df['fornecedor_temp']
            else:
                df['fornecedor'] = df['fornecedor_temp']
                df['cnpj_fornecedor'] = df['cnpj_fornecedor_temp']
            df.drop(columns=['cnpj_fornecedor_temp', 'fornecedor_temp'], inplace=True)
        
        # Fabricante
        if 'cnpj_fabricante_temp' in df.columns and 'fabricante_temp' in df.columns:
            if is_name(df['cnpj_fabricante_temp']):
                self.logger.info("   ⚠️ Inversão de Fabricante detectada e corrigida.")
                df['fabricante'] = df['cnpj_fabricante_temp']
                df['cnpj_fabricante'] = df['fabricante_temp']
            else:
                df['fabricante'] = df['fabricante_temp']
                df['cnpj_fabricante'] = df['cnpj_fabricante_temp']
            df.drop(columns=['cnpj_fabricante_temp', 'fabricante_temp'], inplace=True)
            
        # Instituição
        if 'cnpj_instituicao_temp' in df.columns and 'nome_instituicao_temp' in df.columns:
            if is_name(df['cnpj_instituicao_temp']):
                self.logger.info("   ⚠️ Inversão de Instituição detectada e corrigida.")
                df['nome_instituicao'] = df['cnpj_instituicao_temp']
                df['cnpj_instituicao'] = df['nome_instituicao_temp']
            else:
                df['nome_instituicao'] = df['nome_instituicao_temp']
                df['cnpj_instituicao'] = df['cnpj_instituicao_temp']
            df.drop(columns=['cnpj_instituicao_temp', 'nome_instituicao_temp'], inplace=True)
            
        self.logger.info("Colunas CNPJ/Nome ajustadas.")
        return df

    # Corrige tipos de dados com limpeza robusta para ANOS ANTIGOS
    def _corrigir_tipos(self, df):        
        self.logger.info("Corrigindo tipos de dados (Antigos) com limpeza robusta...")

        # 1. CORREÇÃO NUMÉRICA ROBUSTA (Qtd e Preço)
        colunas_numericas = ['qtd_itens_comprados', 'preco_unitario']
        for coluna in colunas_numericas:
            if coluna in df.columns:
                try:
                    # Passo 1: Limpeza da string para formato regional brasileiro
                    # Remove R$, parênteses e espaços.
                    df[coluna] = df[coluna].astype(str).str.replace(r'[R$()+\s]', '', regex=True)
                    # Remove separadores de milhares (ponto)
                    df[coluna] = df[coluna].str.replace('.', '', regex=False)
                    # Troca separador decimal (vírgula) por ponto
                    df[coluna] = df[coluna].str.replace(',', '.', regex=False)
                    
                    # Passo 2: Conversão para numérico. 'errors=coerce' transforma falhas em NaN.
                    df[coluna] = pd.to_numeric(df[coluna], errors='coerce')

                    # Passo 3: Preenche NaN com 0 para garantir o cálculo e evitar erros.
                    df[coluna] = df[coluna].fillna(0)
                    self.logger.info(f"{coluna}: convertido para numérico (Limpeza regional aplicada)")

                except Exception as e:
                    self.logger.warning(f"{coluna}: erro na conversão numérica - {e}")

        # Recálculo de preco_total (necessário após a correção dos componentes)
        if 'preco_unitario' in df.columns and 'qtd_itens_comprados' in df.columns:
            df['preco_total'] = df['qtd_itens_comprados'] * df['preco_unitario']
            self.logger.info("preco_total recalculado.")
        else:
            self.logger.warning("Não foi possível calcular preco_total. Colunas ausentes.")


        # 2. CORREÇÃO DE DATAS
        colunas_data = ['compra', 'insercao']
        for coluna in colunas_data:
            if coluna in df.columns:
                # Usa dayfirst=True para garantir o formato DD/MM/AA (DD/MM/YYYY)
                df[coluna] = pd.to_datetime(df[coluna], errors='coerce', dayfirst=True)
                self.logger.info(f"{coluna}: convertido para data (dayfirst=True)")
                
        
        # 3. CORREÇÃO DE TIPO DE COMPRA (Padroniza para ADMINISTRATIVA/JUDICIAL em CAIXA ALTA)
        if 'tipo_compra' in df.columns:
            df['tipo_compra'] = (
                df['tipo_compra']
                .astype(str)
                .str.strip()
                .str.upper() # Garante caixa alta
                .replace({
                    'A': 'ADMINISTRATIVA',
                    'J': 'JUDICIAL',
                    # Mapeamento para garantir que se vierem por extenso/minúsculo, fiquem em caixa alta
                    'ADMINISTRATIVA': 'ADMINISTRATIVA',
                    'JUDICIAL': 'JUDICIAL',
                })
            )
            
            # Trata valores nulos ou inesperados
            valores_validos = ['ADMINISTRATIVA', 'JUDICIAL']
            df['tipo_compra'] = df['tipo_compra'].apply(
                lambda x: 'INDEFINIDO' if x not in valores_validos else x
            )

            self.logger.info(f"tipo_compra: padronizado (ADMINISTRATIVA/JUDICIAL).")


        # 4. PADRONIZAÇÃO DO CÓDIGO BR (Remoção do prefixo 'BR')
        if 'codigo_br' in df.columns:
            self.logger.info("codigo_br: Removendo prefixo 'BR'...")
            
            # 1. Converte para string e remove espaços em branco
            df['codigo_br'] = df['codigo_br'].astype(str).str.strip()
            
            # 2. Usa uma expressão regular ou .str.replace() para remover 'BR' se estiver no início
            # O .str.replace é mais simples para este caso:
            df['codigo_br'] = (
                df['codigo_br']
                .str.upper() # Coloca em caixa alta para pegar "br" ou "Br"
                .str.replace(r'^BR0*', '', regex=True) # Remove BR e qualquer zero à esquerda subsequente (se for o caso)
            )
                        
            # Garante que o valor resultante seja um número (em string) sem 'BR' e sem espaços.
            def limpar_codigo_br(codigo):
                if pd.isna(codigo):
                    return None
                # Remove 'BR' e qualquer caractere não-numérico
                codigo_limpo = re.sub(r'^BR', '', str(codigo).strip().upper()) 
                # Garante que não haja espaço no final
                return codigo_limpo.strip() 
            
            df['codigo_br'] = df['codigo_br'].apply(limpar_codigo_br)

            self.logger.info(f"codigo_br: prefixo 'BR' removido e padronizado.")

        
        # 5. CORREÇÃO CNPJ/FLAG/STRING
        # CNPJs
        colunas_cnpj = ['cnpj_instituicao', 'cnpj_fornecedor', 'cnpj_fabricante']
        for coluna in colunas_cnpj:
            if coluna in df.columns:
                df[coluna] = df[coluna].astype(str).str.replace(r'\D', '', regex=True).str.zfill(14)
                self.logger.info(f"{coluna}: limpeza e zfill(14)")

        
        # 6. Flags (Generico)
        if 'generico' in df.columns:
            df['generico'] = (
                df['generico']
                .astype(str)
                .str.strip()
                .str.upper() # Garante que "Sim" e "sim" virem "SIM"
                .replace({'NÃO': 'NÃO', 'NAO': 'NÃO'}) # Garante a acentuação
                .fillna('NÃO')
            )
            # Qualquer valor que ainda seja 'S' ou 'N' de um possível erro anterior será corrigido aqui
            df['generico'] = df['generico'].replace({'S': 'SIM', 'N': 'NÃO'})
            
            # Finalmente, qualquer valor que não seja SIM ou NÃO é forçado para NÃO
            df['generico'] = df['generico'].apply(lambda x: 'NÃO' if x not in ['SIM', 'NÃO'] else x)

            self.logger.info(f"generico: padronizado (SIM/NÃO) (Antigos anos).")

        # 7. Colunas de texto (limpeza básica)
        colunas_string = ['nome_instituicao', 'municipio_instituicao', 'uf',
                          'fornecedor', 'fabricante', 'descricao_catmat',
                          'unidade_fornecimento_capacidade', 'modalidade_compra', 'tipo_compra', 'anvisa', 'codigo_br']
        for coluna in colunas_string:
            if coluna in df.columns:
                df[coluna] = df[coluna].astype(str).str.strip().replace('nan', '').replace('None', '')
        
        # O campo anvisa deve ser tratado como string para evitar perdas de leading zeros.
        if 'anvisa' in df.columns:
            df['anvisa'] = df['anvisa'].str.replace(r'\D', '', regex=True)

        self.logger.info("Tipos corrigidos com sucesso.")
        return df
    
    # Adiciona colunas que não existem nos anos antigos com valores padrão
    def _adicionar_colunas_vazias(self, df):        
        self.logger.info("Adicionando colunas ausentes (capacidade, unidade_medida)...")
        colunas_novas = {
            'capacidade': 0.0,
            'unidade_medida': 'NA',
        }
        for col, default in colunas_novas.items():
            if col not in df.columns:
                df[col] = default
        self.logger.info("Colunas ausentes adicionadas.")
        return df

    # Garante que a ordem das colunas seja igual ao formato NOVO para consolidação.
    def _reordenar_colunas(self, df):        
        self.logger.info("Reordenando colunas...")
        colunas_finais = [
            'ano_compra', 'nome_instituicao', 'cnpj_instituicao', 'municipio_instituicao',
            'uf', 'compra', 'insercao', 'codigo_br', 'descricao_catmat',
            'unidade_fornecimento_capacidade', 'generico', 'anvisa', 'modalidade_compra',
            'tipo_compra', 'capacidade', 'unidade_medida', 'cnpj_fornecedor',
            'fornecedor', 'cnpj_fabricante', 'fabricante', 'qtd_itens_comprados',
            'preco_unitario', 'preco_total'
        ]
        
        # Filtra apenas as colunas que realmente existem no DataFrame
        colunas_presentes = [col for col in colunas_finais if col in df.columns]
        
        # Adiciona colunas presentes que não foram mapeadas na ordem, no final
        for col in df.columns:
            if col not in colunas_presentes:
                colunas_presentes.append(col)
        
        df = df[colunas_presentes]
        self.logger.info("Colunas reordenadas.")
        return df

    # Executa a sequência de transformações para um único DataFrame antigo.
    def _processar_arquivo(self, df):        
        df = self._padronizar_colunas(df)
        df = self._corrigir_colunas_trocadas(df)
        df = self._corrigir_tipos(df)
        df = self._adicionar_colunas_vazias(df)
        df = self._reordenar_colunas(df)
        return df

    # --- METODOS DE I/O E EXECUÇÃO ---

    # Lista arquivos antigos com correspondência exata de nome YYYY.csv
    def listar_arquivos_antigos(self):        
        if not os.path.exists(self.pasta_raw):
            self.logger.error("Pasta raw não existe!")
            return []
            
        todos_arquivos = os.listdir(self.pasta_raw)
        arquivos_antigos = []
        ANOS_ANTIGOS = ['2020', '2021', '2022']
        
        for f in todos_arquivos:
            # Verifica se é um CSV e se o nome do arquivo (sem extensão) está na lista de anos antigos
            nome_base, ext = os.path.splitext(f)
            if ext.lower() == '.csv' and nome_base in ANOS_ANTIGOS:
                caminho = os.path.join(self.pasta_raw, f)
                arquivos_antigos.append(caminho)
                self.logger.info(f"Arquivo antigo encontrado: {f}")

        self.logger.info(f"Total arquivos antigos a processar: {len(arquivos_antigos)}")
        return arquivos_antigos

    # Processa todos os arquivos antigos (2020-2022) em lote, com tratamento de erro individual. 
    def processar_todos_antigos(self):       
        self.logger.info("Iniciando processamento de todos os anos antigos (2020-2022)...")
        arquivos = self.listar_arquivos_antigos()
        dfs = []
        anos_processados = []

        for arquivo_path in arquivos:
            nome_arquivo = os.path.basename(arquivo_path)
            self.logger.info(f"\n>>INICIANDO processamento do arquivo: {nome_arquivo}")

            df = None
            encoding_tentativas = ['utf-8-sig', 'latin-1', 'iso-8859-1'] # Ordem de preferência

            for encoding in encoding_tentativas:
                try:
                    # Tenta ler com a codificação atual
                    df = pd.read_csv(arquivo_path, sep=';', encoding=encoding, low_memory=False)
                    self.logger.info(f"Leitura bem-sucedida com encoding: {encoding}")
                    break # Se leu, sai do loop de tentativas
                
                except UnicodeDecodeError:
                    # Se falhou por causa de codificação, tenta a próxima
                    self.logger.warning(f"Falha na leitura com encoding '{encoding}'. Tentando o próximo...")
                
                except pd.errors.EmptyDataError:
                    self.logger.error(f"Erro de Dados: Arquivo vazio ou ilegível: {nome_arquivo}")
                    break # Se o erro não for de codificação, quebra o loop de tentativas
                
                except Exception as e:
                    # Captura qualquer outro erro que não seja de codificação na leitura
                    self.logger.error(f"ERRO GRAVE inesperado na leitura de {nome_arquivo}: {e}")
                    self.logger.error(f"Detalhes do Erro:\n{traceback.format_exc()}")
                    break

            if df is None or df.empty:
                self.logger.error(f"Processamento ABORTADO para {nome_arquivo}. Não foi possível ler o arquivo com as codificações tentadas.")
                continue # Pula para o próximo arquivo no loop principal

            # Continuação do processamento SE o DataFrame foi lido com sucesso
            try:
                # Processamento (Onde ocorrem as correções de tipos, padronização, etc.)
                df_processado = self._processar_arquivo(df)
                
                dfs.append(df_processado)
                
                # Extrai o ano
                ano = os.path.splitext(nome_arquivo)[0]
                anos_processados.append(ano)
                self.logger.info(f"FINALIZADO com sucesso: {nome_arquivo} ({len(df_processado):,} registros)")
            
            except Exception as e:
                # Captura erros que ocorrem *após* a leitura (ex: conversão de tipos, limpeza)
                self.logger.error(f"ERRO GRAVE na transformação de {nome_arquivo}. O arquivo será pulado.")
                self.logger.error(f"Mensagem do Erro: {e}")
                self.logger.error(f"Detalhes (Traceback):\n{traceback.format_exc()}")

        
        if not dfs:
            self.logger.warning("\nNenhum DataFrame foi processado com sucesso para consolidar.")
            return None

        self.logger.info("\nTentando consolidar todos os DataFrames processados...")

        df_consolidado = pd.concat(dfs, ignore_index=True)
        
        # Salvamento
        anos_str = "_".join(sorted(set(anos_processados)))
        caminho_consolidado = os.path.join(self.pasta_processed, f"compras_antigos_consolidado_{anos_str}.csv")
        df_consolidado.to_csv(caminho_consolidado, index=False, encoding='utf-8-sig', sep=';')
        
        self.logger.info(f"Consolidação completa!")
        self.logger.info(f"Total de registros: {len(df_consolidado):,}")
        self.logger.info(f"Anos: {', '.join(sorted(set(anos_processados))) if anos_processados else 'N/A'}")
        self.logger.info(f"Arquivo: {caminho_consolidado}")
        
        return df_consolidado


# Função de conveniência
# Executa o pipeline completo para anos antigos
def processar_anos_antigos():    
    # Note que o construtor do ETLComprasAntigos usa o caminho fixo
    etl_antigo = ETLComprasAntigos("") 
    return etl_antigo.processar_todos_antigos()

if __name__ == "__main__":
    print("=" * 60)
    print("ETL - COMPRAS PÚBLICAS ANOS ANTIGOS (2020-2022)")
    print("=" * 60)
    
    try:
        df_antigos = processar_anos_antigos()
        
        if df_antigos is not None:
            print(f"\nPROCESSAMENTO DE ANOS ANTIGOS CONCLUÍDO! {len(df_antigos):,} registros consolidados.")
        else:
            print("\n O pipeline foi concluído, mas nenhum dado pôde ser consolidado.")
            
    except Exception as e:
        print(f"\nERRO FATAL NO MAIN: {e}")
        print(f"Detalhes: {traceback.format_exc()}")