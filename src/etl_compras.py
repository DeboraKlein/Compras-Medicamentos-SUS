import pandas as pd
import os
import glob
import logging
import re

# Pipeline de ETL para dados de compras públicas de múltiplos anos (2023-2025)

class ETLComprasPublicas:
    

    def __init__(self, pasta_dados="data"):
        self.pasta_base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.pasta_dados = os.path.join(self.pasta_base, pasta_dados)
        self.pasta_raw = os.path.join(self.pasta_dados, "raw")
        self.pasta_processed = os.path.join(self.pasta_dados, "processed")
        self.pasta_outputs = os.path.join(self.pasta_dados, "outputs")

        self.logger = self._configurar_log()
        self.logger.info("Inicialização do ETL completa.")
        self.logger.info(f"Pastas definidas: raw={self.pasta_raw}, processed={self.pasta_processed}, outputs={self.pasta_outputs}")

    # Configura sistema de logs
    def _configurar_log(self):        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('etl_compras.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)

    # Lista arquivos CSV excluindo anos antigos (2020-2022)
    def listar_arquivos_raw(self):        
        arquivos = [os.path.join(self.pasta_raw, f) for f in os.listdir(self.pasta_raw)
                    if f.endswith('.csv') and not any(ano in f for ano in ['2020', '2021', '2022'])]
        return arquivos

    # Extrai o ano do nome do arquivo
    def extrair_ano_do_arquivo(self, caminho_arquivo):        
        nome = os.path.basename(caminho_arquivo)
        match = re.search(r'20\d{2}', nome)
        if match:
            return int(match.group())
        self.logger.warning(f"Ano não detectado no nome do arquivo: {nome}")
        return None

    # Leitura flexível com tentativas de diferentes separadores e encodings
    def _ler_arquivo_flexivel(self, caminho_arquivo):        
        if not os.path.exists(caminho_arquivo):
            self.logger.error(f"Arquivo não encontrado: {caminho_arquivo}")
            return None

        tentativas = [
            {'sep': ',', 'encoding': 'utf-8'},
            {'sep': ';', 'encoding': 'utf-8'},
            {'sep': '\t', 'encoding': 'utf-8'},
            {'sep': '\t', 'encoding': 'latin-1'},
        ]

        for config in tentativas:
            try:
                df = pd.read_csv(caminho_arquivo, sep=config['sep'], encoding=config['encoding'])
                self.logger.info(f"Arquivo lido com sep='{config['sep']}' e encoding='{config['encoding']}'")
                return df
            except Exception as e:
                self.logger.warning(f"Falha com sep='{config['sep']}' e encoding='{config['encoding']}': {e}")

        self.logger.error(f"Todas as tentativas de leitura falharam para: {caminho_arquivo}")
        return None

    # Converte valores em notação científica de forma segura
    def _converter_notacao_cientifica_segura(self, valor):        
        if pd.isna(valor) or valor == '' or valor is None:
            return None
        
        try:
            valor_str = str(valor).strip()
            
            # Se está em notação científica
            if 'E+' in valor_str or 'E-' in valor_str:
                # Remove vírgula decimal se existir
                valor_str = valor_str.replace(',', '.')
                numero = float(valor_str)
                return str(int(numero)).zfill(14)
            
            # Se já é um número, converte
            try:
                numero = float(valor_str.replace(',', '.'))
                return str(int(numero)).zfill(14)
            except:
                return valor_str
                
        except Exception as e:
            self.logger.warning(f" Erro convertendo {valor}: {e}")
            return valor

    # Aplica correções específicas ao DataFrame
    def _corrigir_problemas_especificos(self, df):        
        if df is None or len(df) == 0:
            return df

        self.logger.info("🔧 Aplicando correções específicas...")

        # Limpeza leve dos nomes das colunas
        df.columns = [col.strip() for col in df.columns]

        # Divisão de coluna única (se necessário)
        if len(df.columns) == 1 and ';' in df.iloc[0, 0]:
            colunas_divididas = df.iloc[:, 0].str.split(';', expand=True)
            novo_cabecalho = colunas_divididas.iloc[0]
            df = colunas_divididas[1:]
            df.columns = novo_cabecalho
            df.reset_index(drop=True, inplace=True)
            self.logger.info(f"Colunas divididas: {len(df.columns)}")

        # Corrigir CNPJs
        colunas_cnpj = ['cnpj_instituicao', 'cnpj_fornecedor', 'cnpj_fabricante', 'anvisa']
        for coluna in colunas_cnpj:
            if coluna in df.columns:
                df[coluna] = df[coluna].apply(self._converter_notacao_cientifica_segura)

        # Corrigir valores numéricos
        colunas_numericas = ['qtd_itens_comprados', 'preco_unitario', 'preco_total', 'capacidade']
        for coluna in colunas_numericas:
            if coluna in df.columns:
                df[coluna] = pd.to_numeric(df[coluna], errors='coerce').fillna(0)

        # Corrigir datas
        colunas_data = ['compra', 'insercao']
        for coluna in colunas_data:
            if coluna in df.columns:
                df[coluna] = pd.to_datetime(df[coluna], errors='coerce')

        # Corrigir flags
        if 'generico' in df.columns:
            df['generico'] = (
                df['generico']
                .astype(str)
                .str.strip()
                .str.upper()
                .map({'S': 'SIM', 'N': 'NÃO'})
                .fillna('NÃO')
            )

        self.logger.info("✅ Correções aplicadas com sucesso")
        return df

    # Processa um único arquivo CSV
    def processar_arquivo_individual(self, caminho_arquivo, forcar_reprocessamento=False):        
        ano = self.extrair_ano_do_arquivo(caminho_arquivo)
        if ano is None:
            self.logger.warning(f"Ano não detectado para {caminho_arquivo}")
            ano = "desconhecido"

        nome_arquivo = os.path.basename(caminho_arquivo)
        arquivo_saida = os.path.join(self.pasta_processed, f"compras_{ano}_tratado.csv")

        # Verifica se já foi processado
        if os.path.exists(arquivo_saida) and not forcar_reprocessamento:
            self.logger.info(f"Arquivo já processado ({ano}). Usando versão tratada.")
            try:
                return pd.read_csv(arquivo_saida, encoding='utf-8-sig')
            except Exception as e:
                self.logger.error(f"Erro ao carregar arquivo tratado: {e}")
                return None

        self.logger.info(f"🎯 Iniciando processamento de: {nome_arquivo}")

        # 1. Leitura flexível
        df = self._ler_arquivo_flexivel(caminho_arquivo)
        if df is None or df.empty:
            self.logger.error(f"Falha ao ler arquivo: {nome_arquivo}")
            return None

        self.logger.info(f"✅ Arquivo lido com {len(df)} registros e {len(df.columns)} colunas")

        # 2. Correções específicas
        df_tratado = self._corrigir_problemas_especificos(df)
        if df_tratado is None or df_tratado.empty:
            self.logger.error(f"❌ Tratamento falhou para: {nome_arquivo}")
            return None

        # 3. Garante coluna de ano
        if 'ano_compra' not in df_tratado.columns:
            df_tratado['ano_compra'] = ano

        # 4. Salva arquivo tratado
        try:
            df_tratado.to_csv(arquivo_saida, index=False, encoding='utf-8-sig')
            self.logger.info(f"💾 Arquivo tratado salvo: {arquivo_saida} ({len(df_tratado)} registros)")
        except Exception as e:
            self.logger.error(f"Erro ao salvar arquivo tratado: {e}")
            return None

        return df_tratado

    # Consolida todos os anos processados em um único DataFrame
    def consolidar_todos_anos(self, forcar_reprocessamento=False):        
        self.logger.info(" Iniciando consolidação de todos os anos...")

        arquivos = self.listar_arquivos_raw()
        if not arquivos:
            self.logger.error("❌ Nenhum arquivo CSV encontrado.")
            self.logger.error(f"Coloque os arquivos .csv em: {self.pasta_raw}")
            return None

        todos_dados = []
        anos_processados = []

        for arquivo in arquivos:
            try:
                df_ano = self.processar_arquivo_individual(arquivo, forcar_reprocessamento)

                if df_ano is None or df_ano.empty:
                    self.logger.warning(f"⚠️ Ignorado: {os.path.basename(arquivo)} — DataFrame vazio ou falha no processamento.")
                    continue

                # Verificações adicionais
                colunas_esperadas = ['ano_compra', 'cnpj_instituicao', 'preco_total']
                colunas_faltando = [col for col in colunas_esperadas if col not in df_ano.columns]

                if colunas_faltando:
                    self.logger.warning(f"⚠️ Ignorado: {os.path.basename(arquivo)} — Colunas ausentes: {colunas_faltando}")
                    continue

                # Adiciona ao consolidado
                todos_dados.append(df_ano)
                ano = self.extrair_ano_do_arquivo(arquivo)
                anos_processados.append(ano)
                self.logger.info(f"✅ {ano}: {len(df_ano):,} registros processados")

            except Exception as e:
                self.logger.error(f"❌ Erro ao processar {os.path.basename(arquivo)}: {e}")
                continue

        if not todos_dados:
            self.logger.error("❌ Nenhum dado foi processado com sucesso.")
            return None

        # Consolida os dados
        df_consolidado = pd.concat(todos_dados, ignore_index=True)

        # Ordena por data de compra, se existir
        if 'compra' in df_consolidado.columns:
            df_consolidado['compra'] = pd.to_datetime(df_consolidado['compra'], errors='coerce')
            df_consolidado = df_consolidado.sort_values('compra')

        # Define nome do arquivo consolidado
        anos_validos = [ano for ano in anos_processados if isinstance(ano, int)]
        if anos_validos:
            anos_str = f"{min(anos_validos)}_{max(anos_validos)}"
        else:
            anos_str = "desconhecido"

        arquivo_consolidado = os.path.join(self.pasta_processed, f"compras_consolidado_{anos_str}.csv")

        try:
            df_consolidado.to_csv(arquivo_consolidado, index=False, encoding='utf-8-sig')
            self.logger.info("🎉 Consolidação completa!")
            self.logger.info(f"📈 Total de registros: {len(df_consolidado):,}")
            self.logger.info(f"📅 Período: {anos_str}")
            self.logger.info(f"💾 Arquivo salvo: {arquivo_consolidado}")
        except Exception as e:
            self.logger.error(f"Erro ao salvar arquivo consolidado: {e}")
            return None

        # Gera estatísticas se possível
        try:
            self._gerar_estatisticas_consolidadas(df_consolidado, anos_validos)
        except Exception as e:
            self.logger.warning(f"Falha ao gerar estatísticas: {e}")

        return df_consolidado

    # Gera estatísticas consolidadas
    def _gerar_estatisticas_consolidadas(self, df, anos):        
        try:
            stats = {
                'total_registros': len(df),
                'total_anos': len(set(anos)),
                'anos_processados': sorted(set(anos)),
                'total_gasto': f"R$ {df['preco_total'].sum():,.2f}" if 'preco_total' in df.columns else "N/A",
                'estados_ativos': df['uf'].nunique() if 'uf' in df.columns else "N/A",
                'municipios_ativos': df['municipio_instituicao'].nunique() if 'municipio_instituicao' in df.columns else "N/A",
                'medicamentos_diferentes': df['descricao_catmat'].nunique() if 'descricao_catmat' in df.columns else "N/A"
            }

            # Período de compras
            if 'compra' in df.columns:
                datas_validas = pd.to_datetime(df['compra'], errors='coerce')
                datas_validas = datas_validas.dropna()
                if not datas_validas.empty:
                    stats['periodo'] = f"{datas_validas.min().strftime('%Y-%m-%d')} a {datas_validas.max().strftime('%Y-%m-%d')}"
                else:
                    stats['periodo'] = "N/A"

            # Salvar estatísticas
            stats_df = pd.DataFrame([stats])
            anos_validos = [ano for ano in anos if isinstance(ano, int)]
            anos_str = f"{min(anos_validos)}_{max(anos_validos)}" if anos_validos else "desconhecido"
            stats_path = os.path.join(self.pasta_outputs, f"estatisticas_consolidadas_{anos_str}.csv")

            stats_df.to_csv(stats_path, index=False, encoding='utf-8-sig')
            self.logger.info("📊 Estatísticas consolidadas geradas com sucesso:")
            for key, value in stats.items():
                self.logger.info(f"   {key}: {value}")

        except Exception as e:
            self.logger.error(f"❌ Erro ao gerar estatísticas: {e}")


# --- FUNÇÃO DE FACILIDADE---

# Processa todos os arquivos CSV em uma pasta
def processar_tudo(pasta_dados="data", forcar_reprocessamento=False):    
    etl = ETLComprasPublicas(pasta_dados)
    return etl.consolidar_todos_anos(forcar_reprocessamento)


if __name__ == "__main__":
    print("=" * 60)
    print("🏥 ETL - COMPRAS PÚBLICAS DE MEDICAMENTOS")
    print("=" * 60)

    try:
        df_final = processar_tudo(forcar_reprocessamento=True)

        if df_final is not None and not df_final.empty:
            print(f"\n✅ PROCESSAMENTO COMPLETO! {len(df_final):,} registros consolidados.")
        else:
            print("\n❌ FALHA NO PROCESSAMENTO.")
            print("Verifique:")
            print("1. Se os arquivos CSV estão em: data/raw/")
            print("2. Se os nomes dos arquivos contêm o ano (ex: 2025.csv)")

    except Exception as e:
        print("\n🚨 ERRO CRÍTICO DURANTE A EXECUÇÃO DO ETL")
        print(f"Detalhes: {e}")