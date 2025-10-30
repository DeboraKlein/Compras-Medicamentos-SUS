# =================================================================
# ARQUIVO PRINCIPAL DO PIPELINE ETL
# =================================================================
import os
import pandas as pd
import traceback
import re 
import argparse

from src.etl_compras import ETLComprasPublicas
from src.etl_compras_antigos import ETLComprasAntigos
from src.modelagem_dim import (gerar_id_pedido, 
                               gerar_mini_fato_radar_enriquecida, 
                               calcular_indice_priorizacao, 
                               calcular_risco_intermitencia, 
                               calcular_concentracao_fornecedor,
                               calcular_zscore_risco
                               )
from src.dimensoes import criar_e_integrar_dimensoes

# =================================================================
# FUNÇÃO PRINCIPAL 
# =================================================================
def main():
    print("=" * 60)
    print(" PIPELINE ETL - COMPRAS PÚBLICAS DE MEDICAMENTOS")
    print("=" * 60)
    
    pasta_base = os.path.dirname(os.path.abspath(__file__))
    pasta_dados = os.path.join(pasta_base, "data")
    pasta_raw = os.path.join(pasta_dados, "raw")
    pasta_outputs = os.path.join(pasta_dados, "outputs")

    # 1. Garante que as pastas existem
    for pasta in [pasta_dados, pasta_raw, pasta_outputs]:
        os.makedirs(pasta, exist_ok=True)

    print(f" Pasta de dados: {pasta_dados}")

    if not os.path.exists(pasta_raw):
        print(f" Pasta 'raw' não encontrada: {pasta_raw}")
        return

    arquivos_raw = [f for f in os.listdir(pasta_raw) if f.endswith('.csv')]
    if not arquivos_raw:
        print(f" Nenhum arquivo CSV encontrado em: {pasta_raw}")
        return

    print(f" Arquivos encontrados: {arquivos_raw}")
    
    # 2. Instanciar os dois ETLs
    etl_novo = ETLComprasPublicas(pasta_dados)
    etl_antigo = ETLComprasAntigos(pasta_dados)

    todos_dados = []
    anos_processados = []

    # 2.1. Separar arquivos novos
    arquivos_novos = []
    for f in arquivos_raw:
        match = re.search(r'20\d{2}', f)
        if match:
            ano = int(match.group())
            if ano >= 2023:
                arquivos_novos.append(f)

    # 2.2. Processar ANOS ANTIGOS (2020-2022) em lote
    print("\n Processando ANOS ANTIGOS (2020-2022) em lote...")
    try:
        df_antigo_consolidado = etl_antigo.processar_todos_antigos() 
        
        if df_antigo_consolidado is not None and not df_antigo_consolidado.empty:
            todos_dados.append(df_antigo_consolidado)
            anos_antigos = df_antigo_consolidado['ano_compra'].unique().tolist()
            anos_processados.extend(anos_antigos)
            print(f"    ANTIGO - {len(df_antigo_consolidado):,} registros consolidados (Anos: {', '.join(map(str, anos_antigos))})")
        else:
            print("    Processamento dos ANOS ANTIGOS não retornou dados.")

    except Exception as e:
        print(f"    ERRO no processamento em lote dos ANOS ANTIGOS: {e}")
        traceback.print_exc()

    # 2.3. Processar ANOS NOVOS (2023+) individualmente
    for arquivo in arquivos_novos:
        caminho_arquivo = os.path.join(pasta_raw, arquivo)
        nome_arquivo = os.path.basename(arquivo)
        
        print(f"\n Processando ANO NOVO: {nome_arquivo}...")
        try:
            df_ano = etl_novo.processar_arquivo_individual(caminho_arquivo, forcar_reprocessamento=True)

            if df_ano is not None and not df_ano.empty:
                todos_dados.append(df_ano)
                ano = df_ano['ano_compra'].iloc[0] if 'ano_compra' in df_ano.columns else re.search(r'20\d{2}', nome_arquivo).group()
                anos_processados.append(ano)
                print(f"    NOVO - {len(df_ano):,} registros processados (Ano: {ano})")
            else:
                print(f"    Processamento de {nome_arquivo} retornou vazio.")

        except Exception as e:
            print(f"    ERRO ao processar {nome_arquivo}: {e}")
            traceback.print_exc()

    # 2.4. CONSOLIDAÇÃO DOS DADOS
    if not todos_dados:
        print(" Nenhum dado foi processado com sucesso.")
        return

    print("\n Consolidando todos os anos...")
    df_final = pd.concat(todos_dados, ignore_index=True)
    print(f" Dados consolidados: {len(df_final):,} registros")

    # 3. GERAÇÃO DO HASH ID_PEDIDO
    print("\n Gerando ID único para cada pedido...")
    df_final = gerar_id_pedido(df_final)

    # 4. Salvar arquivo consolidado
    print(f" SALVANDO ARQUIVO CONSOLIDADO...")
    caminho_saida = os.path.join(pasta_outputs, "compras_consolidado_final.csv")
    df_final.to_csv(caminho_saida, index=False, encoding='utf-8-sig', sep=';')
    print(f" Arquivo consolidado salvo em: {caminho_saida}")

    # 5. MODELAGEM DIMENSIONAL
    print("\n[PASSO 5] Modelagem Dimensional (Dimensões e Tabela Fato)...")
    df_fato = criar_e_integrar_dimensoes(df_final, pasta_outputs) 
    print(f" Modelagem Dimensional concluída. Tabela Fato: {len(df_fato):,} registros.")
    
    # =====================================================================
    #  FASE DE ENRIQUECIMENTO DE DADOS (Risco e Demanda)
    # =====================================================================

    # 6. Risco de Preço (Z-Score)
    # Esta coluna ('score_z_risco') é a base para o Índice de Priorização (Passo 7).
    print("\n[PASSO 6] Cálculo do Z-Score de Risco de Preço...")
    df_fato = calcular_zscore_risco(df_fato)
    print(f" Z-Score de Risco calculado.")

    # 7. CÁLCULO DO ÍNDICE DE PRIORIZAÇÃO
    # Usa 'score_z_risco' e cria as colunas 'demanda_valor' e 'indice_priorizacao'.
    print("\n[PASSO 7] Cálculo do Índice de Priorização de Compras...")
    df_fato = calcular_indice_priorizacao(df_fato)
    print(f" Índice de Priorização e 'demanda_valor' calculados.")

    # 8. Risco de Intermitência (Instabilidade da Demanda)
    # Corrigido: Removida a duplicação e mantida uma única chamada.
    print("\n PASSO [8]: Cálculo de Risco de Intermitência (Demanda)...")
    df_fato = calcular_risco_intermitencia(df_fato) 
    print(" Risco de Intermitência adicionado.")

    # 9. Risco de Concentração de Fornecedor
    print("\n PASSO [9]: Cálculo de Concentração de Fornecedor (Dependência)...")
    df_fato = calcular_concentracao_fornecedor(df_fato)
    print(" Risco de Concentração adicionado.")

    # 10. (Antigo 11.) TABELA RADAR
    print("\n Gerando Mini Tabela Fato para o Radar de Oportunidades...")
    df_radar = gerar_mini_fato_radar_enriquecida(df_fato)
                    
    if not df_radar.empty:
        arquivo_radar = os.path.join(pasta_outputs, "mini_fato_radar_oportunidades.csv")
        df_radar.to_csv(arquivo_radar, sep=';', index=False, encoding='utf-8-sig')
        print(f" Mini Fato Radar exportada para: {arquivo_radar}")
    else:
        print(" A Mini Fato Radar está vazia. Verifique os filtros de PMP/Qtd.")
        
    # 11. EXPORTAÇÃO FINAL DA TABELA FATO
    print("\n[PASSO 9] Exportando Tabela Fato Final...")
    arquivo_fato = os.path.join(pasta_outputs, "fato_compras_medicamentos.csv")
    df_fato.to_csv(arquivo_fato, index=False, sep=';', encoding='utf-8-sig')
    print(f" Tabela Fato exportada: {arquivo_fato}")

    # 12. ESTATÍSTICAS E RELATÓRIO FINAL
    print(f"\n PROCESSAMENTO CONCLUÍDO!")
    print(f"    Total de registros: {len(df_final):,}")

    # 13. Estatísticas básicas
    if 'preco_total' in df_final.columns:
        total_gasto = df_final['preco_total'].sum()
        print(f"    Total gasto: R$ {total_gasto:,.2f}")
        
        # Gastos por ano
        if 'ano_compra' in df_final.columns:
            gastos_por_ano = df_final.groupby('ano_compra')['preco_total'].sum()
            print(f"    Gastos por ano:")
            for ano, gasto in gastos_por_ano.items():
                print(f"      {ano}: R$ {gasto:,.2f}")

        anos_unicos = sorted([a for a in set(anos_processados) if a != 'desconhecido'])
        print(f"    Anos processados: {anos_unicos}")

        if 'uf' in df_final.columns:
            print(f"    Estados participantes: {df_final['uf'].nunique()}")

        if 'descricao_catmat' in df_final.columns:
            print(f"    Medicamentos diferentes: {df_final['descricao_catmat'].nunique()}")

    # 14. Resumo executivo final
        print(f"\n" + "=" * 50)
        print(f" RESUMO EXECUTIVO FINAL")
        print(f"=" * 50)
    
        if anos_unicos:
            print(f" Período: {min(anos_unicos)} a {max(anos_unicos)}")
        
            print(f" Total de registros: {len(df_final):,}")
        
        if 'preco_total' in df_final.columns:
            print(f" Gasto total: R$ {total_gasto:,.2f}")
        if anos_unicos:
             print(f" Média anual: R$ {total_gasto/len(anos_unicos):,.2f}")
    
        if 'uf' in df_final.columns:
            print(f" Estados: {df_final['uf'].nunique()}")
    
        if 'municipio_instituicao' in df_final.columns:
            print(f" Municípios: {df_final['municipio_instituicao'].nunique()}")
    
        if 'descricao_catmat' in df_final.columns:
            print(f" Medicamentos: {df_final['descricao_catmat'].nunique()}")

        print(f"\n PIPELINE COMPLETADO COM SUCESSO!")


def processar_apenas_analises():
    """
    Função para processar apenas as análises se os dados já estiverem consolidados
    """
    print(" PROCURANDO DADOS CONSOLIDADOS PARA ANÁLISE...")
    
    pasta_base = os.path.dirname(os.path.abspath(__file__))
    pasta_outputs = os.path.join(pasta_base, "data", "outputs")
    arquivo_consolidado = os.path.join(pasta_outputs, "compras_consolidado_final.csv")
    
    if not os.path.exists(arquivo_consolidado):
        print(f" Arquivo consolidado não encontrado: {arquivo_consolidado}")
        print("   Execute primeiro o pipeline completo com: python main.py")
        return
    
    try:
        df_final = pd.read_csv(arquivo_consolidado, sep=';', encoding='utf-8-sig')
        print(f" Dados carregados: {len(df_final):,} registros")
                        
        print(f" ANÁLISES GERADAS COM SUCESSO!")
        
    except Exception as e:
        print(f" Erro ao processar análises: {e}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Pipeline ETL - Compras Públicas de Medicamentos')
    parser.add_argument('--apenas-analises', action='store_true', 
                       help='Executa apenas as análises (sem reprocessar dados)')
    
    args = parser.parse_args()
    
    if args.apenas_analises:
        processar_apenas_analises()
    else:
        main()
