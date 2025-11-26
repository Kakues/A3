import pandas as pd
import numpy as np
from datetime import datetime
import warnings
import os
import glob
warnings.filterwarnings('ignore')

class CruzeiroPowerBIExporter:
    """
    Sistema de análise e exportação de dados do Cruzeiro para Power BI
    VERSÃO 2.0 - Com dados financeiros detalhados 2019-2025
    """
    
    def __init__(self, caminho_dados='data/data.csv'):
        """
        Inicializa o exportador
        
        Args:
            caminho_dados: Caminho para a pasta com os CSVs (padrão: 'data/data.csv')
        """
        self.dfs = {}
        self.correlations = {}
        self.caminho_dados = caminho_dados
        self._verificar_arquivos()
    
    def _verificar_arquivos(self):
        """Verifica e lista todos os arquivos CSV disponíveis"""
        print("\n" + "="*60)
        print("VERIFICANDO ARQUIVOS CSV")
        print("="*60)
        
        # Tentar vários caminhos possíveis
        caminhos_possiveis = [
            self.caminho_dados,
            'data/data.csv',
            'data.csv',
            'data\\data.csv',
            '.'
        ]
        
        csv_files = []
        caminho_encontrado = None
        
        for caminho in caminhos_possiveis:
            if os.path.exists(caminho):
                arquivos = glob.glob(os.path.join(caminho, "*.csv"))
                if arquivos:
                    csv_files = arquivos
                    caminho_encontrado = caminho
                    break
        
        if not csv_files:
            print("❌ ERRO: Nenhum arquivo CSV encontrado!")
            print(f"   Pasta atual: {os.getcwd()}")
            print(f"   Caminhos tentados: {caminhos_possiveis}")
            print("\n💡 SOLUÇÃO:")
            print("   1. Verifique se os arquivos CSV estão em 'data/data.csv/'")
            print("   2. Ou execute: exporter = CruzeiroPowerBIExporter(caminho_dados='SEU_CAMINHO')")
            raise FileNotFoundError("Nenhum arquivo CSV encontrado")
        
        print(f"✓ Pasta encontrada: {os.path.abspath(caminho_encontrado)}")
        
        print(f"✓ Encontrados {len(csv_files)} arquivos CSV:\n")
        for i, file in enumerate(csv_files, 1):
            size = os.path.getsize(file) / 1024  # KB
            print(f"  {i}. {file} ({size:.1f} KB)")
        
        print("\n" + "="*60)
        print("MAPEANDO ARQUIVOS...")
        print("="*60 + "\n")
        
        # Mapear nomes de arquivos - Mais flexível
        self.arquivos = {}
        
        for file in csv_files:
            # Normalizar o nome do arquivo
            nome_arquivo = os.path.basename(file).lower()
            nome_limpo = nome_arquivo.replace('%', '').replace(' ', '_').replace('ç', 'c').replace('ã', 'a').replace('õ', 'o').replace('é', 'e')
            
            # ========== NOVO: Detectar arquivo de receitas detalhadas ==========
            if 'receitas_detalhadas' in nome_limpo or 'receita_detalhada' in nome_limpo:
                self.arquivos['receitas_detalhadas'] = file
                print(f"  ✓ receitas_detalhadas (NOVO): {os.path.basename(file)}")
            # ===================================================================
            
            # Mapear cada tipo de arquivo (código existente)
            elif 'setor_fatos' in nome_limpo or 'setor_fato' in nome_limpo:
                self.arquivos['setor_fatos'] = file
                print(f"  ✓ setor_fatos: {os.path.basename(file)}")
                
            elif 'setor_por_jogo' in nome_limpo:
                if 'setor_por_jogo' not in self.arquivos:
                    self.arquivos['setor_por_jogo'] = file
                    print(f"  ✓ setor_por_jogo: {os.path.basename(file)}")
                    
            elif 'jogo_fatos' in nome_limpo or 'jogo_fato' in nome_limpo:
                self.arquivos['jogo_fatos'] = file
                print(f"  ✓ jogo_fatos: {os.path.basename(file)}")
                
            elif 'informacoes_jogos' in nome_limpo or 'informacao_jogo' in nome_limpo:
                if 'jogo_fatos' not in self.arquivos:
                    self.arquivos['jogo_fatos'] = file
                    print(f"  ✓ jogo_fatos (alt): {os.path.basename(file)}")
                    
            elif 'lotacao' in nome_limpo:
                self.arquivos['lotacao'] = file
                print(f"  ✓ lotacao: {os.path.basename(file)}")
                
            elif 'demografico' in nome_limpo or 'perfil' in nome_limpo:
                self.arquivos['demografico'] = file
                print(f"  ✓ demografico: {os.path.basename(file)}")
                
            elif 'receita_fatos' in nome_limpo or 'receita_fato' in nome_limpo:
                self.arquivos['receita'] = file
                print(f"  ✓ receita: {os.path.basename(file)}")
                
            elif 'receitas_mineirao' in nome_limpo or ('mineirao' in nome_limpo and ('2014' in nome_limpo or '2022' in nome_limpo)):
                self.arquivos['receitas_historicas'] = file
                print(f"  ✓ receitas_historicas: {os.path.basename(file)}")
                
            elif 'socio' in nome_limpo and 'torcedor' in nome_limpo:
                self.arquivos['socio_torcedor'] = file
                print(f"  ✓ socio_torcedor: {os.path.basename(file)}")
                
            elif 'ticket_medio_estimativa' in nome_limpo:
                self.arquivos['ticket_medio_estimativa'] = file
                print(f"  ✓ ticket_medio_estimativa: {os.path.basename(file)}")
                
            elif 'ticket_medio_torcedor' in nome_limpo:
                self.arquivos['ticket_medio_torcedor'] = file
                print(f"  ✓ ticket_medio_torcedor: {os.path.basename(file)}")
                
            elif 'vendas_canal' in nome_limpo:
                self.arquivos['vendas_canal'] = file
                print(f"  ✓ vendas_canal: {os.path.basename(file)}")
                
            elif 'vendas_competicao' in nome_limpo:
                self.arquivos['vendas_competicao'] = file
                print(f"  ✓ vendas_competicao: {os.path.basename(file)}")
                
            elif 'precos_produtos' in nome_limpo or 'preco_produto' in nome_limpo:
                self.arquivos['precos_produtos'] = file
                print(f"  ✓ precos_produtos: {os.path.basename(file)}")
                
            elif 'publico_cruzeiro' in nome_limpo:
                self.arquivos['publico_cruzeiro'] = file
                print(f"  ✓ publico_cruzeiro: {os.path.basename(file)}")
        
        print(f"\n✓ Total de arquivos mapeados: {len(self.arquivos)}/{len(csv_files)}")
        
        if len(self.arquivos) < len(csv_files):
            print(f"⚠ {len(csv_files) - len(self.arquivos)} arquivo(s) não foram mapeados (podem ser duplicados ou não utilizados)")
        
        print()
    
    def carregar_dados(self):
        """Carrega todos os CSVs e realiza limpeza inicial"""
        
        print("CARREGANDO DADOS...")
        print("="*60 + "\n")
        
        # ========== NOVO: Carregar receitas detalhadas ==========
        if 'receitas_detalhadas' in self.arquivos:
            try:
                self.dfs['receitas_detalhadas'] = pd.read_csv(self.arquivos['receitas_detalhadas'])
                
                # Limpeza e transformações
                df = self.dfs['receitas_detalhadas']
                
                # Converter ano para inteiro
                df['ano'] = df['ano'].astype(int)
                
                # Criar taxa de ocupação decimal
                df['taxa_ocupacao_decimal'] = df['taxa_ocupacao_percent'] / 100
                
                # Calcular gap de otimização
                df['gap_otimizacao'] = df['receita_bruta_ideal_ingressos'] - df['receita_ingresso']
                
                # Calcular percentuais de receita
                df['perc_receita_produtos'] = (df['receita_produtos_internos'] / df['total_arrecadado'] * 100).round(2)
                df['perc_receita_camarotes'] = (df['receita_camarotes'] / df['total_arrecadado'] * 100).round(2)
                df['perc_receita_estacionamento'] = (df['receita_estacionamento'] / df['total_arrecadado'] * 100).round(2)
                
                # Classificar tipo de adversário
                df['tipo_adversario'] = df['times_que_jogaram'].apply(self._classificar_adversario)
                df['eh_classico'] = df['times_que_jogaram'].str.contains('Atlético-MG', case=False, na=False)
                
                # Identificar era (pré/pós pandemia)
                df['era'] = df['ano'].apply(lambda x: 'Pré-COVID' if x < 2020 else ('Pandemia' if x <= 2021 else 'Pós-COVID'))
                
                print(f"  ✓ {os.path.basename(self.arquivos['receitas_detalhadas'])} carregado - {len(df)} jogos")
                print(f"     Período: {df['ano'].min()} a {df['ano'].max()}")
                print(f"     Competições: {df['competicao'].nunique()} diferentes")
                
            except Exception as e:
                print(f"  ⚠ Erro ao carregar receitas_detalhadas: {e}")
                self.dfs['receitas_detalhadas'] = pd.DataFrame()
        else:
            print("  ⚠ Arquivo receitas_detalhadas não encontrado")
            self.dfs['receitas_detalhadas'] = pd.DataFrame()
        # ========================================================
        
        # 1. Setor Fatos (código existente)
        if 'setor_fatos' in self.arquivos:
            self.dfs['setor_fatos'] = pd.read_csv(self.arquivos['setor_fatos'], skipinitialspace=True)
            self.dfs['setor_fatos'].columns = self.dfs['setor_fatos'].columns.str.strip()
            print(f"  ✓ {os.path.basename(self.arquivos['setor_fatos'])} carregado")
        else:
            print("  ⚠ Arquivo setor_fatos não encontrado, usando dados parciais")
            self.dfs['setor_fatos'] = pd.DataFrame()
        
        # 2. Jogo Fatos (Principal)
        if 'jogo_fatos' in self.arquivos:
            self.dfs['jogo_fatos'] = pd.read_csv(self.arquivos['jogo_fatos'])
            self.dfs['jogo_fatos']['data'] = pd.to_datetime(self.dfs['jogo_fatos']['data'], format='%d/%m/%Y', errors='coerce')
            
            # Extrair público total
            if 'publico total' in self.dfs['jogo_fatos'].columns:
                self.dfs['jogo_fatos']['publico_total'] = self.dfs['jogo_fatos']['publico total'].astype(str).str.extract('(\d+)', expand=False).astype(float)
            elif 'publico_total' in self.dfs['jogo_fatos'].columns:
                self.dfs['jogo_fatos']['publico_total'] = self.dfs['jogo_fatos']['publico_total'].astype(str).str.extract('(\d+)', expand=False).astype(float)
            
            # Padronizar coluna jogo_id
            if 'jogo id' in self.dfs['jogo_fatos'].columns:
                self.dfs['jogo_fatos']['jogo_id'] = self.dfs['jogo_fatos']['jogo id'].str.strip()
            elif 'jogo_id' in self.dfs['jogo_fatos'].columns:
                self.dfs['jogo_fatos']['jogo_id'] = self.dfs['jogo_fatos']['jogo_id'].str.strip()
            
            print(f"  ✓ {os.path.basename(self.arquivos['jogo_fatos'])} carregado")
        else:
            raise FileNotFoundError("Arquivo jogo_fatos.csv é obrigatório!")
        
        # 3-14. Outros arquivos (código existente mantido)
        if 'lotacao' in self.arquivos:
            self.dfs['lotacao'] = pd.read_csv(self.arquivos['lotacao'])
            self.dfs['lotacao']['jogo_id'] = self.dfs['lotacao']['jogo_id'].str.strip()
            print(f"  ✓ {os.path.basename(self.arquivos['lotacao'])} carregado")
        else:
            print("  ⚠ Arquivo lotacao não encontrado")
            self.dfs['lotacao'] = pd.DataFrame()
        
        if 'demografico' in self.arquivos:
            self.dfs['demografico'] = pd.read_csv(self.arquivos['demografico'])
            self.dfs['demografico']['Jogo_ID'] = self.dfs['demografico']['Jogo_ID'].str.strip()
            print(f"  ✓ {os.path.basename(self.arquivos['demografico'])} carregado")
        else:
            print("  ⚠ Arquivo demográfico não encontrado")
            self.dfs['demografico'] = pd.DataFrame()
        
        if 'receita' in self.arquivos:
            self.dfs['receita'] = pd.read_csv(self.arquivos['receita'])
            self.dfs['receita']['data'] = pd.to_datetime(self.dfs['receita']['data'], format='%d/%m/%Y', errors='coerce')
            self.dfs['receita']['jogo_id'] = self.dfs['receita']['jogo_id'].str.strip()
            print(f"  ✓ {os.path.basename(self.arquivos['receita'])} carregado")
        else:
            print("  ⚠ Arquivo receita_fatos não encontrado")
            self.dfs['receita'] = pd.DataFrame()
        
        if 'receitas_historicas' in self.arquivos:
            self.dfs['receitas_historicas'] = pd.read_csv(self.arquivos['receitas_historicas'])
            self.dfs['receitas_historicas']['data'] = pd.to_datetime(self.dfs['receitas_historicas']['data'], errors='coerce')
            self.dfs['receitas_historicas']['Ano'] = self.dfs['receitas_historicas']['Ano'].astype(int)
            print(f"  ✓ {os.path.basename(self.arquivos['receitas_historicas'])} carregado")
        else:
            print("  ⚠ Arquivo receitas_historicas não encontrado")
            self.dfs['receitas_historicas'] = pd.DataFrame()
        
        if 'socio_torcedor' in self.arquivos:
            self.dfs['socio_torcedor'] = pd.read_csv(self.arquivos['socio_torcedor'])
            print(f"  ✓ {os.path.basename(self.arquivos['socio_torcedor'])} carregado")
        else:
            print("  ⚠ Arquivo socio_torcedor não encontrado")
            self.dfs['socio_torcedor'] = pd.DataFrame()
        
        # Arquivos adicionais com tratamento de erro
        for key in ['ticket_medio_estimativa', 'ticket_medio_torcedor', 'vendas_canal', 
                    'vendas_competicao', 'precos_produtos', 'setor_por_jogo']:
            if key in self.arquivos:
                try:
                    self.dfs[key] = pd.read_csv(self.arquivos[key])
                    print(f"  ✓ {os.path.basename(self.arquivos[key])} carregado")
                except Exception as e:
                    print(f"  ⚠ Erro ao carregar {key}: {e}")
        
        # Público Cruzeiro com tratamento especial
        if 'publico_cruzeiro' in self.arquivos:
            try:
                self.dfs['publico_cruzeiro'] = pd.read_csv(
                    self.arquivos['publico_cruzeiro'], 
                    on_bad_lines='skip',
                    engine='python'
                )
                print(f"  ✓ {os.path.basename(self.arquivos['publico_cruzeiro'])} carregado")
            except Exception as e:
                print(f"  ⚠ Erro ao carregar publico_cruzeiro: {e}")
        
        print(f"\n✓ Processo de carga concluído! Total: {len(self.dfs)} datasets carregados\n")
    
    def _classificar_adversario(self, times):
        """Classifica o adversário por importância"""
        grandes = ['Flamengo', 'Palmeiras', 'São Paulo', 'Corinthians', 'Atlético-MG', 
                   'Grêmio', 'Internacional', 'Santos', 'Vasco']
        if any(grande in times for grande in grandes):
            return 'Grande'
        return 'Médio/Pequeno'
    
    # ========== NOVO: Funções para análise de receitas detalhadas ==========
    
    def criar_analise_precificacao(self):
        """Cria análise detalhada de precificação de ingressos"""
        
        if self.dfs['receitas_detalhadas'].empty:
            print("⚠ Dados de receitas detalhadas não disponíveis")
            return
        
        print("Criando análise de precificação...")
        
        df = self.dfs['receitas_detalhadas'].copy()
        
        # Remover jogos da pandemia (público zero)
        df = df[df['publico_presente'] > 0]
        
        # Análise por competição e ano
        precificacao = df.groupby(['ano', 'competicao']).agg({
            'preco_medio_inteira': 'mean',
            'preco_medio_meia': 'mean',
            'ticket_medio_real_ingresso': 'mean',
            'ticket_medio_ideal_ingressos': 'mean',
            'fator_desconto_socios_percent': 'mean',
            'gap_otimizacao': 'sum',
            'publico_presente': 'sum',
            'total_arrecadado': 'sum'
        }).reset_index()
        
        # Calcular eficiência de precificação
        precificacao['eficiencia_precificacao'] = (
            precificacao['ticket_medio_real_ingresso'] / 
            precificacao['ticket_medio_ideal_ingressos'] * 100
        ).round(2)
        
        precificacao.columns = ['ano', 'competicao', 'preco_medio_inteira', 'preco_medio_meia',
                                'ticket_medio_real', 'ticket_medio_ideal', 'desconto_medio_socios',
                                'gap_otimizacao_total', 'publico_total', 'receita_total',
                                'eficiencia_precificacao_percent']
        
        self.dfs['analise_precificacao'] = precificacao
        print(f"✓ Análise de Precificação criada com {len(precificacao)} registros!\n")
    
    def criar_mix_receitas(self):
        """Cria análise do mix de receitas (ingressos, produtos, camarotes, estacionamento)"""
        
        if self.dfs['receitas_detalhadas'].empty:
            print("⚠ Dados de receitas detalhadas não disponíveis")
            return
        
        print("Criando análise de mix de receitas...")
        
        df = self.dfs['receitas_detalhadas'].copy()
        df = df[df['publico_presente'] > 0]
        
        # Análise agregada por ano e competição
        mix = df.groupby(['ano', 'competicao']).agg({
            'receita_ingresso': 'sum',
            'receita_produtos_internos': 'sum',
            'receita_camarotes': 'sum',
            'receita_estacionamento': 'sum',
            'total_arrecadado': 'sum',
            'publico_presente': 'sum'
        }).reset_index()
        
        # Calcular percentuais
        mix['perc_ingresso'] = (mix['receita_ingresso'] / mix['total_arrecadado'] * 100).round(2)
        mix['perc_produtos'] = (mix['receita_produtos_internos'] / mix['total_arrecadado'] * 100).round(2)
        mix['perc_camarotes'] = (mix['receita_camarotes'] / mix['total_arrecadado'] * 100).round(2)
        mix['perc_estacionamento'] = (mix['receita_estacionamento'] / mix['total_arrecadado'] * 100).round(2)
        
        # Receita per capita por categoria
        mix['receita_per_capita_ingresso'] = (mix['receita_ingresso'] / mix['publico_presente']).round(2)
        mix['receita_per_capita_produtos'] = (mix['receita_produtos_internos'] / mix['publico_presente']).round(2)
        
        self.dfs['mix_receitas'] = mix
        print(f"✓ Mix de Receitas criado com {len(mix)} registros!\n")
    
    def criar_analise_ocupacao(self):
        """Cria análise de taxa de ocupação do estádio"""
        
        if self.dfs['receitas_detalhadas'].empty:
            print("⚠ Dados de receitas detalhadas não disponíveis")
            return
        
        print("Criando análise de ocupação...")
        
        df = self.dfs['receitas_detalhadas'].copy()
        df = df[df['publico_presente'] > 0]
        
        # Análise por ano, competição e tipo de adversário
        ocupacao = df.groupby(['ano', 'competicao', 'tipo_adversario']).agg({
            'taxa_ocupacao_percent': ['mean', 'min', 'max'],
            'publico_presente': ['sum', 'mean'],
            'publico_pagante': ['sum', 'mean'],
            'total_arrecadado': 'sum'
        }).reset_index()
        
        # Renomear colunas
        ocupacao.columns = ['_'.join(col).strip('_') for col in ocupacao.columns.values]
        ocupacao.columns = ['ano', 'competicao', 'tipo_adversario',
                           'taxa_ocupacao_media', 'taxa_ocupacao_min', 'taxa_ocupacao_max',
                           'publico_total', 'publico_medio',
                           'pagantes_total', 'pagantes_medio',
                           'receita_total']
        
        # Calcular % não pagantes
        ocupacao['perc_nao_pagantes'] = (
            (ocupacao['publico_total'] - ocupacao['pagantes_total']) / 
            ocupacao['publico_total'] * 100
        ).round(2)
        
        self.dfs['analise_ocupacao'] = ocupacao
        print(f"✓ Análise de Ocupação criada com {len(ocupacao)} registros!\n")
    
    def criar_serie_temporal_completa(self):
        """Cria série temporal completa 2019-2025"""
        
        if self.dfs['receitas_detalhadas'].empty:
            print("⚠ Dados de receitas detalhadas não disponíveis")
            return
        
        print("Criando série temporal completa...")
        
        df = self.dfs['receitas_detalhadas'].copy()
        df = df[df['publico_presente'] > 0]
        
        # Agregar por ano e mês
        temporal = df.groupby(['ano', 'competicao']).agg({
            'publico_presente': ['sum', 'mean'],
            'total_arrecadado': ['sum', 'mean'],
            'taxa_ocupacao_percent': 'mean',
            'ticket_medio_real_ingresso': 'mean',
            'gap_otimizacao': 'sum',
            'times_que_jogaram': 'count'
        }).reset_index()
        
        temporal.columns = ['ano', 'competicao', 'publico_total', 'publico_medio',
                           'receita_total', 'receita_media', 'taxa_ocupacao_media',
                           'ticket_medio', 'gap_otimizacao_total', 'quantidade_jogos']
        
        # Identificar tendências
        temporal['era'] = temporal['ano'].apply(
            lambda x: 'Pré-COVID' if x < 2020 else ('Pandemia' if x <= 2021 else 'Pós-COVID')
        )
        
        self.dfs['serie_temporal_completa'] = temporal
        print(f"✓ Série Temporal Completa criada com {len(temporal)} registros!\n")
    
    # ========================================================================
    
    def criar_fato_consolidado(self):
        """Cria tabela fato principal consolidando todas as informações"""
        
        print("Criando tabela fato consolidada...")
        
        # Verificar colunas disponíveis
        print(f"\nColunas disponíveis em jogo_fatos: {list(self.dfs['jogo_fatos'].columns)}")
        
        df = self.dfs['jogo_fatos'].copy()
        
        # Padronizar nomes de colunas
        col_map = {}
        for col in df.columns:
            col_lower = col.lower().strip()
            if 'jogo' in col_lower and 'id' in col_lower:
                col_map[col] = 'jogo_id'
            elif 'times' in col_lower or 'time' in col_lower:
                col_map[col] = 'times_jogados'
            elif col_lower == 'data':
                col_map[col] = 'data'
            elif 'publico' in col_lower and 'total' in col_lower:
                col_map[col] = 'publico_total'
            elif 'setor' in col_lower and 'visitado' in col_lower:
                col_map[col] = 'setor_mais_visitado'
            elif 'horario' in col_lower or 'horário' in col_lower:
                col_map[col] = 'horario'
        
        df.rename(columns=col_map, inplace=True)
        
        # Garantir colunas essenciais
        required_cols = ['jogo_id', 'times_jogados', 'data']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            print(f"⚠ Colunas obrigatórias ausentes: {missing_cols}")
            print("Não foi possível criar tabela fato consolidada")
            self.dfs['fato_consolidado'] = pd.DataFrame()
            return
        
        # Selecionar colunas disponíveis
        cols_to_use = ['jogo_id', 'times_jogados', 'data']
        optional_cols = ['publico_total', 'setor_mais_visitado', 'horario']
        
        for col in optional_cols:
            if col in df.columns:
                cols_to_use.append(col)
        
        fato = df[cols_to_use].copy()
        
        # Merge com Receitas
        if not self.dfs['receita'].empty and 'jogo_id' in self.dfs['receita'].columns:
            receita_cols = ['jogo_id', 'receita_ingresso', 'receita_produtos_internos', 
                           'total_arrecadado', 'classificacao_para_competicao']
            receita_cols_available = [col for col in receita_cols if col in self.dfs['receita'].columns]
            
            if len(receita_cols_available) > 1:
                fato = fato.merge(self.dfs['receita'][receita_cols_available], on='jogo_id', how='left')
                
                # CORREÇÃO: Garantir que as colunas são numéricas antes de calcular
                if 'receita_ingresso' in fato.columns and 'publico_total' in fato.columns:
                    # Converter para numérico, substituindo erros por NaN
                    fato['receita_ingresso'] = pd.to_numeric(fato['receita_ingresso'], errors='coerce')
                    fato['publico_total'] = pd.to_numeric(fato['publico_total'], errors='coerce')
                    
                    # Calcular ticket médio apenas onde ambos são válidos
                    fato['ticket_medio_ingresso'] = fato.apply(
                        lambda row: round(row['receita_ingresso'] / row['publico_total'], 2) 
                        if pd.notna(row['receita_ingresso']) and pd.notna(row['publico_total']) and row['publico_total'] > 0 
                        else None, 
                        axis=1
                    )
        
        # Adicionar informações de setores
        if not self.dfs['setor_fatos'].empty and 'jogo_id' in self.dfs['setor_fatos'].columns:
            setor_df = self.dfs['setor_fatos'].copy()
            
            for col in setor_df.columns:
                if 'jogo' in col.lower() and 'id' in col.lower():
                    setor_df.rename(columns={col: 'jogo_id'}, inplace=True)
                    break
            
            if 'jogo_id' in setor_df.columns:
                setor_df['jogo_id'] = setor_df['jogo_id'].str.strip()
                setor_pivot = setor_df.set_index('jogo_id')
                fato = fato.merge(setor_pivot, on='jogo_id', how='left')
        
        # Adicionar KPIs calculados
        if 'total_arrecadado' in fato.columns and 'publico_total' in fato.columns:
            # CORREÇÃO: Converter para numérico e tratar divisões
            fato['total_arrecadado'] = pd.to_numeric(fato['total_arrecadado'], errors='coerce')
            fato['publico_total'] = pd.to_numeric(fato['publico_total'], errors='coerce')
            
            fato['receita_per_capita'] = fato.apply(
                lambda row: round(row['total_arrecadado'] / row['publico_total'], 2)
                if pd.notna(row['total_arrecadado']) and pd.notna(row['publico_total']) and row['publico_total'] > 0
                else None,
                axis=1
            )
        
        if 'receita_produtos_internos' in fato.columns and 'total_arrecadado' in fato.columns:
            # CORREÇÃO: Converter e tratar divisões
            fato['receita_produtos_internos'] = pd.to_numeric(fato['receita_produtos_internos'], errors='coerce')
            
            fato['percentual_receita_produtos'] = fato.apply(
                lambda row: round(row['receita_produtos_internos'] / row['total_arrecadado'] * 100, 2)
                if pd.notna(row['receita_produtos_internos']) and pd.notna(row['total_arrecadado']) and row['total_arrecadado'] > 0
                else None,
                axis=1
            )
        
        if 'data' in fato.columns:
            fato['mes'] = fato['data'].dt.month
            fato['ano'] = fato['data'].dt.year
            fato['dia_semana'] = fato['data'].dt.day_name()
            fato['trimestre'] = fato['data'].dt.quarter
        
        # Classificar tipo de jogo
        if 'times_jogados' in fato.columns:
            fato['tipo_adversario'] = fato['times_jogados'].apply(self._classificar_adversario)
            fato['eh_classico'] = fato['times_jogados'].str.contains('Atlético-MG', case=False, na=False)
        
        self.dfs['fato_consolidado'] = fato
        print(f"✓ Tabela Fato Consolidada criada com {len(fato)} registros e {len(fato.columns)} colunas!\n")
    
    def criar_dimensao_produtos(self):
        """Cria dimensão de produtos com análise detalhada"""
        
        if self.dfs['lotacao'].empty:
            print("⚠ Dados de lotação não disponíveis, pulando dimensão de produtos")
            self.dfs['dim_produtos'] = pd.DataFrame()
            return
        
        print("Criando dimensão de produtos...")
        
        produtos = self.dfs['lotacao'].copy()
        
        # Verificar colunas disponíveis
        required_cols = ['jogo_id']
        if not all(col in produtos.columns or any(col.lower() in c.lower() for c in produtos.columns) for col in required_cols):
            print("⚠ Colunas necessárias não encontradas em lotacao")
            self.dfs['dim_produtos'] = pd.DataFrame()
            return
        
        # Padronizar nomes de colunas
        col_map = {}
        for col in produtos.columns:
            col_lower = col.lower().strip()
            if 'produto' in col_lower and 'tipico' in col_lower:
                col_map[col] = 'Produto_Tipico'
            elif 'preco' in col_lower and 'medio' in col_lower:
                col_map[col] = 'Preco_Medio'
            elif 'gasto' in col_lower and 'medio' in col_lower:
                col_map[col] = 'Gasto_Medio_por_Torcedor'
            elif 'receita' in col_lower and 'total' in col_lower:
                col_map[col] = 'Receita_Total_Produto'
        
        produtos.rename(columns=col_map, inplace=True)
        
        # Agregar por jogo e produto
        group_cols = ['jogo_id']
        if 'Produto_Tipico' in produtos.columns:
            group_cols.append('Produto_Tipico')
        
        agg_dict = {}
        if 'Preco_Medio' in produtos.columns:
            agg_dict['Preco_Medio'] = 'mean'
        if 'Gasto_Medio_por_Torcedor' in produtos.columns:
            agg_dict['Gasto_Medio_por_Torcedor'] = 'mean'
        if 'Receita_Total_Produto' in produtos.columns:
            agg_dict['Receita_Total_Produto'] = 'sum'
        
        if agg_dict:
            dim_produtos = produtos.groupby(group_cols).agg(agg_dict).reset_index()
            
            # Adicionar participação percentual
            if 'Receita_Total_Produto' in dim_produtos.columns:
                total_por_jogo = dim_produtos.groupby('jogo_id')['Receita_Total_Produto'].sum()
                dim_produtos = dim_produtos.merge(
                    total_por_jogo.rename('receita_total_jogo'),
                    left_on='jogo_id',
                    right_index=True
                )
                dim_produtos['participacao_percentual'] = (
                    dim_produtos['Receita_Total_Produto'] / 
                    dim_produtos['receita_total_jogo'] * 100
                ).round(2)
            
            self.dfs['dim_produtos'] = dim_produtos
            print(f"✓ Dimensão Produtos criada com {len(dim_produtos)} registros!\n")
        else:
            print("⚠ Colunas de agregação não encontradas")
            self.dfs['dim_produtos'] = pd.DataFrame()
    
    def criar_dimensao_demografica(self):
        """Cria dimensões demográficas agregadas"""
        
        if self.dfs['demografico'].empty:
            print("⚠ Dados demográficos não disponíveis, pulando")
            self.dfs['dim_demografica'] = pd.DataFrame()
            return
        
        print("Criando dimensão demográfica...")
        
        demo = self.dfs['demografico'].copy()
        
        # Verificar colunas necessárias
        required_cols = ['Jogo_ID', 'Tipo_Metrica', 'Categoria', 'Valor_Percentual']
        missing = [col for col in required_cols if col not in demo.columns]
        
        if missing:
            print(f"⚠ Colunas ausentes em demografico: {missing}")
            self.dfs['dim_demografica'] = pd.DataFrame()
            return
        
        try:
            # Limpar percentuais
            if demo['Valor_Percentual'].dtype == 'object':
                demo['Valor_Percentual'] = demo['Valor_Percentual'].str.rstrip('%').astype(float) / 100
            
            # Pivot por tipo de métrica
            genero = demo[demo['Tipo_Metrica'] == 'Gênero'].pivot_table(
                index='Jogo_ID',
                columns='Categoria',
                values='Valor_Percentual',
                aggfunc='first'
            ).add_prefix('perc_')
            
            faixa_etaria = demo[demo['Tipo_Metrica'] == 'Faixa Etária'].pivot_table(
                index='Jogo_ID',
                columns='Categoria',
                values='Valor_Percentual',
                aggfunc='first'
            ).add_prefix('perc_')
            
            regiao = demo[demo['Tipo_Metrica'] == 'Região'].pivot_table(
                index='Jogo_ID',
                columns='Categoria',
                values='Valor_Percentual',
                aggfunc='first'
            ).add_prefix('perc_')
            
            # Consolidar
            dim_demografica = genero.join(faixa_etaria, how='outer').join(regiao, how='outer').reset_index()
            dim_demografica.columns = dim_demografica.columns.str.replace(' ', '_')
            
            self.dfs['dim_demografica'] = dim_demografica
            print(f"✓ Dimensão Demográfica criada com {len(dim_demografica)} registros!\n")
        except Exception as e:
            print(f"⚠ Erro ao criar dimensão demográfica: {e}")
            self.dfs['dim_demografica'] = pd.DataFrame()
    
    def criar_analise_temporal(self):
        """Cria análise de séries temporais"""
        
        # Combinar dados recentes com históricos
        receitas_recentes = self.dfs['receita'][['data', 'receita_ingresso', 
                                                   'receita_produtos_internos', 'total_arrecadado']].copy()
        receitas_recentes['fonte'] = 'Dados Recentes (2024-2025)'
        
        receitas_hist = self.dfs['receitas_historicas'][['data', 'receita_ingresso', 
                                                          'receita_produtos_internos', 'total_arrecadado']].copy()
        receitas_hist['fonte'] = 'Dados Históricos (2014-2022)'
        
        analise_temporal = pd.concat([receitas_hist, receitas_recentes], ignore_index=True)
        analise_temporal['ano'] = analise_temporal['data'].dt.year
        analise_temporal['mes'] = analise_temporal['data'].dt.month
        analise_temporal['trimestre'] = analise_temporal['data'].dt.quarter
        
        # Métricas agregadas por ano
        metricas_anuais = analise_temporal.groupby('ano').agg({
            'receita_ingresso': ['sum', 'mean', 'std'],
            'receita_produtos_internos': ['sum', 'mean', 'std'],
            'total_arrecadado': ['sum', 'mean', 'std']
        }).round(2)
        
        metricas_anuais.columns = ['_'.join(col).strip() for col in metricas_anuais.columns.values]
        metricas_anuais = metricas_anuais.reset_index()
        
        self.dfs['analise_temporal'] = analise_temporal
        self.dfs['metricas_anuais'] = metricas_anuais
        print("✓ Análise Temporal criada!\n")
    
    def calcular_correlacoes(self):
        """Calcula correlações entre variáveis principais"""
        
        # Dataset para correlação
        fato = self.dfs['fato_consolidado'].copy()
        
        # Selecionar colunas numéricas
        colunas_numericas = [
            'publico_total', 'receita_ingresso', 'receita_produtos_internos',
            'total_arrecadado', 'ticket_medio_ingresso', 'receita_per_capita'
        ]
        
        # Adicionar colunas de setores se existirem
        for col in ['Vermelho', 'amarelo', 'roxo', 'laranja']:
            if col in fato.columns:
                colunas_numericas.append(col)
        
        # Filtrar apenas colunas que existem
        colunas_disponiveis = [col for col in colunas_numericas if col in fato.columns]
        
        if len(colunas_disponiveis) < 2:
            print("⚠ Colunas insuficientes para calcular correlações")
            return
        
        correlacao = fato[colunas_disponiveis].corr().round(3)
        
        self.correlations['matriz_correlacao'] = correlacao
        
        # Insights principais
        insights = []
        if 'publico_total' in correlacao.columns and 'total_arrecadado' in correlacao.columns:
            insights.append(f"Correlação Público x Receita Total: {correlacao.loc['publico_total', 'total_arrecadado']:.3f}")
        if 'ticket_medio_ingresso' in correlacao.columns and 'receita_ingresso' in correlacao.columns:
            insights.append(f"Correlação Ticket Médio x Receita: {correlacao.loc['ticket_medio_ingresso', 'receita_ingresso']:.3f}")
        
        self.correlations['insights'] = insights
        print("✓ Correlações calculadas!\n")
    
    def criar_kpis_dashboard(self):
        """Cria tabela de KPIs para dashboard"""
        
        fato = self.dfs['fato_consolidado']
        
        kpis_data = {
            'Métrica': [],
            'Valor': []
        }
        
        if 'publico_total' in fato.columns:
            kpis_data['Métrica'].append('Público Médio')
            kpis_data['Valor'].append(f"{fato['publico_total'].mean():,.0f}")
        
        if 'total_arrecadado' in fato.columns:
            kpis_data['Métrica'].append('Receita Média Total')
            kpis_data['Valor'].append(f"R$ {fato['total_arrecadado'].mean():,.2f}")
        
        if 'ticket_medio_ingresso' in fato.columns:
            kpis_data['Métrica'].append('Ticket Médio Ingresso')
            kpis_data['Valor'].append(f"R$ {fato['ticket_medio_ingresso'].mean():.2f}")
        
        if 'receita_per_capita' in fato.columns:
            kpis_data['Métrica'].append('Receita Per Capita')
            kpis_data['Valor'].append(f"R$ {fato['receita_per_capita'].mean():.2f}")
        
        if 'publico_total' in fato.columns:
            kpis_data['Métrica'].extend(['Maior Público', 'Menor Público', 'Total de Jogos'])
            kpis_data['Valor'].extend([
                f"{fato['publico_total'].max():,.0f}",
                f"{fato['publico_total'].min():,.0f}",
                f"{len(fato)}"
            ])
        
        kpis = pd.DataFrame(kpis_data)
        
        self.dfs['kpis_dashboard'] = kpis
        print("✓ KPIs para Dashboard criados!\n")
    
    def exportar_para_powerbi(self, pasta_saida='exports_powerbi'):
        """Exporta todos os datasets para Power BI"""
        
        import os
        os.makedirs(pasta_saida, exist_ok=True)
        
        # Lista de tabelas para exportar
        tabelas = {
            'FATO_Jogos': 'fato_consolidado',
            'DIM_Produtos': 'dim_produtos',
            'DIM_Demografica': 'dim_demografica',
            'FATO_Temporal': 'analise_temporal',
            'AGG_Metricas_Anuais': 'metricas_anuais',
            'KPI_Dashboard': 'kpis_dashboard',
            # ========== NOVO: Tabelas de receitas detalhadas ==========
            'FATO_Receitas_Detalhadas': 'receitas_detalhadas',
            'ANALISE_Precificacao': 'analise_precificacao',
            'ANALISE_Mix_Receitas': 'mix_receitas',
            'ANALISE_Ocupacao': 'analise_ocupacao',
            'SERIE_Temporal_Completa': 'serie_temporal_completa'
            # ==========================================================
        }
        
        arquivos_criados = []
        
        for nome_arquivo, nome_df in tabelas.items():
            if nome_df in self.dfs and not self.dfs[nome_df].empty:
                caminho = f"{pasta_saida}/{nome_arquivo}.csv"
                self.dfs[nome_df].to_csv(caminho, index=False, encoding='utf-8-sig')
                arquivos_criados.append(nome_arquivo)
                print(f"✓ Exportado: {nome_arquivo}.csv ({len(self.dfs[nome_df])} registros)")
        
        # Exportar matriz de correlação
        if 'matriz_correlacao' in self.correlations:
            caminho_corr = f"{pasta_saida}/CORR_Matriz.csv"
            self.correlations['matriz_correlacao'].to_csv(caminho_corr, encoding='utf-8-sig')
            arquivos_criados.append('CORR_Matriz')
            print(f"✓ Exportado: CORR_Matriz.csv")
        
        # Criar arquivo de documentação
        self._criar_documentacao(pasta_saida, arquivos_criados)
        
        print(f"\n{'='*60}")
        print(f"EXPORTAÇÃO CONCLUÍDA!")
        print(f"{'='*60}")
        print(f"Total de arquivos: {len(arquivos_criados)}")
        print(f"Localização: ./{pasta_saida}/")
    
    def _criar_documentacao(self, pasta, arquivos):
        """Cria documentação dos arquivos exportados"""
        
        doc = f"""
DOCUMENTAÇÃO - EXPORTS POWER BI - VERSÃO 2.0
{'='*70}
Data da Exportação: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}

NOVIDADES DA VERSÃO 2.0:
{'='*70}
✅ Adicionadas 5 novas tabelas com dados financeiros detalhados (2019-2025)
✅ 130 jogos analisados (vs 8 anteriores)
✅ Análise de precificação com gap de otimização
✅ Mix de receitas detalhado (ingressos, produtos, camarotes, estacionamento)
✅ Taxa de ocupação do estádio por competição
✅ Série temporal completa incluindo pandemia COVID-19

ARQUIVOS GERADOS:
{'='*70}

** TABELAS ORIGINAIS **

1. FATO_Jogos.csv
   - Tabela fato principal com informações consolidadas
   - Chave: jogo_id
   - Métricas: Público, Receitas, Setores, KPIs

2. DIM_Produtos.csv
   - Dimensão de produtos vendidos
   - Chave: jogo_id + Produto_Típico

3. DIM_Demografica.csv
   - Perfil demográfico da torcida
   - Chave: Jogo_ID

4. FATO_Temporal.csv
   - Série temporal de receitas
   - Dados: 2014-2025

5. AGG_Metricas_Anuais.csv
   - Agregações anuais

6. KPI_Dashboard.csv
   - KPIs principais formatados

7. CORR_Matriz.csv
   - Matriz de correlação

** NOVAS TABELAS (VERSÃO 2.0) **

8. FATO_Receitas_Detalhadas.csv ⭐ NOVO
   - 130 jogos de 2019 a 2025
   - 26 colunas com dados financeiros detalhados
   - Inclui: Inteiras/Meias, Preços, Camarotes, Estacionamento
   - Taxa de ocupação, Público mandante/visitante
   - Gap de otimização de receitas

9. ANALISE_Precificacao.csv ⭐ NOVO
   - Análise de preços de ingressos por competição/ano
   - Ticket médio real vs ideal
   - Fator de desconto sócios
   - Eficiência de precificação
   - Gap de otimização total

10. ANALISE_Mix_Receitas.csv ⭐ NOVO
    - Composição de receitas por fonte
    - % Ingressos, Produtos, Camarotes, Estacionamento
    - Receita per capita por categoria
    - Análise por competição e ano

11. ANALISE_Ocupacao.csv ⭐ NOVO
    - Taxa de ocupação do estádio
    - Público presente vs pagante
    - % não-pagantes
    - Análise por tipo de adversário

12. SERIE_Temporal_Completa.csv ⭐ NOVO
    - Série histórica completa 2019-2025
    - Identificação de eras (Pré-COVID, Pandemia, Pós-COVID)
    - Tendências de público e receita
    - Quantidade de jogos por período

{'='*70}
RELACIONAMENTOS NO POWER BI:
{'='*70}

** Relacionamentos Originais **
FATO_Jogos[jogo_id] --> DIM_Produtos[jogo_id]
FATO_Jogos[jogo_id] --> DIM_Demografica[Jogo_ID]
FATO_Jogos[data] --> FATO_Temporal[data]

** Novos Relacionamentos **
FATO_Receitas_Detalhadas[ano] --> ANALISE_Precificacao[ano]
FATO_Receitas_Detalhadas[ano] --> ANALISE_Mix_Receitas[ano]
FATO_Receitas_Detalhadas[ano] --> ANALISE_Ocupacao[ano]
FATO_Receitas_Detalhadas[ano] --> SERIE_Temporal_Completa[ano]

** Relacionamento Cruzado **
FATO_Jogos[ano] --> FATO_Receitas_Detalhadas[ano] (para análises combinadas)

{'='*70}
NOVAS MEDIDAS DAX SUGERIDAS:
{'='*70}

// Análise de Precificação
Gap Otimização Total = 
SUM(FATO_Receitas_Detalhadas[gap_otimizacao])

Eficiência Precificação = 
DIVIDE(
    SUM(FATO_Receitas_Detalhadas[receita_ingresso]),
    SUM(FATO_Receitas_Detalhadas[receita_bruta_ideal_ingressos]),
    0
) * 100

Desconto Médio Sócios = 
AVERAGE(FATO_Receitas_Detalhadas[fator_desconto_socios_percent])

// Mix de Receitas
% Receita Camarotes = 
DIVIDE(
    SUM(FATO_Receitas_Detalhadas[receita_camarotes]),
    SUM(FATO_Receitas_Detalhadas[total_arrecadado]),
    0
) * 100

% Receita Estacionamento = 
DIVIDE(
    SUM(FATO_Receitas_Detalhadas[receita_estacionamento]),
    SUM(FATO_Receitas_Detalhadas[total_arrecadado]),
    0
) * 100

Receita Per Capita Produtos = 
DIVIDE(
    SUM(FATO_Receitas_Detalhadas[receita_produtos_internos]),
    SUM(FATO_Receitas_Detalhadas[publico_presente]),
    0
)

// Ocupação
Taxa Ocupação Média = 
AVERAGE(FATO_Receitas_Detalhadas[taxa_ocupacao_percent])

% Não Pagantes = 
DIVIDE(
    SUM(FATO_Receitas_Detalhadas[publico_presente]) - 
    SUM(FATO_Receitas_Detalhadas[publico_pagante]),
    SUM(FATO_Receitas_Detalhadas[publico_presente]),
    0
) * 100

Público Médio Clássicos = 
CALCULATE(
    AVERAGE(FATO_Receitas_Detalhadas[publico_presente]),
    FATO_Receitas_Detalhadas[eh_classico] = TRUE
)

// Análise Temporal
Crescimento Público YoY = 
VAR PublicoAnoAtual = SUM(FATO_Receitas_Detalhadas[publico_presente])
VAR PublicoAnoAnterior = 
    CALCULATE(
        SUM(FATO_Receitas_Detalhadas[publico_presente]),
        SAMEPERIODLASTYEAR(FATO_Receitas_Detalhadas[ano])
    )
RETURN
DIVIDE(PublicoAnoAtual - PublicoAnoAnterior, PublicoAnoAnterior, 0) * 100

Impacto COVID = 
CALCULATE(
    SUM(FATO_Receitas_Detalhadas[publico_presente]),
    FATO_Receitas_Detalhadas[era] = "Pandemia"
)

{'='*70}
NOVAS VISUALIZAÇÕES SUGERIDAS:
{'='*70}

** Página 5: Análise Financeira Avançada **

1. Funil de Otimização
   - Receita Bruta Ideal → Descontos → Receita Real
   - Visual: Gráfico de Funil

2. Mix de Receitas (Waterfall Chart)
   - Ingressos + Produtos + Camarotes + Estacionamento = Total
   - Visual: Gráfico de Cascata

3. Taxa de Ocupação - Linha do Tempo
   - 2019-2025 mostrando impacto COVID
   - Visual: Gráfico de Linha com marcadores

4. Scatter: Ocupação x Receita
   - Identificar oportunidades de otimização
   - Visual: Gráfico de Dispersão

5. Heatmap: Público por Competição
   - Linhas: Competições
   - Colunas: Anos
   - Cores: Intensidade de público

6. Comparativo Pré/Pós COVID
   - Cards comparativos
   - Visual: Cards + Gráficos de Barras

{'='*70}
DICAS DE USO:
{'='*70}

1. Use filtros de Era (Pré-COVID, Pandemia, Pós-COVID) para análises temporais

2. Combine tipo_adversario com taxa_ocupacao para estratégias de pricing

3. Analise gap_otimizacao por competição para identificar oportunidades

4. Compare % não-pagantes entre competições para avaliar políticas de cortesia

5. Use eficiência_precificacao para benchmarking entre períodos

6. Analise mix de receitas para diversificação de fontes

{'='*70}
"""
        
        caminho_doc = f"{pasta}/README_POWERBI_V2.txt"
        with open(caminho_doc, 'w', encoding='utf-8') as f:
            f.write(doc)
        
        print(f"✓ Documentação criada: README_POWERBI_V2.txt")
    
    def executar_pipeline_completo(self):
        """Executa todo o pipeline de processamento e exportação"""
        
        print("\n" + "="*60)
        print("INICIANDO PROCESSAMENTO DE DADOS - CRUZEIRO EC v2.0")
        print("="*60 + "\n")
        
        self.carregar_dados()
        self.criar_fato_consolidado()
        self.criar_dimensao_produtos()
        self.criar_dimensao_demografica()
        self.criar_analise_temporal()
        
        # ========== NOVO: Análises de receitas detalhadas ==========
        if not self.dfs['receitas_detalhadas'].empty:
            self.criar_analise_precificacao()
            self.criar_mix_receitas()
            self.criar_analise_ocupacao()
            self.criar_serie_temporal_completa()
        # ===========================================================
        
        self.calcular_correlacoes()
        self.criar_kpis_dashboard()
        
        print("\n" + "="*60)
        print("EXPORTANDO PARA POWER BI")
        print("="*60 + "\n")
        
        self.exportar_para_powerbi()
        
        # Mostrar insights de correlação
        if self.correlations.get('insights'):
            print("\n" + "="*60)
            print("INSIGHTS DE CORRELAÇÃO")
            print("="*60)
            for insight in self.correlations['insights']:
                print(f"  • {insight}")
            print()
        
        # ========== NOVO: Mostrar resumo das novas análises ==========
        if not self.dfs['receitas_detalhadas'].empty:
            print("\n" + "="*60)
            print("RESUMO DAS NOVAS ANÁLISES")
            print("="*60)
            
            df = self.dfs['receitas_detalhadas']
            df_validos = df[df['publico_presente'] > 0]
            
            print(f"  • Total de jogos analisados: {len(df_validos)}")
            print(f"  • Período: {df_validos['ano'].min()} a {df_validos['ano'].max()}")
            print(f"  • Público total acumulado: {df_validos['publico_presente'].sum():,.0f}")
            print(f"  • Receita total acumulada: R$ {df_validos['total_arrecadado'].sum():,.2f}")
            print(f"  • Taxa de ocupação média: {df_validos['taxa_ocupacao_percent'].mean():.1f}%")
            print(f"  • Gap de otimização total: R$ {df_validos['gap_otimizacao'].sum():,.2f}")
            print(f"  • Competições analisadas: {df_validos['competicao'].nunique()}")
            print()
        # =============================================================


# EXECUÇÃO
if __name__ == "__main__":
    exporter = CruzeiroPowerBIExporter(caminho_dados='data/data.csv')
    exporter.executar_pipeline_completo()