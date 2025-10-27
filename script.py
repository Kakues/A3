import pandas as pd
import numpy as np
from datetime import datetime
import warnings
import os
import glob
warnings.filterwarnings('ignore')

class CruzeiroPowerBIExporter:
    """Sistema de análise e exportação de dados do Cruzeiro para Power BI"""
    
    def __init__(self):
        self.dfs = {}
        self.correlations = {}
        self._verificar_arquivos()
        
    def _verificar_arquivos(self):
        """Verifica e lista todos os arquivos CSV disponíveis"""
        print("\n" + "="*60)
        print("VERIFICANDO ARQUIVOS CSV")
        print("="*60)
        
        # Tentar encontrar CSVs na pasta atual
        csv_files = glob.glob("*.csv")
        
        # Se não encontrar, procurar na subpasta data.csv
        if len(csv_files) <= 1:  # Só tem data.csv ou nenhum
            print("  Procurando em subpastas...")
            csv_files = glob.glob("data.csv/*.csv")
            if not csv_files:
                csv_files = glob.glob("data.csv/**/*.csv", recursive=True)
        
        if not csv_files:
            print("❌ ERRO: Nenhum arquivo CSV encontrado!")
            print(f"   Pasta atual: {os.getcwd()}")
            print("\n💡 SOLUÇÃO: Verifique se os arquivos estão na pasta correta.")
            raise FileNotFoundError("Nenhum arquivo CSV encontrado")
        
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
            
            # Mapear cada tipo de arquivo
            if 'setor_fatos' in nome_limpo or 'setor_fato' in nome_limpo:
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
        
        # 1. Setor Fatos
        if 'setor_fatos' in self.arquivos:
            self.dfs['setor_fatos'] = pd.read_csv(self.arquivos['setor_fatos'], skipinitialspace=True)
            self.dfs['setor_fatos'].columns = self.dfs['setor_fatos'].columns.str.strip()
            print(f"  ✓ {self.arquivos['setor_fatos']} carregado")
        else:
            print("  ⚠ Arquivo setor_fatos não encontrado, usando dados parciais")
            self.dfs['setor_fatos'] = pd.DataFrame()
        
        # 2. Jogo Fatos (Principal)
        if 'jogo_fatos' in self.arquivos:
            self.dfs['jogo_fatos'] = pd.read_csv(self.arquivos['jogo_fatos'])
            self.dfs['jogo_fatos']['data'] = pd.to_datetime(self.dfs['jogo_fatos']['data'], format='%d/%m/%Y', errors='coerce')
            
            # Remove espaços em branco do início e fim de todos os nomes de colunas
            self.dfs['jogo_fatos'].columns = self.dfs['jogo_fatos'].columns.str.strip()

            # Extrair público total (lidar com texto "pagantes")
            if 'publico total' in self.dfs['jogo_fatos'].columns:
                self.dfs['jogo_fatos']['publico_total'] = self.dfs['jogo_fatos']['publico total'].astype(str).str.extract('(\d+)', expand=False).astype(float)
            elif 'publico_total' in self.dfs['jogo_fatos'].columns:
                self.dfs['jogo_fatos']['publico_total'] = self.dfs['jogo_fatos']['publico_total'].astype(str).str.extract('(\d+)', expand=False).astype(float)
            
            # Padronizar coluna jogo_id
            if 'jogo id' in self.dfs['jogo_fatos'].columns:
                self.dfs['jogo_fatos']['jogo_id'] = self.dfs['jogo_fatos']['jogo id'].str.strip()
            elif 'jogo_id' in self.dfs['jogo_fatos'].columns:
                self.dfs['jogo_fatos']['jogo_id'] = self.dfs['jogo_fatos']['jogo_id'].str.strip()
            
            print(f"  ✓ {self.arquivos['jogo_fatos']} carregado")
        else:
            raise FileNotFoundError("Arquivo jogo_fatos.csv é obrigatório!")
        
        # 3. Lotação por Jogo (Produtos)
        if 'lotacao' in self.arquivos:
            self.dfs['lotacao'] = pd.read_csv(self.arquivos['lotacao'])
            self.dfs['lotacao']['jogo_id'] = self.dfs['lotacao']['jogo_id'].str.strip()
            print(f"  ✓ {self.arquivos['lotacao']} carregado")
        else:
            print("  ⚠ Arquivo lotacao não encontrado")
            self.dfs['lotacao'] = pd.DataFrame()
        
        # 4. Perfil Demográfico
        if 'demografico' in self.arquivos:
            self.dfs['demografico'] = pd.read_csv(self.arquivos['demografico'])
            self.dfs['demografico']['Jogo_ID'] = self.dfs['demografico']['Jogo_ID'].str.strip()
            print(f"  ✓ {self.arquivos['demografico']} carregado")
        else:
            print("  ⚠ Arquivo demográfico não encontrado")
            self.dfs['demografico'] = pd.DataFrame()
        
        # 5. Receita Fatos
        if 'receita' in self.arquivos:
            self.dfs['receita'] = pd.read_csv(self.arquivos['receita'])
            self.dfs['receita']['data'] = pd.to_datetime(self.dfs['receita']['data'], format='%d/%m/%Y', errors='coerce')
            self.dfs['receita']['jogo_id'] = self.dfs['receita']['jogo_id'].str.strip()
            print(f"  ✓ {self.arquivos['receita']} carregado")
        else:
            print("  ⚠ Arquivo receita_fatos não encontrado")
            self.dfs['receita'] = pd.DataFrame()
        
        # 6. Receitas Históricas (2014-2022)
        if 'receitas_historicas' in self.arquivos:
            self.dfs['receitas_historicas'] = pd.read_csv(self.arquivos['receitas_historicas'])
            self.dfs['receitas_historicas']['data'] = pd.to_datetime(self.dfs['receitas_historicas']['data'], errors='coerce')
            self.dfs['receitas_historicas']['Ano'] = self.dfs['receitas_historicas']['Ano'].astype(int)
            print(f"  ✓ {self.arquivos['receitas_historicas']} carregado")
        else:
            print("  ⚠ Arquivo receitas_historicas não encontrado")
            self.dfs['receitas_historicas'] = pd.DataFrame()
        
        # 7. Sócio Torcedor
        if 'socio_torcedor' in self.arquivos:
            self.dfs['socio_torcedor'] = pd.read_csv(self.arquivos['socio_torcedor'])
            print(f"  ✓ {os.path.basename(self.arquivos['socio_torcedor'])} carregado")
        else:
            print("  ⚠ Arquivo socio_torcedor não encontrado")
            self.dfs['socio_torcedor'] = pd.DataFrame()
        
        # 8. Ticket Médio Estimativa
        if 'ticket_medio_estimativa' in self.arquivos:
            try:
                self.dfs['ticket_medio_estimativa'] = pd.read_csv(self.arquivos['ticket_medio_estimativa'])
                print(f"  ✓ {os.path.basename(self.arquivos['ticket_medio_estimativa'])} carregado")
            except Exception as e:
                print(f"  ⚠ Erro ao carregar ticket_medio_estimativa: {e}")
        
        # 9. Ticket Médio Torcedor
        if 'ticket_medio_torcedor' in self.arquivos:
            try:
                self.dfs['ticket_medio_torcedor'] = pd.read_csv(self.arquivos['ticket_medio_torcedor'])
                print(f"  ✓ {os.path.basename(self.arquivos['ticket_medio_torcedor'])} carregado")
            except Exception as e:
                print(f"  ⚠ Erro ao carregar ticket_medio_torcedor: {e}")
        
        # 10. Vendas por Canal
        if 'vendas_canal' in self.arquivos:
            try:
                self.dfs['vendas_canal'] = pd.read_csv(self.arquivos['vendas_canal'])
                print(f"  ✓ {os.path.basename(self.arquivos['vendas_canal'])} carregado")
            except Exception as e:
                print(f"  ⚠ Erro ao carregar vendas_canal: {e}")
        
        # 11. Vendas por Competição
        if 'vendas_competicao' in self.arquivos:
            try:
                self.dfs['vendas_competicao'] = pd.read_csv(self.arquivos['vendas_competicao'])
                print(f"  ✓ {os.path.basename(self.arquivos['vendas_competicao'])} carregado")
            except Exception as e:
                print(f"  ⚠ Erro ao carregar vendas_competicao: {e}")
        
        # 12. Preços Produtos
        if 'precos_produtos' in self.arquivos:
            try:
                self.dfs['precos_produtos'] = pd.read_csv(self.arquivos['precos_produtos'])
                print(f"  ✓ {os.path.basename(self.arquivos['precos_produtos'])} carregado")
            except Exception as e:
                print(f"  ⚠ Erro ao carregar precos_produtos: {e}")
        
        # 13. Público Cruzeiro (com tratamento especial para erros de formatação)
        if 'publico_cruzeiro' in self.arquivos:
            try:
                # Tentar com on_bad_lines='skip' para ignorar linhas problemáticas
                self.dfs['publico_cruzeiro'] = pd.read_csv(
                    self.arquivos['publico_cruzeiro'], 
                    on_bad_lines='skip',
                    engine='python'
                )
                print(f"  ✓ {os.path.basename(self.arquivos['publico_cruzeiro'])} carregado (algumas linhas podem ter sido ignoradas)")
            except Exception as e:
                print(f"  ⚠ Erro ao carregar publico_cruzeiro: {e}")
                print(f"     Tentando método alternativo...")
                try:
                    # Método alternativo: ler todas as colunas
                    self.dfs['publico_cruzeiro'] = pd.read_csv(
                        self.arquivos['publico_cruzeiro'],
                        sep=',',
                        quoting=1,  # QUOTE_ALL
                        on_bad_lines='warn'
                    )
                    print(f"  ✓ {os.path.basename(self.arquivos['publico_cruzeiro'])} carregado com método alternativo")
                except Exception as e2:
                    print(f"  ✗ Não foi possível carregar publico_cruzeiro: {e2}")
        
        # 14. Setor por Jogo (adicional)
        if 'setor_por_jogo' in self.arquivos:
            try:
                self.dfs['setor_por_jogo'] = pd.read_csv(self.arquivos['setor_por_jogo'])
                print(f"  ✓ {os.path.basename(self.arquivos['setor_por_jogo'])} carregado")
            except Exception as e:
                print(f"  ⚠ Erro ao carregar setor_por_jogo: {e}")
        
        print(f"\n✓ Processo de carga concluído! Total: {len(self.dfs)} datasets carregados\n")
        
    def criar_fato_consolidado(self):
        """Cria tabela fato principal consolidando todas as informações"""
        
        print("Criando tabela fato consolidada...")
        
        # Verificar colunas disponíveis
        print(f"\nColunas disponíveis em jogo_fatos: {list(self.dfs['jogo_fatos'].columns)}")
        
        # Mapear nomes de colunas (lidar com variações)
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
        
        # Garantir que temos as colunas essenciais
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
        
        # Merge com Receitas se disponível
        if not self.dfs['receita'].empty and 'jogo_id' in self.dfs['receita'].columns:
            receita_cols = ['jogo_id', 'receita_ingresso', 'receita_produtos_internos', 
                           'total_arrecadado', 'classificacao_para_competicao']
            
            # Verificar quais colunas existem
            receita_cols_available = [col for col in receita_cols if col in self.dfs['receita'].columns]
            
            if len(receita_cols_available) > 1:  # Pelo menos jogo_id + 1 coluna
                fato = fato.merge(
                    self.dfs['receita'][receita_cols_available],
                    on='jogo_id', how='left'
                )
                

                # Calcular ticket médio se possível
                if 'receita_ingresso' in fato.columns and 'publico_total' in fato.columns:
                    fato['ticket_medio_ingresso'] = (fato['receita_ingresso'] / fato['publico_total']).round(2)
        
        # Adicionar informações de setores se disponível
        if not self.dfs['setor_fatos'].empty and 'jogo_id' in self.dfs['setor_fatos'].columns:
            setor_df = self.dfs['setor_fatos'].copy()
            
            # Padronizar nome da coluna jogo_id
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
            fato['receita_per_capita'] = (fato['total_arrecadado'] / fato['publico_total']).round(2)
        
        if 'receita_produtos_internos' in fato.columns and 'total_arrecadado' in fato.columns:
            fato['percentual_receita_produtos'] = (fato['receita_produtos_internos'] / fato['total_arrecadado'] * 100).round(2)
        
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
        
    def _classificar_adversario(self, times):
        """Classifica o adversário por importância"""
        grandes = ['Flamengo', 'Palmeiras', 'São Paulo', 'Corinthians', 'Atlético-MG']
        if any(grande in times for grande in grandes):
            return 'Grande'
        return 'Médio/Pequeno'
    
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
            
            # Adicionar participação percentual se possível
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
        
        # Verificar se temos as colunas necessárias
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
        print("✓ Análise Temporal criada!")
        
    def calcular_correlacoes(self):
        """Calcula correlações entre variáveis principais"""
        
        # Dataset para correlação
        fato = self.dfs['fato_consolidado'].copy()
        
        # Selecionar colunas numéricas
        colunas_numericas = [
            'publico_total', 'receita_ingresso', 'receita_produtos_internos',
            'total_arrecadado', 'ticket_medio_ingresso', 'receita_per_capita',
            'Vermelho', 'amarelo', 'roxo', 'laranja'
        ]
        
        correlacao = fato[colunas_numericas].corr().round(3)
        
        self.correlations['matriz_correlacao'] = correlacao
        
        # Insights principais
        insights = []
        insights.append(f"Correlação Público x Receita Total: {correlacao.loc['publico_total', 'total_arrecadado']:.3f}")
        insights.append(f"Correlação Ticket Médio x Receita: {correlacao.loc['ticket_medio_ingresso', 'receita_ingresso']:.3f}")
        insights.append(f"Correlação Setor Amarelo x Público: {correlacao.loc['amarelo', 'publico_total']:.3f}")
        
        self.correlations['insights'] = insights
        print("✓ Correlações calculadas!")
        
    def criar_kpis_dashboard(self):
        """Cria tabela de KPIs para dashboard"""
        
        fato = self.dfs['fato_consolidado']
        
        kpis = pd.DataFrame({
            'Métrica': [
                'Público Médio',
                'Receita Média Total',
                'Ticket Médio Ingresso',
                'Receita Per Capita',
                'Percentual Receita Produtos',
                'Maior Público',
                'Menor Público',
                'Total de Jogos'
            ],
            'Valor': [
                f"{fato['publico_total'].mean():,.0f}",
                f"R$ {fato['total_arrecadado'].mean():,.2f}",
                f"R$ {fato['ticket_medio_ingresso'].mean():.2f}",
                f"R$ {fato['receita_per_capita'].mean():.2f}",
                f"{fato['percentual_receita_produtos'].mean():.1f}%",
                f"{fato['publico_total'].max():,.0f}",
                f"{fato['publico_total'].min():,.0f}",
                f"{len(fato)}"
            ]
        })
        
        self.dfs['kpis_dashboard'] = kpis
        print("✓ KPIs para Dashboard criados!")
        
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
            'KPI_Dashboard': 'kpis_dashboard'
        }
        
        arquivos_criados = []
        
        for nome_arquivo, nome_df in tabelas.items():
            if nome_df in self.dfs:
                caminho = f"{pasta_saida}/{nome_arquivo}.csv"
                self.dfs[nome_df].to_csv(caminho, index=False, encoding='utf-8-sig')
                arquivos_criados.append(nome_arquivo)
                print(f"✓ Exportado: {nome_arquivo}.csv")
        
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
DOCUMENTAÇÃO - EXPORTS POWER BI
{'='*70}
Data da Exportação: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}

ARQUIVOS GERADOS:
{'='*70}

1. FATO_Jogos.csv
   - Tabela fato principal com informações consolidadas de cada jogo
   - Chave: jogo_id
   - Relacionamentos: Todas as outras tabelas
   - Métricas: Público, Receitas, Setores, KPIs calculados

2. DIM_Produtos.csv
   - Dimensão de produtos vendidos por jogo
   - Chave: jogo_id + Produto_Típico
   - Métricas: Receitas por produto, participação percentual

3. DIM_Demografica.csv
   - Perfil demográfico da torcida (Gênero, Idade, Região)
   - Chave: Jogo_ID
   - Dados percentuais por categoria

4. FATO_Temporal.csv
   - Série temporal de receitas (2014-2025)
   - Análise evolutiva com dados históricos e recentes

5. AGG_Metricas_Anuais.csv
   - Agregações anuais (soma, média, desvio padrão)
   - Visão macro da evolução ano a ano

6. KPI_Dashboard.csv
   - KPIs principais para cards do dashboard
   - Métricas resumidas e formatadas

7. CORR_Matriz.csv
   - Matriz de correlação entre variáveis numéricas
   - Análise de relacionamentos estatísticos

{'='*70}
SUGESTÕES DE RELACIONAMENTOS NO POWER BI:
{'='*70}

FATO_Jogos [jogo_id] --> DIM_Produtos [jogo_id]
FATO_Jogos [jogo_id] --> DIM_Demografica [Jogo_ID]
FATO_Jogos [data] --> FATO_Temporal [data]

{'='*70}
MEDIDAS DAX SUGERIDAS:
{'='*70}

Receita Total = SUM(FATO_Jogos[total_arrecadado])
Público Total = SUM(FATO_Jogos[publico_total])
Ticket Médio = AVERAGE(FATO_Jogos[ticket_medio_ingresso])
Taxa de Ocupação = [Público Total] / 61927
Crescimento YoY = ([Receita Total] - CALCULATE([Receita Total], SAMEPERIODLASTYEAR(FATO_Temporal[data]))) / CALCULATE([Receita Total], SAMEPERIODLASTYEAR(FATO_Temporal[data]))

{'='*70}
"""
        
        caminho_doc = f"{pasta}/README_POWERBI.txt"
        with open(caminho_doc, 'w', encoding='utf-8') as f:
            f.write(doc)
        
        print(f"✓ Documentação criada: README_POWERBI.txt")
        
    def executar_pipeline_completo(self):
        """Executa todo o pipeline de processamento e exportação"""
        
        print("\n" + "="*60)
        print("INICIANDO PROCESSAMENTO DE DADOS - CRUZEIRO EC")
        print("="*60 + "\n")
        
        self.carregar_dados()
        self.criar_fato_consolidado()
        self.criar_dimensao_produtos()
        self.criar_dimensao_demografica()
        self.criar_analise_temporal()
        self.calcular_correlacoes()
        self.criar_kpis_dashboard()
        
        print("\n" + "="*60)
        print("EXPORTANDO PARA POWER BI")
        print("="*60 + "\n")
        
        self.exportar_para_powerbi()
        
        # Mostrar insights de correlação
        print("\n" + "="*60)
        print("INSIGHTS DE CORRELAÇÃO")
        print("="*60)
        for insight in self.correlations['insights']:
            print(f"  • {insight}")
        print()


# EXECUÇÃO
if __name__ == "__main__":
    exporter = CruzeiroPowerBIExporter()
    exporter.executar_pipeline_completo()