# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os
from datetime import timedelta

# Define o diretório de saída
OUTPUT_DIR = "mineirao_2024_2025_star_schema"
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

print(f"Iniciando a geração do Star Schema para a temporada 2024-2025 no diretório: '{OUTPUT_DIR}'...\n")

# Configuração de Seed para reprodutibilidade
np.random.seed(42)

# ==============================================================================
# 1. Definição das Dimensões (Contexto Estático)
# ==============================================================================

# DIM_ADVERSARIO (Dimensão Jogo e Competição) - Base para Projeção (Tema 9)
DIM_ADVERSARIO = pd.DataFrame({
    'adversario_id': range(1, 10),
    'nome_adversario': ['Atlético-MG', 'Palmeiras', 'Flamengo', 'São Paulo', 'Athletico-PR', 'Grêmio', 'Ceará', 'Tombense', 'CRB'],
    'competicao': ['Brasileiro', 'Brasileiro', 'Copa do Brasil', 'Libertadores', 'Brasileiro', 'Brasileiro', 'Copa do Brasil', 'Mineiro', 'Copa do Brasil'],
    'nivel_confronto': ['Classico', 'Grande', 'Grande', 'Grande', 'Medio', 'Medio', 'Medio', 'Pequeno', 'Pequeno'],
    'classico_local': [True, False, False, False, False, False, False, False, False]
})

# DIM_SETOR (Dimensão Localização/Mapa de Calor) - Tema 2, 5, 10
DIM_SETOR = pd.DataFrame({
    'setor_id': [1, 2, 3, 4, 5, 6, 7],
    'nome_setor': ['Amarelo Inferior', 'Vermelho Inferior', 'Roxo Superior', 'Laranja Superior', 'Roxo Inferior', 'Camarotes', 'Visitante'],
    'capacidade_mil': [12, 18, 15, 10, 5, 1.5, 2.5], # Capacidade simulada em milhares
    'tipo_acesso': ['Portão 1-3', 'Portão 4-6', 'Portão 7-9', 'Portão 10-12', 'Portão 13', 'Portão 14', 'Portão 15']
})

# DIM_PRODUTO (Dimensão Consumo) - Tema 1
DIM_PRODUTO = pd.DataFrame({
    'produto_id': [1, 2, 3, 4, 5, 6],
    'item_vendido': ['Cerveja (350ml)', 'Refrigerante', 'Pipoca/Salgado', 'Hot Dog', 'Água Mineral', 'Camisa Oficial (Loja)'],
    'categoria': ['Bebida Alcoolica', 'Bebida Nao-Alcoolica', 'Comida', 'Comida', 'Bebida Nao-Alcoolica', 'Merchandising'],
    'preco_medio_rs': [15.00, 10.00, 25.00, 30.00, 8.00, 350.00]
})

# DIM_PERFIL_TORCEDOR (Dimensão Demográfica) - Tema 4
DIM_PERFIL_TORCEDOR = pd.DataFrame({
    'perfil_id': [1, 2, 3, 4, 5],
    'faixa_etaria': ['18-24 anos', '25-34 anos', '35-44 anos', '45-59 anos', '60+ anos'],
    'genero': ['Masculino', 'Feminino', 'Masculino', 'Feminino', 'Masculino'],
    'regiao_origem': ['Capital - BH', 'Interior - MG', 'Outros Estados', 'Capital - BH', 'Interior - MG']
})

# ==============================================================================
# 2. Geração da Tabela FATO_JOGOS e DIM_DATA
# ==============================================================================

# Definição do Período e Jogos
START_DATE = pd.to_datetime('2024-03-01')
END_DATE = pd.to_datetime('2025-11-30')
NUM_JOGOS = 25 # Simulação de 25 jogos no Mineirão no período

# 2.1. Criação dos IDs de Jogo e Datas
dates = pd.to_datetime(np.random.uniform(START_DATE.value, END_DATE.value, NUM_JOGOS).astype(np.int64))
dates = np.sort(dates)

df_base = pd.DataFrame({
    'jogo_id': range(1, NUM_JOGOS + 1),
    'data': dates,
    'adversario_id': np.random.choice(DIM_ADVERSARIO['adversario_id'], NUM_JOGOS)
})

# 2.2. Geração de Fatos com Veracidade
def gerar_fato_jogo(row):
    # Merge com dados do Adversário
    adv = DIM_ADVERSARIO[DIM_ADVERSARIO['adversario_id'] == row['adversario_id']].iloc[0]
    
    is_classico = adv['classico_local']
    is_grande = adv['nivel_confronto'] in ['Classico', 'Grande']
    is_fim_de_semana = row['data'].dayofweek in [5, 6] # Sabado ou Domingo

    # Lógica de Público e Receita (Tema 6: Lotação)
    # Classicos/Grandes e Finais de Semana tem público maior
    publico_medio = 45000 if is_classico else (35000 if is_grande else 20000)
    publico_medio = publico_medio * 1.1 if is_fim_de_semana else publico_medio
    
    publico_pago = np.random.normal(publico_medio, publico_medio * 0.1)
    publico_pago = max(10000, min(61846, int(publico_pago))) # Mineirão capacidade max

    # Receita (Mais sensível ao público e ticket)
    ticket_medio_base = np.random.uniform(70, 150)
    if is_classico: ticket_medio_base *= 1.5
    if adv['competicao'] == 'Libertadores': ticket_medio_base *= 1.2
    
    receita_ingresso_rs = publico_pago * ticket_medio_base / 1000.0
    
    # Simula consumo (Tema 1: Consumo Médio)
    ticket_medio_consumo_rs = np.random.normal(55, 10)
    
    return {
        'publico_pago': publico_pago,
        'receita_ingresso_mil_rs': round(receita_ingresso_rs, 3),
        'ticket_medio_ingresso_rs': round(receita_ingresso_rs * 1000 / publico_pago, 2),
        'ticket_medio_consumo_rs': round(ticket_medio_consumo_rs, 2), # Usado em FATO_CONSUMO
        'taxa_ocupacao': round(publico_pago / 61846 * 100, 2),
        'horario_jogo': np.random.choice(['21:30', '19:00', '16:00'])
    }

fato_data = df_base.apply(gerar_fato_jogo, axis=1, result_type='expand')
DF_FATO_JOGOS = pd.concat([df_base, fato_data], axis=1)
DF_FATO_JOGOS = DF_FATO_JOGOS.sort_values(by='data').reset_index(drop=True)

# 2.3. Criação da DIM_DATA
DF_DIM_DATA = DF_FATO_JOGOS[['data']].drop_duplicates().reset_index(drop=True)
DF_DIM_DATA.index.name = 'data_id'
DF_DIM_DATA = DF_DIM_DATA.reset_index()
DF_DIM_DATA['data_id'] = DF_DIM_DATA['data_id'] + 1

DF_DIM_DATA['ano'] = DF_DIM_DATA['data'].dt.year
DF_DIM_DATA['mes'] = DF_DIM_DATA['data'].dt.month
DF_DIM_DATA['dia_semana'] = DF_DIM_DATA['data'].dt.day_name(locale='pt_BR')
DF_DIM_DATA['feriado'] = DF_DIM_DATA['dia_semana'].apply(lambda x: True if x in ['Domingo'] else False) # Simula Feriado/Final de Semana

# 2.4. Finalização da FATO_JOGOS (Adiciona data_id e chaves)
DF_FATO_JOGOS = pd.merge(DF_FATO_JOGOS, DF_DIM_DATA[['data', 'data_id']], on='data', how='left')
DF_FATO_JOGOS.drop(columns=['data', 'adversario_id'], inplace=True)
DF_FATO_JOGOS.rename(columns={'ticket_medio_consumo_rs': 'ticket_medio_consumo_base_rs'}, inplace=True)

# Define as colunas finais da FATO_JOGOS
colunas_fato_jogos = ['jogo_id', 'data_id', 'publico_pago', 'receita_ingresso_mil_rs', 
                      'ticket_medio_ingresso_rs', 'ticket_medio_consumo_base_rs', 'taxa_ocupacao']
DF_FATO_JOGOS = DF_FATO_JOGOS[colunas_fato_jogos]


# ==============================================================================
# 3. Geração das Tabelas Fato Filhas/Agregadas
# ==============================================================================

# 3.1. FATO_CONSUMO (Detalhe de Vendas e Receita - Tema 1, 8)
# Relaciona: FATO_JOGOS, DIM_PRODUTO
consumo_data = []
for index, jogo in df_base.iterrows():
    publico_pago = fato_data.iloc[index]['publico_pago']
    ticket_medio_consumo = fato_data.iloc[index]['ticket_medio_consumo_rs']
    
    # Distribuição de Consumo por Jogo
    # Produtos mais baratos (1, 2, 5) são vendidos em maior volume
    venda_volumes = np.array([0.4, 0.15, 0.15, 0.05, 0.2, 0.05]) # Peso de venda simulado
    venda_volumes = venda_volumes / venda_volumes.sum() # Normaliza
    
    for prod_id, volume in enumerate(venda_volumes, 1):
        item_data = DIM_PRODUTO[DIM_PRODUTO['produto_id'] == prod_id].iloc[0]
        
        # Simula que ~60% do público compra
        fator_publico_consumidor = 0.6 
        
        # Quantidade vendida (Estimativa)
        qtd_vendida = int(publico_pago * fator_publico_consumidor * volume)
        
        # Receita
        receita_produto_rs = qtd_vendida * item_data['preco_medio_rs']
        
        # Consumo por pessoa é apenas para produtos internos (não merchandising)
        consumo_por_pessoa_rs = receita_produto_rs / publico_pago if item_data['categoria'] != 'Merchandising' else 0

        consumo_data.append({
            'jogo_id': jogo['jogo_id'],
            'produto_id': prod_id,
            'qtd_vendida': qtd_vendida,
            'receita_produto_rs': round(receita_produto_rs, 2),
            'consumo_por_pessoa_rs': round(consumo_por_pessoa_rs, 2)
        })

DF_FATO_CONSUMO = pd.DataFrame(consumo_data)


# 3.2. FATO_MOBILIDADE_INCIDENTES (Tema 5: Mobilidade, Tema 10: Incidentes)
# Relaciona: FATO_JOGOS, DIM_SETOR
mobilidade_incidente_data = []
for index, jogo in DF_FATO_JOGOS.iterrows():
    publico_pago = jogo['publico_pago']
    
    for index_setor, setor in DIM_SETOR.iterrows():
        # Simulação de Público por Setor (Tema 2: Mapa de Calor)
        # Setores Roxo e Vermelho (maior capacidade) tendem a ter mais público
        capacidade_setor = setor['capacidade_mil'] * 1000
        
        # Fator de Ocupação (mais alto para jogos de maior público)
        fator_ocupacao = jogo['taxa_ocupacao'] / 100 * (np.random.uniform(0.9, 1.1))
        
        publico_setor = int(capacidade_setor * fator_ocupacao)
        
        if publico_setor > 1000: # Ignora setores quase vazios
            
            # Tema 5: Mobilidade (Tempo médio)
            # Portões populares/maior movimento (Amarelo, Laranja) têm tempo maior
            tempo_entrada_base = 15 if setor['nome_setor'] in ['Amarelo Inferior', 'Laranja Superior'] else 10
            tempo_saida_base = 25 if setor['nome_setor'] in ['Amarelo Inferior', 'Laranja Superior'] else 20
            
            tempo_entrada = np.random.normal(tempo_entrada_base, 3)
            tempo_saida = np.random.normal(tempo_saida_base, 5)
            
            # Tema 10: Incidentes
            # Setores de maior densidade/popular (Amarelo, Vermelho) tendem a ter mais incidentes
            fator_incidente = 0.00015 if setor['nome_setor'] in ['Amarelo Inferior', 'Vermelho Inferior'] else 0.00005
            incidente_contagem = int(publico_setor * fator_incidente * np.random.uniform(0.8, 1.5))
            
            # Tempo de Resposta (inversamente proporcional ao incidente)
            tempo_resposta_min = np.random.normal(7, 2)

            mobilidade_incidente_data.append({
                'jogo_id': jogo['jogo_id'],
                'setor_id': setor['setor_id'],
                'publico_setor': publico_setor,
                'tempo_entrada_medio_min': round(max(5, tempo_entrada), 1),
                'tempo_saida_medio_min': round(max(10, tempo_saida), 1),
                'incidente_contagem': max(0, incidente_contagem),
                'tempo_resposta_min': round(max(3, tempo_resposta_min), 1)
            })

DF_FATO_MOBILIDADE_INCIDENTES = pd.DataFrame(mobilidade_incidente_data)


# 3.3. FATO_MERCADO_INGRESSOS (Tema 3: Sócios, Tema 7: Canais de Venda)
# Relaciona: DIM_DATA
mercado_data = []
socio_base = 45000 # Base inicial de sócios

for index, data in DF_DIM_DATA.iterrows():
    is_classico = DF_FATO_JOGOS[DF_FATO_JOGOS['data_id'] == data['data_id']].merge(df_base, on='jogo_id', how='left').merge(DIM_ADVERSARIO, on='adversario_id', how='left')['classico_local'].iloc[0]
    
    # Tema 3: Evolução de Sócios-Torcedores (Simulação Mensal/Anual)
    # A base de sócios cresce a cada mês (ou a cada jogo)
    novas_adesoes = np.random.randint(50, 300)
    socio_base += novas_adesoes
    
    # Tema 7: Venda de Ingressos por Canal (Simulação)
    # Classicos/Grandes priorizam Site/App (Canais 1 e 3)
    venda_total_jogo = DF_FATO_JOGOS[DF_FATO_JOGOS['data_id'] == data['data_id']]['publico_pago'].iloc[0]
    
    if is_classico:
        proporcoes = [0.55, 0.10, 0.35] # Site, Bilheteria, App
    else:
        proporcoes = [0.40, 0.25, 0.35] # Site, Bilheteria, App
        
    vendas_site = int(venda_total_jogo * proporcoes[0] * np.random.uniform(0.9, 1.1))
    vendas_bilheteria = int(venda_total_jogo * proporcoes[1] * np.random.uniform(0.9, 1.1))
    vendas_app = int(venda_total_jogo * proporcoes[2] * np.random.uniform(0.9, 1.1))
    
    mercado_data.append({
        'data_id': data['data_id'],
        'socios_ativos': socio_base,
        'novas_adesoes': novas_adesoes,
        'vendas_site': vendas_site,
        'vendas_bilheteria': vendas_bilheteria,
        'vendas_app': vendas_app,
        'canal_id': 1 # Site
    })
    mercado_data.append({
        'data_id': data['data_id'],
        'socios_ativos': socio_base,
        'novas_adesoes': novas_adesoes,
        'vendas_site': 0, # Já contabilizado no item acima
        'vendas_bilheteria': vendas_bilheteria,
        'vendas_app': 0, # Já contabilizado no item acima
        'canal_id': 2 # Bilheteria
    })
    mercado_data.append({
        'data_id': data['data_id'],
        'socios_ativos': socio_base,
        'novas_adesoes': novas_adesoes,
        'vendas_site': 0,
        'vendas_bilheteria': 0,
        'vendas_app': vendas_app,
        'canal_id': 3 # App
    })

DF_FATO_MERCADO_INGRESSOS = pd.DataFrame(mercado_data)

# 3.4. FATO_PROJECAO (Tema 9: Projeção de Público)
# Usa os dados simulados para criar uma projeção futura simplificada
projecao_data = []
for index, jogo in df_base.iterrows():
    adv = DIM_ADVERSARIO[DIM_ADVERSARIO['adversario_id'] == jogo['adversario_id']].iloc[0]
    data_jogo = jogo['data']
    
    # Fatores da Projeção
    fator_adversario = {'Classico': 1.0, 'Grande': 0.8, 'Medio': 0.6, 'Pequeno': 0.4}[adv['nivel_confronto']]
    fator_dia = 1.1 if data_jogo.dayofweek in [5, 6] else 0.9 # Fim de semana
    
    # Simula projeção baseada em 40.000
    publico_projetado = int(40000 * fator_adversario * fator_dia * np.random.uniform(0.95, 1.05))
    publico_projetado = max(15000, min(61846, publico_projetado))
    
    # Simula a Projeção de Receita (com 5% de margem)
    receita_projetada = DF_FATO_JOGOS[DF_FATO_JOGOS['jogo_id'] == jogo['jogo_id']]['receita_ingresso_mil_rs'].iloc[0] * np.random.uniform(0.95, 1.05)
    
    projecao_data.append({
        'jogo_id': jogo['jogo_id'],
        'adversario': adv['nome_adversario'],
        'publico_projetado': publico_projetado,
        'receita_projetada_mil_rs': round(receita_projetada, 3),
        'base_analise': f"Dia: {data_jogo.day_name(locale='pt_BR')}, Adversário: {adv['nivel_confronto']}"
    })
    
DF_FATO_PROJECAO = pd.DataFrame(projecao_data)


# 3.5. FATO_RECEITA_AGREGADA (Tema 8: Comparativo Receita Ingresso vs. Produtos Internos)
# Dados consolidados para o período 2024-2025
receita_total_ingresso = DF_FATO_JOGOS['receita_ingresso_mil_rs'].sum()
receita_total_produto = DF_FATO_CONSUMO['receita_produto_rs'].sum() / 1000 # Converter para mil R$

DF_FATO_RECEITA_AGREGADA = pd.DataFrame({
    'categoria_receita': ['Ingressos', 'Produtos Internos'],
    'receita_total_mil_rs': [round(receita_total_ingresso, 3), round(receita_total_produto, 3)],
    'percentual_total': [
        round(receita_total_ingresso / (receita_total_ingresso + receita_total_produto) * 100, 2),
        round(receita_total_produto / (receita_total_ingresso + receita_total_produto) * 100, 2)
    ]
})

# ==============================================================================
# 4. Exportação para CSV
# ==============================================================================

# Cria o dicionário de DataFrames para exportação
dataframes_to_export = {
    # 4 Dimensões
    "DIM_DATA": DF_DIM_DATA,
    "DIM_ADVERSARIO": DIM_ADVERSARIO,
    "DIM_SETOR": DIM_SETOR,
    "DIM_PRODUTO": DIM_PRODUTO,
    "DIM_PERFIL_TORCEDOR": DIM_PERFIL_TORCEDOR,
    
    # 5 Fatos
    "FATO_JOGOS": DF_FATO_JOGOS,
    "FATO_CONSUMO": DF_FATO_CONSUMO,
    "FATO_MOBILIDADE_INCIDENTES": DF_FATO_MOBILIDADE_INCIDENTES,
    "FATO_MERCADO_INGRESSOS": DF_FATO_MERCADO_INGRESSOS,
    "FATO_PROJECAO": DF_FATO_PROJECAO,
    "FATO_RECEITA_AGREGADA": DF_FATO_RECEITA_AGREGADA
}

arquivos_gerados = []
for nome_tabela, df in dataframes_to_export.items():
    filename = f"{nome_tabela.lower()}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    # Exporta para CSV (separador=ponto-e-vírgula, decimal=vírgula)
    df.to_csv(filepath, sep=';', decimal=',', index=False, encoding='utf-8')
    arquivos_gerados.append(filename)
    print(f"✅ Gerado: {filename} (Tamanho: {df.shape[0]} linhas)")

print("\n---")
print("Processo concluído! O banco de dados simulado com 10 temas foi gerado com sucesso no modelo Star Schema.")
print("Total de 11 arquivos CSV gerados.")