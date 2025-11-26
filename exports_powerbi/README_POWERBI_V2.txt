
DOCUMENTAÇÃO - EXPORTS POWER BI - VERSÃO 2.0
======================================================================
Data da Exportação: 26/11/2025 05:54:46

NOVIDADES DA VERSÃO 2.0:
======================================================================
✅ Adicionadas 5 novas tabelas com dados financeiros detalhados (2019-2025)
✅ 130 jogos analisados (vs 8 anteriores)
✅ Análise de precificação com gap de otimização
✅ Mix de receitas detalhado (ingressos, produtos, camarotes, estacionamento)
✅ Taxa de ocupação do estádio por competição
✅ Série temporal completa incluindo pandemia COVID-19

ARQUIVOS GERADOS:
======================================================================

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

======================================================================
RELACIONAMENTOS NO POWER BI:
======================================================================

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

======================================================================
NOVAS MEDIDAS DAX SUGERIDAS:
======================================================================

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

======================================================================
NOVAS VISUALIZAÇÕES SUGERIDAS:
======================================================================

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

======================================================================
DICAS DE USO:
======================================================================

1. Use filtros de Era (Pré-COVID, Pandemia, Pós-COVID) para análises temporais

2. Combine tipo_adversario com taxa_ocupacao para estratégias de pricing

3. Analise gap_otimizacao por competição para identificar oportunidades

4. Compare % não-pagantes entre competições para avaliar políticas de cortesia

5. Use eficiência_precificacao para benchmarking entre períodos

6. Analise mix de receitas para diversificação de fontes

======================================================================
