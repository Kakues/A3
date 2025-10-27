
DOCUMENTAÇÃO - EXPORTS POWER BI
======================================================================
Data da Exportação: 27/10/2025 08:57:46

ARQUIVOS GERADOS:
======================================================================

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

======================================================================
SUGESTÕES DE RELACIONAMENTOS NO POWER BI:
======================================================================

FATO_Jogos [jogo_id] --> DIM_Produtos [jogo_id]
FATO_Jogos [jogo_id] --> DIM_Demografica [Jogo_ID]
FATO_Jogos [data] --> FATO_Temporal [data]

======================================================================
MEDIDAS DAX SUGERIDAS:
======================================================================

Receita Total = SUM(FATO_Jogos[total_arrecadado])
Público Total = SUM(FATO_Jogos[publico_total])
Ticket Médio = AVERAGE(FATO_Jogos[ticket_medio_ingresso])
Taxa de Ocupação = [Público Total] / 61927
Crescimento YoY = ([Receita Total] - CALCULATE([Receita Total], SAMEPERIODLASTYEAR(FATO_Temporal[data]))) / CALCULATE([Receita Total], SAMEPERIODLASTYEAR(FATO_Temporal[data]))

======================================================================
