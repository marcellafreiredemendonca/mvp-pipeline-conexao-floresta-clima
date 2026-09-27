# Databricks notebook source
# MAGIC %md
# MAGIC ## Objetivo
# MAGIC
# MAGIC ### Contexto do Projeto
# MAGIC
# MAGIC Este trabalho se insere no projeto **Conexão Floresta & Clima**, desenvolvido pela Entidade Ambientalista Onda Verde e patrocinada pela Petrobrás, com foco no monitoramento ambiental do Rio Soberbo, em Guapimirim (RJ). O componente de monitoramento hídrico e meteorológico do projeto tem duração planejada de **3 anos**, com coletas periódicas realizadas por alunos da rede pública treinados para essa finalidade — caracterizando uma iniciativa de **ciência cidadã** aplicada à educação básica.
# MAGIC
# MAGIC A autora deste MVP atuou inicialmente como professora orientadora e atualmente exerce a função de coordenadora do projeto na unidade escolar, sendo responsável pela ponte entre a ONG, o corpo docente e os alunos envolvidos nas atividades.
# MAGIC
# MAGIC O motivador prático do projeto é uma questão real observada na comunidade: em períodos de chuva intensa, o aumento de turbidez do Rio Soberbo leva ao fechamento do abastecimento de água para a população local. Compreender essa relação — entre eventos de chuva e alterações na qualidade da água — é o problema de negócio central que orienta este trabalho.
# MAGIC
# MAGIC ### Escopo deste MVP
# MAGIC
# MAGIC Este MVP contempla o desenvolvimento do pipeline de engenharia de dados, responsável por estruturar, tratar e integrar duas fontes heterogêneas de informação: os registros físico-químicos das coletas de campo da ciência cidadã e a série temporal pluviométrica do CEMADEN. O foco desta entrega é garantir a higienização, a consistência e a disponibilização de uma base analítica unificada sobre o Rio Soberbo. Essa arquitetura de dados servirá como infraestrutura essencial para viabilizar, futuramente, a criação de soluções preditivas e de um sistema de alertas à população sobre a gestão e o uso consciente da água diante de riscos de desabastecimento.	
# MAGIC
# MAGIC ### Perguntas de Negócio
# MAGIC
# MAGIC - Existe relação entre eventos de chuva e o aumento de turbidez ou alteração de outros parâmetros de qualidade da água no Rio Soberbo?
# MAGIC - Os parâmetros de qualidade da água se mantêm dentro de padrões esperados ao longo do período monitorado?
# MAGIC - Quais dias de coleta coincidem com períodos de chuva, e o que isso sugere sobre o padrão de resposta do rio a esses eventos?
# MAGIC
# MAGIC ### Fontes de Dados
# MAGIC
# MAGIC - **Qualidade da água:** Coletas e análises de parâmetros de qualidade de água realizadas por alunos treinados no âmbito do projeto. (pH, amônia, nitrato, nitrito, ortofosfato, oxigênio dissolvido, temperatura, condutividade, salinidade, "TDS" ou "Sólidos Totais Dissolvidos" e turbidez.)
# MAGIC - **Precipitação:** Estação pluviométrica pública do CEMADEN (estação "Rio Soberbo", Guapimirim), adotada em caráter transitório devido ao processo em andamento de readequação da estação meteorológica instalada na unidade escolar, situada próxima ao ponto de monitoramento.
# MAGIC
# MAGIC ### Governança, Procedência e Consentimento dos Dados
# MAGIC
# MAGIC Os dados primários de qualidade da água foram disponibilizados no âmbito da atuação da autora como coordenadora do projeto junto à Onda Verde, para fins de pesquisa acadêmica. Para assegurar a conformidade, a reprodutibilidade e a integridade da informação, todo o ciclo de ingestão, sanitização e cruzamento com a base pública do CEMADEN foi desenvolvido em ambiente Databricks, garantindo a rastreabilidade (data lineage) e a padronização das transformações.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Coleta e Ingestão (Camada Bronze)
# MAGIC
# MAGIC Dados de qualidade da água coletados em campo por alunos treinados (6 coletas entre abril e setembro de 2026) e dados de precipitação da estação CEMADEN "Rio Soberbo" em Guapimirim, ambos enviados como CSV para um Volume do Unity Catalog e carregados nesta camada sem transformação. 

# COMMAND ----------

path_volume = "/Volumes/conexao_floresta_clima/bronze/arquivos_brutos"
file_name = "coletas_agua_rio_soberbo.csv"

df = spark.read.csv(f"{path_volume}/{file_name}",
  header=True,
  inferSchema=True,
  sep=",")

display(df)

# COMMAND ----------

df.printSchema() 

# COMMAND ----------

from pyspark.sql.types import DoubleType

df = df.withColumn("ph_kit", df["ph_kit"].cast(DoubleType()))
df = df.withColumn("ortofosfato", df["ortofosfato"].cast(DoubleType()))

df.printSchema()

# COMMAND ----------

df.write.mode("overwrite").option("overwriteSchema","true").saveAsTable("conexao_floresta_clima.bronze.coletas_agua_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Transformação e Limpeza (Camada Silver) - Qualidade da Água 
# MAGIC
# MAGIC Ajuste de tipos de dados e adição de metadados de controle (fonte e data de ingestão). Nesta etapa também são documentadas as limitações conhecidas do dataset: ausência de sonda na 1° coleta, evolução do protocolo (turbidez/cor incorporadas a partir da 6° coleta), e um dado retificado manualmente (troca entre salinidade e TDS na coleta de 17/06). O erro foi corrigido na fonte (CSV) e a tabela Bronze foi carregada com `overwriteSchema`, sendo o histórico de versões preservado no Delta Lake (ver `DESCRIBE HISTORY`).  
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import DoubleType

df_bronze = spark.table("conexao_floresta_clima.bronze.coletas_agua_raw")

df_silver = (
    df_bronze
    .withColumn("turbidez", df_bronze["turbidez"].cast(DoubleType()))
    .withColumn("ortofosfato", df_bronze["ortofosfato"].cast(DoubleType()))
    .withColumn("fonte_dado", lit("Coleta de campo — Conexão Floresta e Clima (alunos treinados)"))
    .withColumn("data_ingestao", current_timestamp())
)

display(df_silver)

# COMMAND ----------

df_silver.write.mode("overwrite").saveAsTable("conexao_floresta_clima.silver.coletas_agua")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Transformação e Limpeza
# MAGIC
# MAGIC Leitura de dados brutos da estação CEMADEN "Rio Soberbo", agregação de leituras sub-horárias em totais diários de precipitação (mm)

# COMMAND ----------


from pyspark.sql.functions import to_date, sum as spark_sum, col, regexp_replace

path_volume = "/Volumes/conexao_floresta_clima/bronze/arquivos_brutos"

df_chuva_raw = spark.read.csv(f"{path_volume}/data_*.csv",
  header=True,
  sep=";",
  inferSchema=False)

df_chuva_raw.printSchema()
display(df_chuva_raw)


# COMMAND ----------

df_chuva_raw = (
    df_chuva_raw
    .withColumn("valorMedida", regexp_replace(col("valorMedida"), ",", ".").cast("double"))
    .withColumn("data", to_date(col("datahora")))
)

df_rio_soberbo = df_chuva_raw.filter(col("nomeEstacao") == "Rio Soberbo")

from pyspark.sql.functions import count, when

df_precipitacao_diaria = (
    df_rio_soberbo
    .groupBy("data")
    .agg(
        spark_sum("valorMedida").alias("precipitacao_mm"),
        count("*").alias("numero_leituras")
    )
    .withColumn(
        "cobertura_dados_chuva",
        when(col("numero_leituras") < 5, "baixa").otherwise("adequada")
    )
    .orderBy("data")
)

display(df_precipitacao_diaria)

# COMMAND ----------

from pyspark.sql.functions import round as spark_round

df_precipitacao_diaria = df_precipitacao_diaria.withColumn(
    "precipitacao_mm", spark_round(col("precipitacao_mm"), 1)
)

# COMMAND ----------

df_precipitacao_diaria.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("conexao_floresta_clima.silver.precipitacao_diaria")


# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE conexao_floresta_clima.silver.coletas_agua ALTER COLUMN data_coleta COMMENT 'Data em que a coleta de água foi realizada no Rio Soberbo';
# MAGIC ALTER TABLE conexao_floresta_clima.silver.coletas_agua ALTER COLUMN condicao_tempo COMMENT 'Condição climática do dia da coleta (seco ou chuvoso); retroativa por observação de campo para coletas anteriores a 10/09/2026';
# MAGIC ALTER TABLE conexao_floresta_clima.silver.coletas_agua ALTER COLUMN temperatura_agua_c COMMENT 'Temperatura da água em graus Celsius';
# MAGIC ALTER TABLE conexao_floresta_clima.silver.coletas_agua ALTER COLUMN temperatura_ar_c COMMENT 'Temperatura do ar em graus Celsius';
# MAGIC ALTER TABLE conexao_floresta_clima.silver.coletas_agua ALTER COLUMN od COMMENT 'Oxigênio dissolvido na água';
# MAGIC ALTER TABLE conexao_floresta_clima.silver.coletas_agua ALTER COLUMN ph_kit COMMENT 'pH medido com kit colorimétrico';
# MAGIC ALTER TABLE conexao_floresta_clima.silver.coletas_agua ALTER COLUMN ph_sonda COMMENT 'pH medido com sonda eletrônica; ausente na 1ª coleta (15/04/2026) por falta do equipamento';
# MAGIC ALTER TABLE conexao_floresta_clima.silver.coletas_agua ALTER COLUMN amonia COMMENT 'Concentração de amônia (mg/L)';
# MAGIC ALTER TABLE conexao_floresta_clima.silver.coletas_agua ALTER COLUMN nitrato COMMENT 'Concentração de nitrato (mg/L)';
# MAGIC ALTER TABLE conexao_floresta_clima.silver.coletas_agua ALTER COLUMN nitrito COMMENT 'Concentração de nitrito (mg/L)';
# MAGIC ALTER TABLE conexao_floresta_clima.silver.coletas_agua ALTER COLUMN ortofosfato COMMENT 'Concentração de ortofosfato (mg/L)';
# MAGIC ALTER TABLE conexao_floresta_clima.silver.coletas_agua ALTER COLUMN turbidez COMMENT 'Turbidez medida instrumentalmente (NTU); disponível apenas a partir da 6ª coleta (10/09/2026)';
# MAGIC ALTER TABLE conexao_floresta_clima.silver.coletas_agua ALTER COLUMN condutividade COMMENT 'Condutividade elétrica da água';
# MAGIC ALTER TABLE conexao_floresta_clima.silver.coletas_agua ALTER COLUMN salinidade COMMENT 'Salinidade da água';
# MAGIC ALTER TABLE conexao_floresta_clima.silver.coletas_agua ALTER COLUMN tds COMMENT 'Sólidos totais dissolvidos (TDS); ausente na coleta de 17/06/2026';
# MAGIC ALTER TABLE conexao_floresta_clima.silver.coletas_agua ALTER COLUMN fonte_dado COMMENT 'Origem do dado: coleta de campo realizada por alunos treinados, no âmbito do projeto Conexão Floresta e Clima';
# MAGIC ALTER TABLE conexao_floresta_clima.silver.coletas_agua ALTER COLUMN data_ingestao COMMENT 'Timestamp de quando o registro foi carregado na plataforma Databricks';

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE conexao_floresta_clima.silver.precipitacao_diaria ALTER COLUMN data COMMENT 'Data de referência da medição de chuva';
# MAGIC ALTER TABLE conexao_floresta_clima.silver.precipitacao_diaria ALTER COLUMN precipitacao_mm COMMENT 'Precipitação total diária (mm), somada a partir das leituras sub-horárias da estação CEMADEN Rio Soberbo (Guapimirim); dias sem nenhuma leitura registrada ficam ausentes da série, não confundir com chuva zero';

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE conexao_floresta_clima.silver.coletas_agua ALTER COLUMN tds COMMENT 'Sólidos Totais Dissolvidos (Total Dissolved Solids) em mg/L — quantidade total de substâncias dissolvidas na água (minerais, sais, matéria orgânica); ausente na coleta de 17/06/2026';

# COMMAND ----------

# MAGIC %md
# MAGIC ##Modelagem e Integração (Camada Gold)
# MAGIC
# MAGIC Junção das tabelas Silver de qualidade da água e precipitação diária, por data de coleta, formando uma visão única (Star Schema simplificado: fato `qualidade_água`+ dimensão `precipitacao`). Nesta etapa são calculados os campos derivados `choveu` (indicador booleano) e `dias_desde_ultima_chuva`, usados para responder à pergunta de negócio sobre a relação entre eventos de chuva e variação nos parâmetros de qualidade da água.

# COMMAND ----------

from pyspark.sql.functions import col, when, last, datediff
from pyspark.sql.window import Window

df_chuva = spark.table("conexao_floresta_clima.silver.precipitacao_diaria")

df_chuva_flag = df_chuva.withColumn(
    "choveu", when(col("precipitacao_mm") > 0, True).otherwise(False)
)

window_spec = Window.orderBy("data").rowsBetween(Window.unboundedPreceding, 0)

df_chuva_flag = df_chuva_flag.withColumn(
    "data_ultima_chuva",
    last(when(col("choveu"), col("data")), ignorenulls=True).over(window_spec)
)

df_chuva_flag = df_chuva_flag.withColumn(
    "dias_desde_ultima_chuva",
    when(col("data_ultima_chuva").isNotNull(), datediff(col("data"), col("data_ultima_chuva")))
)

display(df_chuva_flag)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE conexao_floresta_clima.silver.precipitacao_diaria ALTER COLUMN numero_leituras COMMENT 'Quantidade de leituras registradas pela estação naquele dia; usado para avaliar confiabilidade do dado agregado';
# MAGIC ALTER TABLE conexao_floresta_clima.silver.precipitacao_diaria ALTER COLUMN cobertura_dados_chuva COMMENT 'Indicador de confiabilidade do dado diário: "baixa" quando há menos de 5 leituras no dia (possível falha ou intermitência da estação), "adequada" caso contrário';

# COMMAND ----------

df_coletas = spark.table("conexao_floresta_clima.silver.coletas_agua")

df_gold = df_coletas.join(
    df_chuva_flag.select("data", "precipitacao_mm", "choveu", "dias_desde_ultima_chuva", "numero_leituras", "cobertura_dados_chuva"),
    df_coletas.data_coleta == df_chuva_flag.data,
    "left"
).drop("data")

display(df_gold)

# COMMAND ----------

df_gold.write.mode("overwrite").saveAsTable("conexao_floresta_clima.gold.fato_qualidade_agua")


# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE conexao_floresta_clima.gold.fato_qualidade_agua ALTER COLUMN choveu COMMENT 'Indicador booleano se houve chuva registrada no dia da coleta (precipitação > 0mm)';
# MAGIC ALTER TABLE conexao_floresta_clima.gold.fato_qualidade_agua ALTER COLUMN dias_desde_ultima_chuva COMMENT 'Número de dias desde o último evento de chuva registrado antes da data de coleta';

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE conexao_floresta_clima.gold.fato_qualidade_agua ALTER COLUMN numero_leituras COMMENT 'Quantidade de leituras registradas pela estação naquele dia; usado para avaliar confiabilidade do dado agregado';
# MAGIC ALTER TABLE conexao_floresta_clima.gold.fato_qualidade_agua ALTER COLUMN cobertura_dados_chuva COMMENT 'Indicador de confiabilidade do dado diário: "baixa" quando há menos de 5 leituras no dia (possível falha ou intermitência da estação), "adequada" caso contrário';

# COMMAND ----------

# MAGIC %md
# MAGIC ##Qualidade de Dados
# MAGIC
# MAGIC Verificação de completude, consistência e outliers nos dados de qualidade da água e preciptação. Discussão dos problemas identificados (lacunas por ausência de instrumento, dado retificado manualmente, falhas de leitura na série de chuva) e como cada um foi tratado ou documentado.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Completude
# MAGIC
# MAGIC - **Sonda ausente na 1ª coleta (15/04/2026):** os campos `temperatura_agua_c`, `temperatura_ar_c`, `ph_sonda`, `condutividade`, `salinidade` e `tds` estão nulos nessa data porque o equipamento (sonda) ainda não estava disponível no início do projeto.
# MAGIC - **TDS ausente na coleta de 17/06/2026:** valor não registrado nessa data específica, sem relação com a ausência de equipamento (a sonda já estava em uso, com pH sonda, condutividade e salinidade presentes).
# MAGIC - **Turbidez, cor e condição climática ausentes até a 5ª coleta:** esses parâmetros foram incorporados ao protocolo apenas a partir da 6ª coleta (10/09/2026), refletindo a evolução natural de um projeto piloto. Para as coletas anteriores, `cor` e `turbidez_qualitativa` foram preenchidos retroativamente com base em observação de campo (não instrumental), e `turbidez` (NTU) permanece nula.
# MAGIC - **Lacunas na série de precipitação:** a estação CEMADEN "Rio Soberbo" não possui nenhuma leitura registrada para os dias 18/05/2026 e 17/06/2026 (datas que coincidem com coletas de água), resultando em valores nulos de `precipitacao_mm` para essas datas na camada Gold — distintos de dias com chuva zero efetivamente registrada.
# MAGIC
# MAGIC ### Consistência
# MAGIC
# MAGIC - Os dados de precipitação vieram no formato brasileiro (separador decimal `,`), exigindo conversão para `.` antes da tipagem numérica.
# MAGIC - Os tipos de dado de todas as colunas foram validados e ajustados na camada Silver (ex.: `ph_kit` e `ortofosfato`, inicialmente inferidos como `integer`, foram convertidos para `double` para suportar valores decimais em coletas futuras).
# MAGIC
# MAGIC ### Unicidade
# MAGIC
# MAGIC - Cada linha da tabela `coletas_agua` corresponde a uma data de coleta única; não foram identificadas duplicatas.
# MAGIC - Na precipitação, a agregação por dia (soma de leituras sub-horárias) garante uma linha por data, sem duplicidade.
# MAGIC
# MAGIC ### Acurácia
# MAGIC
# MAGIC - **Erro de transcrição identificado e corrigido:** na coleta de 17/06/2026, os valores de salinidade e TDS haviam sido registrados trocados na fonte original. O erro foi identificado durante a análise, corrigido no CSV de origem, e a tabela Bronze foi recarregada (histórico de versões preservado via `DESCRIBE HISTORY` do Delta Lake).
# MAGIC - **Divergência entre observação de campo e dado de estação pública:** na 6ª coleta (10/09/2026), a condição climática foi registrada como "chuvoso" em campo, mas a estação CEMADEN teve apenas 1 leitura no dia (valor 0mm), classificada como cobertura **"baixa"** na coluna `cobertura_dados_chuva`. Essa divergência não foi corrigida artificialmente; em vez disso, foi tornada rastreável através das colunas `numero_leituras` e `cobertura_dados_chuva`, permitindo que análises futuras considerem ou excluam esse dado conforme a confiabilidade necessária.
# MAGIC
# MAGIC ### Outliers
# MAGIC
# MAGIC - **Salinidade elevada em 10/08/2026 (valor 113, frente a 10,1 e 12,8 nas demais coletas):** inicialmente identificado como possível erro, foi posteriormente confirmado como valor real coincidente com um dia de chuva — reforçando a hipótese de que eventos pluviométricos alteram as características físico-químicas do rio, e sustentando a relevância da pergunta de negócio deste projeto.
# MAGIC
# MAGIC ### Conclusão
# MAGIC
# MAGIC As limitações identificadas são típicas de um projeto de monitoramento ambiental em fase inicial (piloto), com protocolo em evolução e coleta realizada por alunos treinados. Nenhuma limitação impediu a persistência e modelagem dos dados; todas foram documentadas na origem (comentários de coluna no Unity Catalog) e refletidas em colunas específicas que tornam a confiabilidade de cada registro transparente para análises futuras.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Análise Final
# MAGIC
# MAGIC Consultas e análises respondendo às perguntas de negócio definidas, com discussão dos resultados e conexão com o problema original (relação entre chuva e qualidade da água / turbidez no Rio Soberbo)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   data_coleta,
# MAGIC   condicao_tempo,
# MAGIC   ph_kit,
# MAGIC   ph_sonda,
# MAGIC   od,
# MAGIC   turbidez,
# MAGIC   turbidez_qualitativa,
# MAGIC   condutividade,
# MAGIC   salinidade,
# MAGIC   precipitacao_mm,
# MAGIC   choveu,
# MAGIC   cobertura_dados_chuva
# MAGIC FROM conexao_floresta_clima.gold.fato_qualidade_agua
# MAGIC ORDER BY data_coleta
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Pergunta 1: Existe relação entre eventos de chuva e o aumento de turbidez ou alteração de outros parâmetros de qualidade da água?
# MAGIC
# MAGIC Das seis amostragens realizadas, duas coincidiram com eventos pluviométricos na data exata da amostragem (10/08/2026 e 10/09/2026). Em 10/08/2026, registraram-se as máximas de salinidade (113) e de condutividade elétrica (35,1) de toda a série histórica, contrastando com o intervalo de 1,9 a 12,8 para salinidade e 2,8 a 32,5 para condutividade observados nos períodos secos. Tal comportamento sugere que a precipitação direta alterou temporariamente o equilíbrio físico-químico do corpo hídrico. Similarmente, na amostragem de 10/09/2026, sob ocorrência de chuva, o pH (kit) apresentou o valor mínimo da série (5,0, em comparação ao padrão de 7,0 das demais datas), momento em que também se obteve a primeira aferição instrumental de turbidez, registrando 25 NTU.
# MAGIC
# MAGIC Nas amostragens de 15/04/2026 e 02/07/2026, o indicador `dias_desde_ultima_chuva` apontou precipitações ocorridas três e cinco dias antes da coleta, respectivamente. Nesses cenários, os parâmetros físico-químicos mantiveram-se dentro da faixa de variação normal das amostragens secas, sem a presença dos outliers identificados em 10/08 e 10/09. Essa evidência indica que a imediatez temporal do evento pluvial possui maior relevância sobre a qualidade da água do que a ocorrência de chuvas em dias antecedentes, restringindo as alterações mais expressivas aos dias de precipitação concomitante à coleta.
# MAGIC
# MAGIC Esses indícios são consistentes com a hipótese de que eventos de chuva impactam a qualidade da água do Rio Soberbo; no entanto, a robustez estatística dessa conclusão é limitada por uma amostra de somente 2 dias de chuva registrados — reforçando a importância da continuidade do projeto ao longo dos 3 anos previstos para a coleta.
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Pergunta 2: Os parâmetros de qualidade da água se mantêm dentro de padrões esperados ao longo do período monitorado?
# MAGIC
# MAGIC Em linhas gerais, os parâmetros analisados mantiveram-se dentro de padrões compatíveis com um corpo hídrico saudável ao longo do período monitorado. O pH manteve-se predominantemente neutro (6,0 a 7,9, a depender do instrumento) ao longo do período, com exceção da coleta de 10/09 (pH 5). O oxigênio dissolvido variou entre 5,0 e 9,0, dentro de faixas aceitáveis para a saúde do corpo hídrico. Amônia, nitrito e ortofosfato permaneceram próximos de zero em todas as coletas, indicando ausência de poluição orgânica significativa no período. A cor da água foi registrada como incolor em todas as amostragens, e a avaliação de transparência — qualitativa (disco de Secchi) ou instrumental (turbidímetro) — indicou água visualmente límpida na maior parte do tempo monitorado.
# MAGIC
# MAGIC ### Pergunta 3: Quais dias de coleta coincidem com períodos de chuva, e o que isso sugere?
# MAGIC
# MAGIC Entre as seis amostragens realizadas, duas registraram precipitação na data exata da coleta (10/08/2026 e 10/09/2026) e duas (15/04/2026 e 02/07/2026) apresentaram chuva recente — três e cinco dias antes, respectivamente. Para as coletas de 18/05/2026 e 17/06/2026, não foi possível determinar a ocorrência de chuva no período antecedente, em razão da ausência de leituras da estação pluviométrica CEMADEN nesses intervalos — limitação já discutida na seção de Qualidade de Dados.
# MAGIC
# MAGIC Cabe ressaltar, ainda, que o dado de precipitação da coleta de 10/09/2026 apresentou baixa cobertura de leituras na estação (apenas um registro no dia), de modo que a classificação de "dia de chuva" para essa data fundamenta-se na observação de campo, e não no dado quantitativo da estação pública.
# MAGIC
# MAGIC Esse padrão sugere que o desenho atual de coleta — realizado em calendário fixo, não condicionado à ocorrência de chuva — captura eventos pluviométricos de maneira incidental, resultando em contraste limitado entre condições secas e chuvosas dentro da amostra. Isso evidencia a necessidade de um planejamento mais estratégico do calendário de coleta do projeto: agendar parte das coletas já previstas para dias de chuva, em vez de seguir apenas uma periodicidade fixa, enriqueceria a compreensão do comportamento do rio nesses eventos, com valor tanto para o monitoramento ambiental do projeto quanto para sustentar, no futuro, um modelo preditivo mais robusto.
# MAGIC
# MAGIC Adicionalmente, as lacunas identificadas na série da estação CEMADEN — ausência total de leituras em determinados dias e cobertura insuficiente em outros — são consistentes com limitações conhecidas de redes automatizadas de baixo custo. Vale registrar que, antes de adotar a CEMADEN, buscou-se também a base da ANA (Agência Nacional de Águas), cuja estação mais próxima retornou arquivos sem dados disponíveis para o período — confirmando que a limitação de cobertura climática nesta região não é exclusiva de uma única fonte. Isso não invalida o uso da CEMADEN como fonte provisória neste MVP, mas reforça o valor de, no futuro, contar com a estação meteorológica própria do projeto (atualmente em processo de reinstalação) como fonte primária de dados climáticos, com maior controle sobre a qualidade e continuidade das leituras.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Autoavaliação
# MAGIC
# MAGIC ### Objetivos Alcançados
# MAGIC
# MAGIC Os objetivos definidos na etapa inicial deste MVP foram integralmente atingidos: foi construído um pipeline de dados completo, cobrindo desde a definição do problema até a análise final, seguindo a arquitetura medalhão (Bronze, Silver e Gold) na plataforma Databricks. As três perguntas de negócio formuladas foram respondidas com base nos dados disponíveis, e a documentação de linhagem, catálogo de dados e qualidade foi mantida ao longo de todo o processo.
# MAGIC
# MAGIC ### Dificuldades Encontradas
# MAGIC
# MAGIC A principal dificuldade foi o volume reduzido de dados disponíveis: apenas seis coletas de qualidade da água foram realizadas até o momento, o que limita a robustez estatística das inferências apresentadas. Também foram identificadas lacunas significativas na série de precipitação de fontes públicas — tanto na ANA, cuja estação mais próxima não retornou dados para o período, quanto na CEMADEN, cuja série apresentou dias sem nenhuma leitura registrada e outros com cobertura insuficiente.
# MAGIC
# MAGIC Também foi identificado, durante o processo, um erro de transcrição nos dados de campo (troca entre os valores de salinidade e TDS em uma das coletas), corrigido a partir do reprocessamento da camada Bronze com preservação do histórico de versões via Delta Lake.
# MAGIC
# MAGIC ### O que não foi possível atingir
# MAGIC
# MAGIC Não foi possível estabelecer uma relação estatisticamente significativa entre eventos de chuva e alteração nos parâmetros de qualidade da água, dado o tamanho reduzido da amostra (apenas duas coletas coincidiram com chuva no dia exato). As tendências observadas são consistentes com a hipótese de impacto, mas não permitem generalização.
# MAGIC
# MAGIC ### Trabalhos Futuros
# MAGIC
# MAGIC Este MVP estabelece a infraestrutura de dados (pipeline Bronze/Silver/Gold) que servirá de base para etapas futuras de análise mais aprofundada sobre a relação entre eventos de chuva e qualidade da água no Rio Soberbo. Visto que o componente de monitoramento do projeto Conexão Floresta e Clima tem duração planejada de três anos, espera-se que a base de dados cresça substancialmente, fortalecendo tanto a consistência estatística das análises quanto a confiabilidade de um futuro modelo preditivo destinado à antecipação de eventos de chuva e possíveis fechamentos de abastecimento no Rio Soberbo.
# MAGIC
# MAGIC Entre os refinamentos identificados para ciclos futuros do projeto, destacam-se: (i) um planejamento mais estratégico do calendário de coleta, contemplando dias de chuva de forma deliberada; e (ii) a substituição da estação pluviométrica pública (CEMADEN) pela estação meteorológica própria do projeto, o que deverá reduzir a dependência de fontes externas com cobertura irregular.
# MAGIC
# MAGIC Adicionalmente, os dados e a infraestrutura construídos neste MVP têm potencial de uso para além do escopo acadêmico: a coordenação da ONG Onda Verde manifestou interesse em uma publicação científica em coautoria a partir dos resultados deste monitoramento, o que reforça o valor do rigor metodológico mantido ao longo deste trabalho.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Glossário de Termos Técnicos
# MAGIC
# MAGIC - **pH**: Potencial hidrogeniônico — mede o quão ácida ou alcalina é a água, numa escala de 0 (muito ácido) a 14 (muito alcalino); 7 é neutro.
# MAGIC - **OD (Oxigênio Dissolvido)**: Quantidade de oxigênio disponível na água, essencial para a vida aquática; valores baixos podem indicar poluição orgânica.
# MAGIC - **Amônia**: Composto nitrogenado geralmente associado a esgoto ou matéria orgânica em decomposição; concentrações altas indicam poluição recente.
# MAGIC - **Nitrato e Nitrito**: Formas de nitrogênio na água, frequentemente ligadas a fertilizantes agrícolas ou esgoto; o nitrito é mais tóxico e geralmente transitório (se converte em nitrato).
# MAGIC - **Ortofosfato**: Forma de fósforo dissolvido na água, associada a fertilizantes e detergentes; em excesso, contribui para a eutrofização (crescimento excessivo de algas).
# MAGIC - **Turbidez**: Medida da transparência da água — quanto mais partículas em suspensão (sedimentos, matéria orgânica), maior a turbidez. Medida em NTU.
# MAGIC - **NTU (Nephelometric Turbidity Unit)**: Unidade de medida da turbidez, baseada na quantidade de luz dispersada pela água.
# MAGIC - **Disco de Secchi**: Instrumento simples (disco branco e preto) usado para avaliar visualmente a transparência da água; na ausência de um turbidímetro, faz uma medição qualitativa da turbidez.
# MAGIC - **Condutividade**: Capacidade da água de conduzir corrente elétrica, relacionada à quantidade de íons dissolvidos (sais, minerais).
# MAGIC - **Salinidade**: Concentração de sais dissolvidos na água.
# MAGIC - **TDS (Total Dissolved Solids / Sólidos Totais Dissolvidos)**: Quantidade total de substâncias dissolvidas na água (minerais, sais, matéria orgânica), medida em mg/L.