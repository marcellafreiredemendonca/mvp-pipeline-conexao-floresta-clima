# MVP — Pipeline de Dados: Qualidade da Água do Rio Soberbo

**Pós-graduação em Data Science and Analytics — PUC-Rio**
**Autora:** Marcella Freire de Mendonça

> Pipeline de engenharia de dados construído em Databricks (Free Edition), seguindo a arquitetura medalhão (Bronze/Silver/Gold), a partir de dados reais de monitoramento ambiental do Rio Soberbo (Guapimirim, RJ), coletados no âmbito do projeto **Conexão Floresta e Clima** (Onda Verde, patrocínio Petrobras).

---

## Sumário

1. [Objetivo](#objetivo)
2. [Coleta de Dados](#coleta-de-dados)
3. [Modelagem de Dados e Catálogo](#modelagem-de-dados-e-catálogo)
4. [Pipeline de ETL](#pipeline-de-etl)
5. [Qualidade de Dados](#qualidade-de-dados)
6. [Análise Final](#análise-final)
7. [Autoavaliação](#autoavaliação)
8. [Evidências (prints)](#evidências-prints)
9. [Estrutura do repositório](#estrutura-do-repositório)

---

## Objetivo

### Contexto do Projeto

Este trabalho se insere no projeto **Conexão Floresta & Clima**, desenvolvido pela entidade ambientalista Onda Verde e patrocinado pela Petrobras, com foco no monitoramento ambiental do Rio Soberbo, em Guapimirim (RJ). O componente de monitoramento hídrico e meteorológico do projeto tem duração planejada de **3 anos**, com coletas periódicas realizadas por alunos da rede pública treinados para essa finalidade — caracterizando uma iniciativa de **ciência cidadã** aplicada à educação básica.

A autora deste MVP atuou inicialmente como professora orientadora e atualmente exerce a função de coordenadora do projeto na unidade escolar, sendo responsável pela ponte entre a ONG, o corpo docente e os alunos envolvidos nas atividades.

O motivador prático do projeto é uma questão real observada na comunidade: em períodos de chuva intensa, o aumento de turbidez do Rio Soberbo leva ao fechamento do abastecimento de água para a população local. Compreender essa relação — entre eventos de chuva e alterações na qualidade da água — é o problema de negócio central que orienta este trabalho.

### Escopo deste MVP

Este MVP contempla o desenvolvimento do pipeline de engenharia de dados, responsável por estruturar, tratar e integrar duas fontes heterogêneas de informação: os registros físico-químicos das coletas de campo da ciência cidadã e a série temporal pluviométrica do CEMADEN. O foco desta entrega é garantir a higienização, a consistência e a disponibilização de uma base analítica unificada sobre o Rio Soberbo. Essa arquitetura de dados servirá como infraestrutura essencial para viabilizar, futuramente, a criação de soluções preditivas e de um sistema de alertas à população sobre a gestão e o uso consciente da água diante de riscos de desabastecimento.

### Perguntas de Negócio

- Existe relação entre eventos de chuva e o aumento de turbidez ou alteração de outros parâmetros de qualidade da água no Rio Soberbo?
- Os parâmetros de qualidade da água se mantêm dentro de padrões esperados ao longo do período monitorado?
- Quais dias de coleta coincidem com períodos de chuva, e o que isso sugere sobre o padrão de resposta do rio a esses eventos?

### Governança, Procedência e Consentimento dos Dados

Os dados primários de qualidade da água foram disponibilizados no âmbito da atuação da autora como coordenadora do projeto junto à Onda Verde, para fins de pesquisa acadêmica. Para assegurar a conformidade, a reprodutibilidade e a integridade da informação, todo o ciclo de ingestão, sanitização e cruzamento com a base pública do CEMADEN foi desenvolvido em ambiente Databricks, garantindo a rastreabilidade (data lineage) e a padronização das transformações.

---

## Coleta de Dados

| Fonte | Descrição | Formato | Frequência |
|---|---|---|---|
| **Qualidade da água** | Coletas e análises de parâmetros físico-químicos (pH, amônia, nitrato, nitrito, ortofosfato, oxigênio dissolvido, temperatura, condutividade, salinidade, TDS e turbidez) realizadas por alunos treinados no âmbito do projeto Conexão Floresta e Clima | CSV, carregado via Volume do Unity Catalog | 6 coletas realizadas entre abril e setembro/2026 |
| **Precipitação** | Estação pluviométrica pública do CEMADEN — estação "Rio Soberbo", Guapimirim, RJ. Fonte adotada em caráter transitório, enquanto a estação meteorológica própria do projeto (instalada na unidade escolar) passa por processo de readequação | CSV mensal, separado por `;`, valores em formato numérico brasileiro (decimal `,`) | Leituras sub-horárias, agregadas para granularidade diária |

Antes de adotar o CEMADEN, foi avaliada a base da ANA (Agência Nacional de Águas), cuja estação mais próxima retornou arquivos sem dados disponíveis para o período — o que motivou a mudança de fonte (ver detalhes na seção de Análise Final).

### Registro Fotográfico

Fotos do processo de coleta em campo — o Rio Soberbo, a sonda multiparâmetro, o kit de análise e um aluno realizando a coleta (sem exposição de rosto, por se tratar de menores de idade) — estão disponíveis na pasta `/fotos_coleta` deste repositório.

---

## Modelagem de Dados e Catálogo

Arquitetura medalhão implementada em um catálogo Unity Catalog (`conexao_floresta_clima`), com um schema por camada:

### Bronze — dado bruto

| Tabela | Origem | Descrição |
|---|---|---|
| `bronze.coletas_agua_raw` | `coletas_agua_rio_soberbo.csv` | Ingestão bruta das coletas de campo, sem tratamento de tipos |
| `bronze.chuva_raw` | Arquivos mensais CEMADEN (`data.csv`) | Ingestão bruta das leituras pluviométricas de todas as estações do arquivo, incluindo a estação "Rio Soberbo" |

### Silver — dado limpo e tipado

| Tabela | Descrição | Principais colunas |
|---|---|---|
| `silver.coletas_agua` | Coletas de água com tipos corrigidos (`double` para campos numéricos) e colunas de proveniência (`fonte_dado`, `data_ingestao`) | `data_coleta`, `condicao_tempo`, `cor`, `turbidez_qualitativa`, `temperatura_agua_c`, `temperatura_ar_c`, `od`, `ph_kit`, `ph_sonda`, `amonia`, `nitrato`, `nitrito`, `ortofosfato`, `turbidez`, `condutividade`, `salinidade`, `tds` |
| `silver.precipitacao_diaria` | Leituras da estação "Rio Soberbo" filtradas, com decimal convertido (`,`→`.`) e agregação diária (soma das leituras sub-horárias) | `data`, `precipitacao_mm`, `numero_leituras`, `cobertura_dados_chuva` (`"baixa"` se `numero_leituras < 5`, senão `"adequada"`) |

### Gold — dado pronto para análise

| Tabela | Descrição |
|---|---|
| `gold.fato_qualidade_agua` | Tabela fato resultante do join entre `silver.coletas_agua` e `silver.precipitacao_diaria` pela data, enriquecida com indicadores de contexto pluviométrico: chuva registrada no dia da coleta e `dias_desde_ultima_chuva` (calculado via função de janela sobre o histórico de precipitação), além de `numero_leituras` e `cobertura_dados_chuva` propagados da camada Silver |

Todas as tabelas e colunas foram documentadas via `COMMENT ON TABLE` e `ALTER TABLE ... ALTER COLUMN ... COMMENT` no Unity Catalog (ver [Evidências](#evidências-prints)).

> Para a lista completa e definitiva de colunas de cada tabela, consulte o Catalog Explorer do Databricks (prints em `/prints`) — este README resume o modelo conceitual.

---

## Pipeline de ETL

1. **Extração (Bronze):** upload dos arquivos CSV brutos (coletas de água e boletins mensais do CEMADEN) para Volumes do Unity Catalog; leitura via `spark.read.csv` e persistência como tabelas Delta (`saveAsTable`), preservando o dado como veio da fonte.
2. **Transformação (Silver):**
   - Conversão de tipos (`cast`) e padronização de decimais (`regexp_replace` para trocar `,` por `.` nos dados de chuva);
   - Filtragem da estação "Rio Soberbo" dentre as demais estações do CEMADEN;
   - Agregação da precipitação sub-horária em granularidade diária (`groupBy` + `agg`), com contagem de leituras por dia (`numero_leituras`) e classificação de cobertura (`cobertura_dados_chuva`);
   - Enriquecimento das coletas de água com colunas de proveniência (`fonte_dado`, `data_ingestao`).
3. **Carga (Gold):** join entre as tabelas Silver de qualidade da água e precipitação diária, com cálculo de `dias_desde_ultima_chuva` via função de janela (`Window.orderBy(...).rowsBetween(...)` com `last(..., ignorenulls=True)` e `datediff`), consolidando a tabela fato usada na análise.
4. **Correção e versionamento:** correções pontuais na fonte (ex.: troca de salinidade/TDS em uma coleta) foram aplicadas via recarga da camada Bronze com `overwriteSchema`, preservando o histórico de versões via `DESCRIBE HISTORY` (Delta Lake), garantindo rastreabilidade completa da correção.

---

## Qualidade de Dados

Nesta etapa, os dados das camadas de qualidade da água e precipitação foram avaliados quanto a completude, consistência, unicidade, acurácia e presença de outliers, conforme descrito a seguir.

### Completude

- **Sonda ausente na 1ª coleta (15/04/2026):** os campos `temperatura_agua_c`, `temperatura_ar_c`, `ph_sonda`, `condutividade`, `salinidade` e `tds` estão nulos nessa data porque o equipamento (sonda) ainda não estava disponível no início do projeto.
- **TDS ausente na coleta de 17/06/2026:** valor não registrado nessa data específica, sem relação com a ausência de equipamento (a sonda já estava em uso, com pH sonda, condutividade e salinidade presentes).
- **Turbidez, cor e condição climática ausentes até a 5ª coleta:** esses parâmetros foram incorporados ao protocolo apenas a partir da 6ª coleta (10/09/2026), refletindo a evolução natural de um projeto piloto. Para as coletas anteriores, `cor` e `turbidez_qualitativa` foram preenchidos retroativamente com base em observação de campo (não instrumental), e `turbidez` (NTU) permanece nula.
- **Lacunas na série de precipitação:** a estação CEMADEN "Rio Soberbo" não possui nenhuma leitura registrada para os dias 18/05/2026 e 17/06/2026 (datas que coincidem com coletas de água), resultando em valores nulos de `precipitacao_mm` para essas datas na camada Gold — distintos de dias com chuva zero efetivamente registrada.

### Consistência

- Os dados de precipitação vieram no formato brasileiro (separador decimal `,`), exigindo conversão para `.` antes da tipagem numérica.
- Os tipos de dado de todas as colunas foram validados e ajustados na camada Silver (ex.: `ph_kit` e `ortofosfato`, inicialmente inferidos como `integer`, foram convertidos para `double` para suportar valores decimais em coletas futuras).

### Unicidade

- Cada linha da tabela `coletas_agua` corresponde a uma data de coleta única; não foram identificadas duplicatas.
- Na precipitação, a agregação por dia (soma de leituras sub-horárias) garante uma linha por data, sem duplicidade.

### Acurácia

- **Erro de transcrição identificado e corrigido:** na coleta de 17/06/2026, os valores de salinidade e TDS haviam sido registrados trocados na fonte original. O erro foi identificado durante a análise, corrigido no CSV de origem, e a tabela Bronze foi recarregada (histórico de versões preservado via `DESCRIBE HISTORY` do Delta Lake).
- **Divergência entre observação de campo e dado de estação pública:** na 6ª coleta (10/09/2026), a condição climática foi registrada como "chuvoso" em campo, mas a estação CEMADEN teve apenas 1 leitura no dia (valor 0mm), classificada como cobertura **"baixa"** na coluna `cobertura_dados_chuva`. Essa divergência não foi corrigida artificialmente; em vez disso, foi tornada rastreável através das colunas `numero_leituras` e `cobertura_dados_chuva`, permitindo que análises futuras considerem ou excluam esse dado conforme a confiabilidade necessária.

### Outliers

- **Salinidade elevada em 10/08/2026 (valor 113, frente a 10,1 e 12,8 nas demais coletas):** inicialmente identificado como possível erro, foi posteriormente confirmado como valor real coincidente com um dia de chuva — reforçando a hipótese de que eventos pluviométricos alteram as características físico-químicas do rio, e sustentando a relevância da pergunta de negócio deste projeto.

### Conclusão

As limitações identificadas são típicas de um projeto de monitoramento ambiental em fase inicial (piloto), com protocolo em evolução e coleta realizada por alunos treinados. Nenhuma limitação impediu a persistência e modelagem dos dados; todas foram documentadas na origem (comentários de coluna no Unity Catalog) e refletidas em colunas específicas que tornam a confiabilidade de cada registro transparente para análises futuras.

---

## Análise Final

### Pergunta 1: Existe relação entre eventos de chuva e o aumento de turbidez ou alteração de outros parâmetros de qualidade da água?

Das seis amostragens realizadas, duas coincidiram com eventos pluviométricos na data exata da amostragem (10/08/2026 e 10/09/2026). Em 10/08/2026, registraram-se as máximas de salinidade (113) e de condutividade elétrica (35,1) de toda a série histórica, contrastando com o intervalo de 1,9 a 12,8 para salinidade e 2,8 a 32,5 para condutividade observados nos períodos secos. Tal comportamento sugere que a precipitação direta alterou temporariamente o equilíbrio físico-químico do corpo hídrico. Similarmente, na amostragem de 10/09/2026, sob ocorrência de chuva, o pH (kit) apresentou o valor mínimo da série (5,0, em comparação ao padrão de 7,0 das demais datas), momento em que também se obteve a primeira aferição instrumental de turbidez, registrando 25 NTU.

Nas amostragens de 15/04/2026 e 02/07/2026, o indicador `dias_desde_ultima_chuva` apontou precipitações ocorridas três e cinco dias antes da coleta, respectivamente. Nesses cenários, os parâmetros físico-químicos mantiveram-se dentro da faixa de variação normal das amostragens secas, sem a presença dos outliers identificados em 10/08 e 10/09. Essa evidência indica que a imediatez temporal do evento pluvial possui maior relevância sobre a qualidade da água do que a ocorrência de chuvas em dias antecedentes, restringindo as alterações mais expressivas aos dias de precipitação concomitante à coleta.

Esses indícios são consistentes com a hipótese de que eventos de chuva impactam a qualidade da água do Rio Soberbo; no entanto, a robustez estatística dessa conclusão é limitada por uma amostra de somente 2 dias de chuva registrados — reforçando a importância da continuidade do projeto ao longo dos 3 anos previstos para a coleta.

### Pergunta 2: Os parâmetros de qualidade da água se mantêm dentro de padrões esperados ao longo do período monitorado?

Em linhas gerais, os parâmetros analisados mantiveram-se dentro de padrões compatíveis com um corpo hídrico saudável ao longo do período monitorado. O pH manteve-se predominantemente neutro (6,0 a 7,9, a depender do instrumento) ao longo do período, com exceção da coleta de 10/09 (pH 5). O oxigênio dissolvido variou entre 5,0 e 9,0, dentro de faixas aceitáveis para a saúde do corpo hídrico. Amônia, nitrito e ortofosfato permaneceram próximos de zero em todas as coletas, indicando ausência de poluição orgânica significativa no período. A cor da água foi registrada como incolor em todas as amostragens, e a avaliação de transparência — qualitativa (disco de Secchi) ou instrumental (turbidímetro) — indicou água visualmente límpida na maior parte do tempo monitorado.

### Pergunta 3: Quais dias de coleta coincidem com períodos de chuva, e o que isso sugere?

Entre as seis amostragens realizadas, duas registraram precipitação na data exata da coleta (10/08/2026 e 10/09/2026) e duas (15/04/2026 e 02/07/2026) apresentaram chuva recente — três e cinco dias antes, respectivamente. Para as coletas de 18/05/2026 e 17/06/2026, não foi possível determinar a ocorrência de chuva no período antecedente, em razão da ausência de leituras da estação pluviométrica CEMADEN nesses intervalos — limitação já discutida na seção de Qualidade de Dados.

Cabe ressaltar, ainda, que o dado de precipitação da coleta de 10/09/2026 apresentou baixa cobertura de leituras na estação (apenas um registro no dia), de modo que a classificação de "dia de chuva" para essa data fundamenta-se na observação de campo, e não no dado quantitativo da estação pública.

Esse padrão sugere que o desenho atual de coleta — realizado em calendário fixo, não condicionado à ocorrência de chuva — captura eventos pluviométricos de maneira incidental, resultando em contraste limitado entre condições secas e chuvosas dentro da amostra. Isso evidencia a necessidade de um planejamento mais estratégico do calendário de coleta do projeto: agendar parte das coletas já previstas para dias de chuva, em vez de seguir apenas uma periodicidade fixa, enriqueceria a compreensão do comportamento do rio nesses eventos, com valor tanto para o monitoramento ambiental do projeto quanto para sustentar, no futuro, um modelo preditivo mais robusto.

Adicionalmente, as lacunas identificadas na série da estação CEMADEN — ausência total de leituras em determinados dias e cobertura insuficiente em outros — são consistentes com limitações conhecidas de redes automatizadas de baixo custo. Vale registrar que, antes de adotar a CEMADEN, buscou-se também a base da ANA (Agência Nacional de Águas), cuja estação mais próxima retornou arquivos sem dados disponíveis para o período — confirmando que a limitação de cobertura climática nesta região não é exclusiva de uma única fonte. Isso não invalida o uso da CEMADEN como fonte provisória neste MVP, mas reforça o valor de, no futuro, contar com a estação meteorológica própria do projeto (atualmente em processo de reinstalação) como fonte primária de dados climáticos, com maior controle sobre a qualidade e continuidade das leituras.

---

## Autoavaliação

### Objetivos Alcançados

Os objetivos definidos na etapa inicial deste MVP foram integralmente atingidos: foi construído um pipeline de dados completo, cobrindo desde a definição do problema até a análise final, seguindo a arquitetura medalhão (Bronze, Silver e Gold) na plataforma Databricks. As três perguntas de negócio formuladas foram respondidas com base nos dados disponíveis, e a documentação de linhagem, catálogo de dados e qualidade foi mantida ao longo de todo o processo.

### Dificuldades Encontradas

A principal dificuldade foi o volume reduzido de dados disponíveis: apenas seis coletas de qualidade da água foram realizadas até o momento, o que limita a robustez estatística das inferências apresentadas. Também foram identificadas lacunas significativas na série de precipitação de fontes públicas — tanto na ANA, cuja estação mais próxima não retornou dados para o período, quanto na CEMADEN, cuja série apresentou dias sem nenhuma leitura registrada e outros com cobertura insuficiente.

Também foi identificado, durante o processo, um erro de transcrição nos dados de campo (troca entre os valores de salinidade e TDS em uma das coletas), corrigido a partir do reprocessamento da camada Bronze com preservação do histórico de versões via Delta Lake.

### O que não foi possível atingir

Não foi possível estabelecer uma relação estatisticamente significativa entre eventos de chuva e alteração nos parâmetros de qualidade da água, dado o tamanho reduzido da amostra (apenas duas coletas coincidiram com chuva no dia exato). As tendências observadas são consistentes com a hipótese de impacto, mas não permitem generalização.

### Trabalhos Futuros

Este MVP estabelece a infraestrutura de dados (pipeline Bronze/Silver/Gold) que servirá de base para etapas futuras de análise mais aprofundada sobre a relação entre eventos de chuva e qualidade da água no Rio Soberbo. Visto que o componente de monitoramento do projeto Conexão Floresta e Clima tem duração planejada de três anos, espera-se que a base de dados cresça substancialmente, fortalecendo tanto a consistência estatística das análises quanto a confiabilidade de um futuro modelo preditivo destinado à antecipação de eventos de chuva e possíveis fechamentos de abastecimento no Rio Soberbo.

Entre os refinamentos identificados para ciclos futuros do projeto, destacam-se: (i) um planejamento mais estratégico do calendário de coleta, contemplando dias de chuva de forma deliberada; e (ii) a substituição da estação pluviométrica pública (CEMADEN) pela estação meteorológica própria do projeto, o que deverá reduzir a dependência de fontes externas com cobertura irregular.

Adicionalmente, os dados e a infraestrutura construídos neste MVP têm potencial de uso para além do escopo acadêmico: a coordenação da ONG Onda Verde manifestou interesse em uma publicação científica em coautoria a partir dos resultados deste monitoramento, o que reforça o valor do rigor metodológico mantido ao longo deste trabalho.

---

## Evidências (prints)

Os prints das seguintes etapas estão disponíveis na pasta `/prints` deste repositório:

- Criação do catálogo, schemas e volumes (Unity Catalog)
- Overview e Sample Data das tabelas Bronze, Silver e Gold
- Comentários de tabela e de coluna (documentação do catálogo)
- `DESCRIBE HISTORY` da tabela corrigida (versionamento Delta Lake)
- Execução das células do notebook (pipeline completo)

---

## Estrutura do repositório

```
.
├── README.md                          # este arquivo
├── notebook/
│   └── conexao_notebook.py            # notebook exportado do Databricks, com todo o pipeline
├── data/
│   └── coletas_agua_rio_soberbo.csv   # dado bruto de qualidade da água (fonte original)
├── fotos_coleta/                      # fotos do processo de coleta em campo
├── glossario.md                       # glossário de termos técnicos (qualidade da água + engenharia de dados)
└── prints/                            # evidências em imagem de cada etapa do pipeline
```
