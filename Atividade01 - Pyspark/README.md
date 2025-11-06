# Resoluções dos exercícios — Atividade 01 (PySpark)

Este arquivo contém as resoluções dos exercícios presentes no notebook `Atividade1_Pyspark.ipynb`.

## Sumário

- Exercício 1.1 — Leitura do arquivo `fut_players` e primeiras 5 linhas
- Exercício 1.2 — Jogadores "The Bests" (dribbling > 90 e shooting > 90) e nacionalidade
- Exercício 2 — País com melhor overall médio (coluna `avg_overall`)
- Exercício 2.1 — Classificação dos jogadores por faixa de `overall`
- Desafio — Montando o Time dos Sonhos do Brasil (4-4-2) e Desafio Bônus (sem duplicatas)

> Observação: estes trechos devem ser executados em um ambiente com Spark disponível e com as tabelas:
> - `workspace.bronze.metal_bands`
> - `workspace.bronze.fut_players_data`
> - `workspace.bronze.pokemon_data`

> Obervação 2: Ao dar upload nos datasets, criei 3 schemas diferentes seguindo a arquitetura medalhão: bronze, silver e gold.

---

## Exercício 1.1 — Leitura do arquivo fut_players e 5 primeiras linhas

Código:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("AtividadePraticaSpark").getOrCreate()

fut_players = spark.table("workspace.bronze.fut_players_data")

display(fut_players.limit(5))
```

---

## Exercício 1.2 — Jogadores "The Bests" e nacionalidade

Requisitos: jogadores com `dribbling > 90` e `shooting > 90`. Fazer join para obter a nacionalidade.

Código:

```python
the_best = fut_players.where(
	(F.col('dribbling') > 90) &
	(F.col('shooting') > 90)
)

nationalities = fut_players.select('player_id', 'player_name', 'nationality')

# left join para manter os jogadores de the_best e trazer suas nacionalidades
the_best_nationality = the_best.join(
	nationalities,
	on='player_id',
	how='left'
)

the_best_nationality.select(
	'player_id', 'player_name', 'nationality', 'position', 'dribbling', 'shooting', 'overall'
).display()
```

---

## Exercício 2 — País com melhor overall médio

Requisitos: Crie a coluna `avg_overall` agrupando por `nationality`.

Código:

```python
country_avg_overall = (
	fut_players
	.groupBy("nationality")
	.agg(
		F.avg(F.col("overall")).alias("avg_overall")
	)
)

# nacionalidade com maior overall médio
melhor = (
	country_avg_overall
	.orderBy(F.col("avg_overall").desc())
	.limit(1)
	.collect()[0]
)

# overall médio do Brasil
brasil = (
	country_avg_overall
	.filter(F.col("nationality") == "Brazil")
	.collect()[0]
)

display({
	"Melhor overall médio": f"{melhor['nationality']}: {melhor['avg_overall']:.2f}",
	"Overall médio do Brasil": round(brasil['avg_overall'], 2)
})
```

---

## Exercício 2.1 — Classificação por faixa de overall

Classificação solicitada:

- <= 50 -> "Amador"
- 51-60 -> "Ruim"
- 61-70 -> "Ok"
- 71-80 -> "Bom"
- 81-90 -> "Ótimo"
- 91+ -> "Lenda"

Código:

```python
fut_players = spark.table("workspace.bronze.fut_players_data")

fut_players_classification = fut_players.withColumn(
	"classification",
	F.when(F.col("overall") <= 50, "Amador")
	 .when(F.col("overall") <= 60, "Ruim")
	 .when(F.col("overall") <= 70, "Ok")
	 .when(F.col("overall") <= 80, "Bom")
	 .when(F.col("overall") <= 90, "Ótimo")
	 .otherwise("Lenda")
)

fut_players_classification.groupBy("classification").count().orderBy("count", ascending=False).display()
```

---

## Desafio — Montando o Time dos Sonhos do Brasil (formação 4-4-2)

Requisitos: agrupar posições em `Goleiro`, `Defesa`, `Meio` e `Ataque`, ordenar por `overall` e selecionar os melhores por vaga (1 Goleiro, 4 Defesa, 4 Meio, 2 Ataque).

Implementação (sem tratar duplicatas de cartas do mesmo jogador):

```python
fut_players = spark.table("workspace.bronze.fut_players_data")

gk = (
	fut_players.filter(F.col("position") == "GK")
	.select("nationality", "position", "player_name", "overall")
	.orderBy(F.col("overall").desc())
	.withColumn("position_group", F.lit("Goleiro"))
)

defense = (
	fut_players.filter(F.col("position").isin(["CB", "RB", "LB", "RWB", "LWB"]))
	.select("nationality", "position", "player_name", "overall")
	.orderBy(F.col("overall").desc())
	.withColumn("position_group", F.lit("Defesa"))
)

midfield = (
	fut_players.filter(F.col("position").isin(["CDM", "CM", "RM", "LM", "CAM"]))
	.select("nationality", "position", "player_name", "overall")
	.orderBy(F.col("overall").desc())
	.withColumn("position_group", F.lit("Meio"))
)

attack = (
	fut_players.filter(F.col("position").isin(["ST", "CF", "LW", "RW", "RF", "LF"]))
	.select("nationality", "position", "player_name", "overall")
	.orderBy(F.col("overall").desc())
	.withColumn("position_group", F.lit("Ataque"))
)

team = (
	gk.limit(1)
	.unionByName(defense.limit(4))
	.unionByName(midfield.limit(4))
	.unionByName(attack.limit(2))
)

display(team)
```

### Desafio Bônus — evitar duplicatas de jogador (escolher a carta com maior overall)

Passo a passo do que eu fiz:

1. Ordenei por `overall` decrescente;
2. Removi duplicatas por `player_name` usando `dropDuplicates` (mantendo a carta com maior overall);
3. Reapliquei a lógica de seleção por posição.

Exemplo de implementação:

```python
fut_without_duplicates = (
	fut_players
	.orderBy(F.col("overall").desc())
	.dropDuplicates(["player_name"])
)

# após isso, eu repeti o mesmo processo de filtragem por grupos e união (gk/defense/midfield/attack)

display(fut_without_duplicates.limit(5))
```

---

## Observações finais

- Estes blocos de código foram extraídos do notebook `Atividade1_Pyspark.ipynb` e adaptados para serem executados diretamente em um ambiente Spark com as tabelas já carregadas.
