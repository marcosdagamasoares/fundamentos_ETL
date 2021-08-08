#  Desenvolvimento do projeto ETL - Extração e validação
## Realizando a transformação de dados 

import pandas as pd
import pandera as pa

dataframe = pd.read_csv("/cursos/bootcamp/fundamentos de etl/ocorrencia_2010_2020.csv", sep=';')
dataframe.head(2)

df = pd.read_csv("ocorrencia_2010_2020.csv", sep=';')
df.head(2)  retorna 2 linhas

df = pd.read_csv("ocorrencia_2010_2020.csv", sep=';')
df  retorna todas as linhas

parse_date - fazer o parser do campo que está como objeto para data
ocorrencia_dia passa a ser datetime64[ns]
df = pd.read_csv("ocorrencia_2010_2020.csv", sep=';', parse_dates=['ocorrencia_dia'])
df.head(2)

## VISUALIZAR OS TIPO DE DADOS
ocorrecia_dia:  está como objeto mas é data, com o comando acima agora é do tipo Data
df.dtypes

#### **Acesso o campo “ocorrencia_data”**
df.ocorrencia_dia

**EXTRAÇÃO DO MÊS DE UMA DATA**
df.OCORRENCIA_DIA.dt.month

**Como validar a extração de dados
tail: traz os últimos registros
Campo data vazia, vira como: NaT
Se campo texto vazio: NaN**

df.tail(2)

**VALIDAÇÃO DOS TIPOS DE DADOS
Os CAMPOS objects são texto, não há problema trabalhar com objects
Se o campo (ocorrencia_dia) estiver (03/50/2010) ao verificar o tipo 
retornará como objeto, mesmo fazendo parse_dates=['ocorrencia_dia']  o parse não 
é feito e não terei a validação.**

df.dtypes

**Para não ocorrer o problema acima referente a data, USAR a BIBLIOTECA ( pandera ) - import pandera as pa
que permite montar um esquema de validação deste dataframe, esse esquema vai validar se a coluna 
( ocorrencia_dia ) é do tipo ( data ) se não for do tipo data o processo irá parar, ocorrerá uma 
Exceção.**

**Iniciar essa sequencia de código é importante, caso execute todos os códigos do notebook, essa sequencia
garante que as dependências de cada função sejam satisfeitas**

df = pd.read_csv("ocorrencia_2010_2020.csv", sep=';', parse_dates=['ocorrencia_dia'], dayfirst=True)
df.head(5)

**MONTAR O ESQUEMA
A função DataFrameSchema(): traz o schema**

schema = pa.DataFrameSchema(
        columns = {
            "codigo_ocorrencia":pa.Column(pa.Int)
        }
)

**validate(): VALIDAÇÃO DO DATAFRAME
A EXECUÇÃO NORMAL DO validate() significa foi validado mediante a coluna ( codigo_ocorrencia )
e ESSA COLUNA POSSUI DADOS DO TIPO INTEIRO**

schema.validate(df)

**“Colunas”: Validação das Colunas 
Cada coluna com seu tipo de dado ocorrencia_dia do tipo (string) futuramente colocar verificação de formato de hora para 24 hs utilizando expressão regular**

schema = pa.DataFrameSchema(
    columns = {
        "codigo_ocorrencia": pa.Column(pa.Int),
        "codigo_ocorrencia2": pa.Column(pa.Int),
        "ocorrencia_classificacao": pa.Column(pa.String),
        "ocorrencia_cidade": pa.Column(pa.String),
        "ocorrencia_uf": pa.Column(pa.String),
        "ocorrencia_aerodromo": pa.Column(pa.String),
        "ocorrencia_dia": pa.Column(pa.DateTime),
        "ocorrencia_hora": pa.Column(pa.String, nullable=True),
        "total_recomendacoes": pa.Column(pa.Int) 
    }
)

**VALIDAR O DATAFRAME COM BASE NO NOVO SCHEMA
Dará erro: SchemaError: non-nullable series 'ocorrencia_hora' contains null values: {4100: nan}
nullable=True - Ignora campo vazio**

schema.validate(df)

**Como validar a extração de dados 
VERIFICAR EXPRESSÕES REGULARES
VALIDAÇÃO DA COLUNA ocorrencia_hora, seu tipo de dados é "string" 
Estabelecer um formato de Hora:Minuto:Segundo
Usar o recurso de restrição através de expressão regular**

schema = pa.DataFrameSchema(
    columns = {
        "codigo_ocorrencia": pa.Column(pa.Int),
        "codigo_ocorrencia2": pa.Column(pa.Int),
        "ocorrencia_classificacao": pa.Column(pa.String),
        "ocorrencia_cidade": pa.Column(pa.String),
        "ocorrencia_uf": pa.Column(pa.String),
        "ocorrencia_aerodromo": pa.Column(pa.String),
        "ocorrencia_dia": pa.Column(pa.DateTime),
        "ocorrencia_hora": pa.Column(pa.String, pa.Check.str_matches(r'^([0-1]?[0-9]|[2][0-3]):([0-5][0-9])(:[0-5][0-9])?$'),  					nullable=True),
        "total_recomendacoes": pa.Column(pa.Int) 
    }
)

**VALIDAR O DATAFRAME COM BASE NAS ALTERAÇÕES DO schema**
schema.validate(df)

**TRATAMENTO DO TAMANHO DO DADO CAMPO ocorrencia_uf máximo e minimo caracteres ou nulo 
Usar: lenght(2,2) - tamanho mínimo e máximo**

schema = pa.DataFrameSchema(
    columns = {
        "codigo_ocorrencia": pa.Column(pa.Int),
        "codigo_ocorrencia2": pa.Column(pa.Int),
        "ocorrencia_classificacao": pa.Column(pa.String),
        "ocorrencia_cidade": pa.Column(pa.String),        
        "ocorrencia_uf": pa.Column(pa.String, pa.Check.str_length(2,2)),        
        "ocorrencia_aerodromo": pa.Column(pa.String),
        "ocorrencia_dia": pa.Column(pa.DateTime),
        "ocorrencia_hora": pa.Column(pa.String, pa.Check.str_matches(r'^([0-1]?[0-9]|[2][0-3]):([0-5][0-9])(:[0-5][0-9])?$'), 		nullable=True),
        "total_recomendacoes": pa.Column(pa.Int) 
    }
)

**VALIDAR**
schema.validate(df)

**Desenvolvimento do projeto ETL - Limpeza
Como a limpeza de dados será realizada
Criar novo notebook
Nome: limpeza**

import pandas as pd
df = pd.read_csv("ocorrencia_2010_2020.csv", sep=';', parse_dates=['ocorrencia_dia"],	dayfirst=True)
df.head(5)

codigo_ocorrencia	codigo_ocorrencia1	codigo_ocorrencia2 codigo_ocorrenciaB	codigo_ocorrencia4	ocorrencia_classificacao
0	40211	40211	40211	40211	40211	INCIDENTE
1	40349	40349	40349	40349	40349	INCIDENTE


**Localizar na Linha 1 e coluna (ocorrencia_cidade )**
df.loc[l, 'ocorrencia_cidade']
'BELEM'

**PEGAR OS DADOS DE TODA UMA LINHA, POR EXEMPLO LINHA 3**
def.loc[indice da linha]

df.loc[3]
codigo_ocorrencia	39527
codigo_ocorrencia1	39527
codigo_ocorrencia2	39527
codigo_ocorrencia3	39527
codigo_ocorrencia4	39527

**PEGAR OS DADOS DA LINHA1 ATE A LINHA 3**
df.loc[1:3]

codigo_ocorrencia codigo_ocorrencia1	codigo_ocorrencia2	codigo_ocorrenciaB	codigo_ocorrencia4	ocorrencia_classificacao
1	40349	40349	40349	40349	40349	INCIDENTE
2	40351	40351	40351	40351	40351	INCIDENTE
3	39527	39527	39527	39527	39527	ACIDENTE


**TRAZER DADOS DA LINHA 10 e 40 (sem informar a coluna)**
df.loc[[10,40]]

**codigo_ocorrencia codigo_ocorrencia1 codigo_ocorrencia2 codigo_ocorrencia3**
10	39789	39789	39789	39789
40	39158	39158	39158	39158


**PEGAR TODOS OS DADOS DA COLUNA SEM INFORMA AS LINHAS**
df.loc[:. Nome da Coluna]
df.loc[:,'ocorrencia_cidade']
0	RIO DE JANEIRO
1	BELEM


**REALIZANDO A LIMPEZA DOS DADOS - PARTE 2
VERFICAR SE 0 CAMPO TEM VALORES ÚNICOS PARA LINHAS DIFERENTES**
df.codigo_ocorrencia.is_unique
True

**Como é unico vou utiliza-lo como indice no DataFrame, no codigo abaixo nao atualizo o DataFrame**
Nao fiz alteração DataFrame

df.set_index('codigo_ocorrencia')
codigo_ocorrencia1 codigo_ocorrencia2 codigo_ocorrencia3 codigo_ocorrencia4 ocorrencia_classificacao ocorrencia_latitude
codigo_ocorrencia
40211	40211	40211	40211	40211	INCIDENTE
40349	40349	40349	40349	40349	INCIDENTE	Nan


REALIZANDO A LIMPEZA DE DADOS - PARTE 3
EFETUAR A LIMPEZA - LIMPAR OS DADOS NÃO INF0RMADOS 
ocorrencia_uf
**
ocorrencia_aerodromo
****
*****
####


**LIMPAR TODAS LINHAS QUE TEM **
NA -not available - atribuir valores que nao estao disponiveis na celula.**
df.loc[df.ocorrencia_aerodromo == '****'> ['ocorrencia_aerodromo']] = pd.NA

df.head(5)
codigo_ocorrencia	codigo_ocorrencia1	codigo_ocorrencia2	codigo_ocorrencia3	codigo_ocorrencia4	ocorrencia_classificacao	ocorrenciajatitude
0 40211	40211	40211	40211	40211	INCIDENTE	<NA>
1 40349	40349	40349	40349	40349	INCIDENTE	NaN
2 40351	40351	40351	40351	40351	INCIDENTE	NaN
3 39527	39527	39527	39527	39527	ACIDENTE	-13.1066666667
4 40324	40324	40324	40324	40324	INCIDENTE	NaN

**Leitura de arquivo csv (read_csv)**
https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_

**Validação: Pandera**
https://pandera.readthedocs.io/en/stable/

**Limpeza: Valores ausentes**
https://pandas.pydata.org/pandas-docs/stable/user_guide/missing_data.html

**Transformação: Variações de filtros (tempo execução)**
https://medium.com/data-hackers/a-maneira-eficiente-de-filtrar-um-data-frame-pandas-4158a4eB7clO
