########################### BIBLIOTECAS ##############################
import pandas as pd
import requests
from prefect import flow, task
import numpy as np
import pycountry
from pycountry_convert import country_alpha2_to_continent_code, convert_continent_code_to_continent_name

##########################################################################################################################
##########################################################################################################################
###################################################### Entregavél 1 ######################################################
##########################################################################################################################
##########################################################################################################################

# Carregamento do CSV original
df = pd.read_csv("Impact_of_Remote_Work_on_Mental_Health.csv")

# Mostrar info geral do dataset
print("Dimensões:", df.shape)
print("Colunas:", df.columns.tolist())
print(df.head())

# Salvar uma amostra para inspeção inicial
df.head(20).to_csv("amostra_dados.csv", index=False)

###########################2.1.1 Script de Extração (incluindo chamada à API)###########################

# Lista de códigos de indicadores da WHO a extrair
apis_who = [
    "WHOSIS_000015",
    "MH_1",# mental health policy
    "MH_3", # mental health lesgilation
    "MH_6",#psiquiatras em mental hospitals
    "MH_7",#  nurses in mental hospital
    "MH_9", # Psicologos em mental hospital
    "MH_16", # Beds in mental hospital
    "MH_19" #Admissoes em hospital
]

@task
def carregar_csv():
    return pd.read_csv('Impact_of_Remote_Work_on_Mental_Health.csv')


@task
def extrair_api_por_codigo(codigo: str) -> pd.DataFrame:
    url = f"https://ghoapi.azureedge.net/api/{codigo}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()["value"]

        # Lista para armazenar os dicionários de dados
        dados_para_df = []
        for d in data:
            # Adicionar "FactValueNumeric" à lista de campos extraídos
            # e também garantir que outras colunas relevantes sejam incluídas
            record = {
                "codigo": codigo,
                "country": d.get("SpatialDim"),
                "year": d.get("TimeDim"),
                "sex": d.get("Dim1"),
                "dim2": d.get("Dim2"),  # Manter outras colunas que possam ser úteis
                "dim3": d.get("Dim3"),  # Manter outras colunas que possam ser úteis
                "value_type": d.get("ValueType"),  # Manter outras colunas que possam ser úteis
                "source": d.get("DataSourceDim"),  # Manter outras colunas que possam ser úteis
                "value": d.get("Value"),  # Coluna original com o texto
            }
            # Adicionar apenas se houver algum valor (Value ou FactValueNumeric)
            # para evitar linhas completamente vazias para um indicador/ano/país
            if d.get("Value") is not None or d.get("FactValueNumeric") is not None:
                dados_para_df.append(record)

        return pd.DataFrame(dados_para_df)
    else:
        print(f" Erro ao consultar API: {codigo} ({response.status_code})")
        return pd.DataFrame()

@flow
def fluxo_extracao_todos_codigos():
    codigos = [
        "WHOSIS_000015", "MH_1", "MH_3", "MH_6", "MH_7", "MH_9", "MH_16", "MH_19"
    ]

    dfs_api = []
    for codigo in codigos:
        df_codigo = extrair_api_por_codigo(codigo)
        if not df_codigo.empty:
            dfs_api.append(df_codigo)

    if dfs_api:
        df_final = pd.concat(dfs_api)
        df_final.to_csv("todos_dados_who.csv", index=False)
        print(" Todos os dados da WHO foram extraídos e salvos.")
    else:
        print(" Nenhum dado extraído.")


if __name__ == "__main__":
    fluxo_extracao_todos_codigos()

##########################################################################################################################
##########################################################################################################################
###################################################### Entregavél 2 ######################################################
##########################################################################################################################
##########################################################################################################################

# 0. Carregar corretamente o CSV
df = pd.read_csv("Impact_of_Remote_Work_on_Mental_Health.csv", na_values=[], keep_default_na=False)

# Dados da API OMS (já extraídos anteriormente)
df_api = pd.read_csv("dados_suicidio_api.csv")

###################################################### Limpeza dos dados ######################################################

# 1. Verificar os tipos de dados
print(" Tipos de dados por coluna:")
print(df.dtypes)
print("\n")

# 2. Verificar valores nulos
print(" Quantidade de valores nulos por coluna:")
print(df.isnull().sum())
print("\n")

# 3. Verificar duplicados
duplicados = df.duplicated().sum()
print(f" Número total de registos duplicados: {duplicados}")
print("\n")

# 4. Verificar valores únicos por coluna
print(" Número de valores únicos por coluna:")
print(df.nunique())
print("\n")

# 5. Estatísticas descritivas das colunas numéricas e categóricas
print(" Estatísticas descritivas gerais:")
print(df.describe(include='all'))
print("\n")

# 6. Verificar valores infinitos nas colunas numéricas
valores_infinitos = np.isinf(df.select_dtypes(include=[np.number])).sum()
print(" Quantidade de valores infinitos por coluna numérica:")
print(valores_infinitos)
print("\n")

# 7. Verificar valores negativos em colunas numéricas
valores_negativos = (df.select_dtypes(include=[np.number]) < 0).sum()
print(" Quantidade de valores negativos por coluna numérica:")
print(valores_negativos)
print("\n")

############################### Transformar os dados ###############################

# 8. Substituir "None" por valores descritivos nas colunas indicadas
df["Mental_Health_Condition"] = df["Mental_Health_Condition"].replace("None", "Nenhuma")
df["Physical_Activity"] = df["Physical_Activity"].replace("None", "Não faz")

# 9. Confirmar substituições únicas
print(" Valores únicos em 'Mental_Health_Condition':")
print(df["Mental_Health_Condition"].unique())
print("\n")

print(" Valores únicos em 'Physical_Activity':")
print(df["Physical_Activity"].unique())
print("\n")

############################### Normalizar os dados ###############################

df["Work_Life_Balance_Norm"] = df["Work_Life_Balance_Rating"] / 5
df["Social_Isolation_Norm"] = df["Social_Isolation_Rating"] / 5
df["Company_Support_Norm"] = df["Company_Support_for_Remote_Work"] / 5

#Guardar os dados limpos num ficheiro csv
df.to_csv("dados_transformados.csv", index=False)

###################################################### Merge dos dados ######################################################
try:
    df_who_para_mapeamento = pd.read_csv("todos_dados_who.csv")
except FileNotFoundError:
    print("Erro: O ficheiro 'todos_dados_who.csv' não foi encontrado. Execute o fluxo de extração primeiro.")
    # Considerar adicionar um sys.exit() ou levantar uma exceção se este ficheiro for crucial
    df_who_para_mapeamento = pd.DataFrame(columns=['country']) # DataFrame vazio para evitar erros subsequentes


# Mapeamento manual de alguns países mais comuns (adiciona mais conforme necessário)
# Mapeamento manual de países ISO para regiões
# Obter os valores únicos das regiões e países
regioes_main = df["Region"].dropna().unique()
paises_api = df_api["country"].dropna().unique()

# Criar o dicionário de mapeamento de códigos ISO para regiões
# Inicialmente vazio
iso_para_regiao_completo = {}

# Mapeamento manual inicial (será expandido automaticamente abaixo)
mapeamento_inicial = {
    "Europe": [],
    "North America": [],
    "Asia": [],
    "Africa": [],
    "South America": [],
    "Oceania": []
}

def mapear_regiao(iso3):
    try:
        # Converter ISO-3 para ISO-2 (ex: 'FRA' → 'FR')
        alpha2 = pycountry.countries.get(alpha_3=iso3).alpha_2
        cont_code = country_alpha2_to_continent_code(alpha2)
        return convert_continent_code_to_continent_name(cont_code)
    except:
        return None  # Se não conseguir mapear, retorna None

# Obter os códigos únicos dos países de TODOS os dados da WHO
if not df_who_para_mapeamento.empty and 'country' in df_who_para_mapeamento.columns:
    paises_todos_who = df_who_para_mapeamento["country"].dropna().unique()
else:
    paises_todos_who = []
    if 'country' not in df_who_para_mapeamento.columns and not df_who_para_mapeamento.empty:
        print("Aviso: A coluna 'country' não foi encontrada em 'todos_dados_who.csv' para o mapeamento de regiões.")


# Obter os códigos únicos dos países da API
paises_api = df_api["country"].dropna().unique()

# Aplicar o mapeamento
iso_para_regiao_completo = {iso: mapear_regiao(iso) for iso in paises_todos_who} # <--- CORRIGIDO para usar paises_todos_who
# Opcional: verificar quais não foram mapeados
nao_mapeados = [iso for iso, regiao in iso_para_regiao_completo.items() if regiao is None]
print(f" Países não mapeados automaticamente: {len(nao_mapeados)}")
print(nao_mapeados[:20])  # Mostra os 20 primeiros

# Carregar dados extraídos da WHO
df_who = pd.read_csv("todos_dados_who.csv")

for codigo in df_who["codigo"].unique():
    df_temp = df_who[df_who["codigo"] == codigo].copy()
    df_temp["value"] = pd.to_numeric(df_temp["value"], errors="coerce")
    df_temp["Region"] = df_temp["country"].map(iso_para_regiao_completo)

    num_sem_regiao = df_temp["Region"].isna().sum()#
    print(f" {codigo} - Países sem região mapeada: {num_sem_regiao} de {len(df_temp)}")#
    print(df_temp[df_temp["Region"].isna()]["country"].unique()[:10])  # mostra os 10 primeiros#

    print(f" {codigo} - Regiões mapeadas:")#
    print(df_temp["Region"].unique())#

    print(" Códigos de país nos dados da OMS:")#
    print(df_who["country"].unique())#
################################################################ Metricas ##########################################################################



# print("\n Média de psicólogos por 100.000 habitantes por sexo e região (MH_9):")
# df_mh9 = df_who[df_who["codigo"] == "MH_9"].copy()
# df_mh9["value"] = pd.to_numeric(df_mh9["value"], errors="coerce")
# df_mh9["Region"] = df_mh9["country"].map(iso_para_regiao_completo)  # ADICIONA ISTO
# #media_psicologos = df_mh9.groupby(["Region", "sex"])["value"].mean().round(2).reset_index()
# media_psicologos = df_mh9.groupby(["Region"])["value"].mean().round(2).reset_index()
# print(media_psicologos)
# media_psicologos.to_csv("media_psicologos_por_sexo_regiao.csv", index=False)
# print(df_mh9["country"].unique())


print("\n[Métricas] - Frequência de condições de saúde mental:")
cond_freq = df["Mental_Health_Condition"].value_counts(normalize=True).round(3) * 100
print(cond_freq)

print("\n [Métricas] - Média de apoio da empresa por região:")
apoio_medio = df.groupby("Region")["Company_Support_for_Remote_Work"].mean().round(2)
print(apoio_medio)

print("\n[Métricas] - Correlação entre fatores de bem-estar:")
correlacoes = df[[
    "Work_Life_Balance_Norm",
    "Social_Isolation_Norm",
    "Company_Support_Norm"
]].corr().round(2)
print(correlacoes)

print("\n [Métricas] - Índice composto de bem-estar:")
df["Mental_Wellness_Index"] = (df["Work_Life_Balance_Norm"] +
                                (1 - df["Social_Isolation_Norm"]) +
                                    df["Company_Support_Norm"]) / 3

print(df[["Mental_Wellness_Index"]].describe().round(2))

print("\n [Métricas] - Índice médio por região:")
indice_medio_regiao = df.groupby("Region")["Mental_Wellness_Index"].mean().round(2)
print(indice_medio_regiao)

    # Se quiseres guardar como CSV
metricas_regionais = df.groupby("Region")[[
    "Work_Life_Balance_Norm",
    "Social_Isolation_Norm",
    "Company_Support_Norm",
    "Mental_Wellness_Index"
]].mean().round(2).reset_index()

#  Adicional: médias por sexo nos dados da OMS
df_who["value"] = pd.to_numeric(df_who["value"], errors="coerce")
df_who["Region"] = df_who["country"].map(iso_para_regiao_completo)

print("\n [Métricas] - Média dos indicadores da OMS por região e sexo:")
media_por_regiao_sexo = df_who.groupby(["Region", "sex", "codigo"])["value"].mean().reset_index()
print(media_por_regiao_sexo.head())

#  Adicional: evolução temporal
print("\n [Métricas] - Evolução anual dos indicadores da OMS (média global):")
evolucao_anual = df_who.groupby(["year", "codigo"])["value"].mean().reset_index()
print(evolucao_anual.head())
################################################################################
# --------------------------------------
# 🚨 AGREGAR MÉDIAS DA OMS POR REGIÃO 🚨
# --------------------------------------
df_who["value"] = pd.to_numeric(df_who["value"], errors="coerce")
df_who["Region"] = df_who["country"].map(iso_para_regiao_completo)

# Agrupar os indicadores da WHO por região
media_oms = df_who.groupby(["Region", "codigo"])["value"].mean().unstack().reset_index()

# Renomear colunas (conforme o SQL)
media_oms.columns.name = None
media_oms.rename(columns={
    "WHOSIS_000015": "Life expectancy at age 60 (years)",
    "MH_1": "MH_1_avg",
    "MH_3": "MH_3_avg",
    "MH_6": "MH_6_avg",
    "MH_7": "MH_7_avg",
    "MH_9": "MH_9_avg",
    "MH_16": "MH_16_avg",
    "MH_19": "MH_19_avg"
}, inplace=True)

# Juntar com os dados principais
df = pd.merge(df, media_oms, on="Region", how="left")

# Validar dimensões e colunas
print("\n✅ Dimensões finais do DataFrame:", df.shape)
print("✅ Colunas finais:", df.columns.tolist())
# Guardar resultado final
df.to_csv("dados_transformados_com_todas_apis.csv", index=False)
print("Merge com todos os dados da OMS concluído.")


# ------------------------------------------
# COMPLEMENTO: Tabelas adicionais para Entregável 2
# ------------------------------------------

# --- Tabela 1: Profissionais de saúde (MH_9)
df_mh9 = df_who[df_who["codigo"] == "MH_9"].copy()
df_mh9["value"] = pd.to_numeric(df_mh9["value"], errors="coerce")
df_mh9["Region"] = df_mh9["country"].map(iso_para_regiao_completo)

media_psicologos_g = df_mh9.groupby(["Region"])
media_psicologos = media_psicologos_g["value"].mean().round(2).reset_index()
media_psicologos.rename(columns={"value": "Avg_Psychologists_per_100k"}, inplace=True)
# media_psicologos.to_csv("tabela_profissionais_saude.csv", index=False)
# print("✅ Tabela de profissionais de saúde salva como 'tabela_profissionais_saude.csv'.")

# Psiquiatras (MH_6)
df_mh6 = df_who[df_who["codigo"] == "MH_6"].copy()
df_mh6["value"] = pd.to_numeric(df_mh6["value"], errors="coerce")
df_mh6["Region"] = df_mh6["country"].map(iso_para_regiao_completo)
media_psychiatrists = df_mh6.groupby("Region")["value"].mean().round(2).reset_index()
media_psychiatrists.rename(columns={"value": "Avg_Psychiatrists_per_100k"}, inplace=True)

# Enfermeiros (MH_7)
df_mh7 = df_who[df_who["codigo"] == "MH_7"].copy()
df_mh7["value"] = pd.to_numeric(df_mh7["value"], errors="coerce")
df_mh7["Region"] = df_mh7["country"].map(iso_para_regiao_completo)
media_nurses = df_mh7.groupby("Region")["value"].mean().round(2).reset_index()
media_nurses.rename(columns={"value": "Avg_Nurses_per_100k"}, inplace=True)

# Merge com metricas_regionais
# Primeiro, metricas_regionais com psicólogos (MH_9)
df_unificado = pd.merge(metricas_regionais, media_psicologos, on="Region", how="left")
# Depois, adicionar psiquiatras (MH_6)
df_unificado = pd.merge(df_unificado, media_psychiatrists, on="Region", how="left")
# Finalmente, adicionar enfermeiros (MH_7)
df_unificado = pd.merge(df_unificado, media_nurses, on="Region", how="left")

df_unificado.to_csv("dados_unificados_por_regiao.csv", index=False)
print("✅ Tabela unificada por região (com psicólogos, psiquiatras, enfermeiros) salva como 'dados_unificados_por_regiao.csv'.")


# --- Tabela 2: Profissionais de IT com dados estáticos
df_static = pd.read_csv("Impact_of_Remote_Work_on_Mental_Health.csv")
df_static["Region"] = df_static["Region"].fillna("Desconhecida")

prof_it = df_static.groupby(["Industry", "Region", "Mental_Health_Condition"]).agg({
    "Company_Support_for_Remote_Work": "mean",
    "Work_Life_Balance_Rating": "mean",
    "Social_Isolation_Rating": "mean"
}).round(2).reset_index()

prof_it.rename(columns={
    "Company_Support_for_Remote_Work": "Avg_Company_Support",
    "Work_Life_Balance_Rating": "Avg_Work_Life_Balance",
    "Social_Isolation_Rating": "Avg_Social_Isolation"
}, inplace=True)

prof_it.to_csv("tabela_profissionais_it.csv", index=False)
print("✅ Tabela de profissionais de IT salva como 'tabela_profissionais_it.csv'.")

# --------------------------------------
# Tabela 3: Indicadores MH_6, MH_7, MH_9 por País (ISO) – SEM MH_3
# --------------------------------------

# Parte 1: Indicadores MH_6, MH_7, MH_9 (numéricos) com ano incluído
codigos_numericos = ["MH_6", "MH_7", "MH_9"]
df_numericos = df_who[df_who["codigo"].isin(codigos_numericos)].copy()

# Garantir que o valor é numérico
df_numericos["value"] = pd.to_numeric(df_numericos["value"], errors="coerce")

# Garantir que o ano é inteiro
df_numericos["year"] = pd.to_numeric(df_numericos["year"], errors="coerce").astype("Int64")

# Agrupar por país, ano e código
media_numericos_ano = df_numericos.groupby(["country", "year", "codigo"])["value"].mean().unstack().reset_index()
media_numericos_ano.columns.name = None

# Renomear colunas
media_numericos_ano.rename(columns={
    "MH_6": "Avg_MH_6_PsychiatristsInMH",
    "MH_7": "Avg_MH_7_NursesInMH",
    "MH_9": "Avg_MH_9_PsychologistsInMH"
}, inplace=True)

# Exportar com ano incluído
media_numericos_ano.to_csv("tabela_indicadores_api_por_pais_ano.csv", index=False)
print("✅ Tabela com MH_6, MH_7, MH_9 por país e ano salva com sucesso.")


# --------------------------------------
# Tabela 4: Indicador MH_3 (Legislação em Saúde Mental) por País (ISO)
# --------------------------------------

# Carregar o ficheiro com todos os dados da WHO
df_who = pd.read_csv("todos_dados_who.csv")

# Filtrar apenas o indicador MH_3
df_mh3 = df_who[df_who["codigo"] == "MH_3"].copy()

# Manter apenas colunas relevantes
df_mh3 = df_mh3[["country", "year", "value"]]

# Limpar e normalizar os valores (Yes / No)
df_mh3["value_str"] = df_mh3["value"].astype(str).str.strip().str.lower()

# Converter para booleano: Yes → 1, No → 0, outros → None
df_mh3["MH_3_Legislation_Status"] = df_mh3["value_str"].apply(
    lambda x: 1 if x == "yes" else 0 if x == "no" else None
)

# Remover os inválidos (None)
df_mh3_clean = df_mh3.dropna(subset=["MH_3_Legislation_Status"]).copy()

# Converter ano para inteiro (caso esteja como string)
df_mh3_clean["year"] = df_mh3_clean["year"].astype(int)

# Agrupar por país e ano: 1 se houve pelo menos um "yes"
df_mh3_ano = df_mh3_clean.groupby(["country", "year"])["MH_3_Legislation_Status"].max().reset_index()

# Exportar como nova tabela
df_mh3_ano.to_csv("tabela_mh3_legislacao_por_pais_ano.csv", index=False)
print("✅ Tabela MH_3 com anos criada com sucesso.")


# --------------------------------------
# Tabela 5
# --------------------------------------
print("\n🔄 Processando Tabela 5: Esperança de Vida aos 60 (extraindo de 'value')...")

# 1. Filtrar indicador WHOSIS_000015 (Life expectancy at 60)
df_life60 = df_who[df_who["codigo"] == "WHOSIS_000015"].copy()
print(f"Linhas após filtrar por código WHOSIS_000015: {len(df_life60)}")

# 2. Extrair o primeiro número da coluna "value" e converter para numérico
#    Isto irá lidar com strings como "15.9 [15.3-16.8]" para obter 15.9
#    Se não houver um número no início da string, o resultado será NaN.
df_life60['extracted_value'] = df_life60['value'].astype(str).str.extract(r'^\s*(\d+\.?\d*)')[0]
df_life60["LifeExpectancyAt60"] = pd.to_numeric(df_life60["extracted_value"], errors="coerce")

# Remover linhas onde a extração/conversão para numérico falhou (resultando em NaN)
# Esta linha mantém o comportamento original de apenas incluir linhas com valores numéricos válidos.
df_life60.dropna(subset=["LifeExpectancyAt60"], inplace=True)
print(f"Linhas após extrair e converter 'value' e remover NaNs: {len(df_life60)}")

# 3. Adicionar Região (diretamente ao df_life60!)
#    Certifica-te que a coluna "country" em df_life60 contém os códigos ISO corretos
#    e que iso_para_regiao_completo está definido.
if "country" in df_life60.columns:
    df_life60["Region"] = df_life60["country"].map(iso_para_regiao_completo)
    df_life60["Region"] = df_life60["Region"].fillna("Desconhecida")
else:
    df_life60["Region"] = "Desconhecida"
    print("⚠️ Coluna 'country' não encontrada para mapear regiões.")


# 4. Selecionar colunas finais e remover NaNs essenciais (country, year)
#    LifeExpectancyAt60 já não deve ter NaNs devido ao dropna anterior.
colunas_finais = ["country", "year", "LifeExpectancyAt60", "Region"]
colunas_existentes_para_selecao = [col for col in colunas_finais if col in df_life60.columns]

# Verificar se as colunas essenciais para dropna existem
subset_dropna_final = []
if "country" in colunas_existentes_para_selecao:
    subset_dropna_final.append("country")
if "year" in colunas_existentes_para_selecao:
    subset_dropna_final.append("year")
# LifeExpectancyAt60 já foi tratada, mas podemos manter para consistência se a coluna existir
if "LifeExpectancyAt60" in colunas_existentes_para_selecao:
    subset_dropna_final.append("LifeExpectancyAt60")


if not df_life60.empty:
    df_life60_final = df_life60[colunas_existentes_para_selecao].copy()
    if subset_dropna_final:
        df_life60_final.dropna(subset=subset_dropna_final, inplace=True)
else:
    # Se df_life60 estiver vazio, cria um DataFrame final vazio com as colunas esperadas
    df_life60_final = pd.DataFrame(columns=colunas_existentes_para_selecao)

print(f"Linhas após selecionar colunas e remover NaNs de 'country'/'year': {len(df_life60_final)}")

# 5. Garantir ano como inteiro (depois de dropar NaNs no ano)
if not df_life60_final.empty and 'year' in df_life60_final.columns:
    # Tenta converter para Int64 para permitir NaNs se ainda existirem (embora não devam)
    df_life60_final["year"] = pd.to_numeric(df_life60_final["year"], errors='coerce').astype('Int64')

# 6. Exportar
output_filename_life60 = "tabela_life_expectancy_at_60.csv"
df_life60_final.to_csv(output_filename_life60, index=False)
print(f"✅ Tabela corrigida com Life Expectancy at 60 (extraído de 'value') gerada: {output_filename_life60} ({len(df_life60_final)} linhas)")


##########################################################################################################################
##########################################################################################################################
###################################################### Entregavél 3 ######################################################
##########################################################################################################################
##########################################################################################################################
import pandas as pd
import pyodbc
import numpy as np

# Conexão
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=CARLOTA_SANTOS\\SQLEXPRESS;'
    'DATABASE=Projetoo;'
    'UID=sa;'
    'PWD=sa;'
    'Encrypt=yes;'
    'TrustServerCertificate=yes;'
)
cursor = conn.cursor()

def preparar_df(path, colunas_float=None):
    df = pd.read_csv(path)
    if colunas_float:
        for col in colunas_float:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].astype(float)
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.dropna(subset=colunas_float)
    return df.where(pd.notnull(df), None)


# ----------------------- Tabela 1: dados_transformados_com_todas_apis -----------------------
colunas_float_1 = [
    'MH_1_avg', 'MH_3_avg', 'MH_6_avg', 'MH_7_avg', 'MH_9_avg',
    'MH_16_avg', 'MH_19_avg', 'Life expectancy at age 60 (years)',
    'Work_Life_Balance_Norm', 'Social_Isolation_Norm',
    'Company_Support_Norm', 'Mental_Wellness_Index'
]

df1 = preparar_df("dados_transformados_com_todas_apis.csv", colunas_float_1)

for idx, row in df1.iterrows():
    try:
        cursor.execute(f"""
            INSERT INTO dados_transformados VALUES ({','.join(['?' for _ in row])})
        """, tuple(row))
    except Exception as e:
        print(f"[dados_transformados] Erro na linha {idx}: {e}")


# ----------------------- Tabela 2: tabela_profissionais_it.csv -----------------------
df2 = preparar_df("tabela_profissionais_it.csv")

for idx, row in df2.iterrows():
    try:
        cursor.execute("""
            INSERT INTO profissionais_it_regionais VALUES (?,?,?,?,?,?)
        """, tuple(row))
    except Exception as e:
        print(f"[profissionais_it_regionais] Erro na linha {idx}: {e}")


# ----------------------- Tabela 3: dados_unificados_por_regiao.csv -----------------------
df3 = preparar_df("dados_unificados_por_regiao.csv")

for idx, row in df3.iterrows():
    try:
        cursor.execute("""
            INSERT INTO dados_unificados_por_regiao VALUES (?,?,?,?,?,?,?,?)
        """, tuple(row))
    except Exception as e:
        print(f"[dados_unificados_por_regiao] Erro na linha {idx}: {e}")


# ----------------------- Tabela 4: tabela_indicadores_api_por_pais_ano.csv -----------------------
df4 = pd.read_csv("tabela_indicadores_api_por_pais_ano.csv")

# Converter colunas para float com erro controlado
colunas_float = [
    'Avg_MH_6_PsychiatristsInMH',
    'Avg_MH_7_NursesInMH',
    'Avg_MH_9_PsychologistsInMH'
]

for col in colunas_float:
    df4[col] = pd.to_numeric(df4[col], errors='coerce').round(4)

# Eliminar linhas com float inválido (NaN)
df4 = df4.dropna(subset=colunas_float)

# Substituir outros NaNs por None
df4 = df4.where(pd.notnull(df4), None)

# Inserir
for idx, row in df4.iterrows():
    try:
        placeholders = ','.join(['?'] * len(row))
        query = f"INSERT INTO indicadores_api_por_pais_ano VALUES ({placeholders})"
        cursor.execute(query, tuple(row))
    except Exception as e:
        print(f"[indicadores_api_por_pais_ano] Erro na linha {idx}: {e}")
        print(row)



# ----------------------- Tabela 5: tabela_mh3_legislacao_por_pais_ano.csv -----------------------
df5 = preparar_df("tabela_mh3_legislacao_por_pais_ano.csv")

for idx, row in df5.iterrows():
    try:
        cursor.execute("""
            INSERT INTO legislacao_mh3_por_pais_ano VALUES (?,?,?)
        """, tuple(row))
    except Exception as e:
        print(f"[legislacao_mh3_por_pais_ano] Erro na linha {idx}: {e}")


# ----------------------- Tabela 6: tabela_life_expectancy_at_60.csv -----------------------
# ----------------------- Tabela 6: tabela_life_expectancy_at_60.csv -----------------------
df6 = pd.read_csv("tabela_life_expectancy_at_60.csv")

# Substituir NaNs por None
df6 = df6.where(pd.notnull(df6), None)

# Verifica se há valores estranhos (opcional)
#print(df6["country"].unique())

for idx, row in df6.iterrows():
    try:
        placeholders = ','.join(['?'] * len(row))
        query = f"INSERT INTO life_expectancy_at_60 VALUES ({placeholders})"
        cursor.execute(query, tuple(row))
    except Exception as e:
        print(f"[life_expectancy_at_60] Erro na linha {idx}: {e}")
        print(row)



# Finalização
conn.commit()
cursor.close()
conn.close()
print("✅ Todos os dados foram inseridos no SQL Server com sucesso.")
