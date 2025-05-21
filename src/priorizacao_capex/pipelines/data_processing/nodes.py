import pandas as pd
import numpy as np
import numpy_financial as npf
from priorizacao_capex.objects.utils.enums import *
from datetime import datetime

CT = datetime.now().strftime("%d-%m-%Y_%Hh%Mm%Ss")

# Definindo atalhos para captar os nomes de colunas por meio do Enums, a fim de reduzir a poluição visual para o entendimento das lógicas de cálculos
col_tx_desc = ColsParams.taxa_desconto.value

col_fluxo = ColsOutros.fluxo.value
col_bacia = ColsOutros.bacia.value
col_cod_mun = ColsOutros.cod_mun.value
col_bloco = ColsOutros.bloco.value
col_tir = ColsOutros.tir.value
col_vpl = ColsOutros.vpl.value
col_ano = ColsOutros.ano.value
col_bacia_predec = ColsOutros.bacia_predec.value

ic_e = ColsIC.ic_e.value
ic_bac_apoio = ColsIC.ic_bac_apoio.value
ic_bac = ColsIC.ic_bac.value
ic_mun = ColsIC.ic_mun.value
ic_blo = ColsIC.ic_blo.value
ic_glo = ColsIC.ic_glo.value
ic_bac_tot = ColsIC.ic_bac_tot.value

eco_pot = ColsEco.eco_pot.value
eco_incr_conced = ColsEco.eco_incr_conced.value
eco_pot_mun = ColsEco.eco_pot_mun.value
eco_pot_blo = ColsEco.eco_pot_blo.value
eco_pot_glo = ColsEco.eco_pot_glo.value
eco_fact_bac = ColsEco.eco_fact_bac.value
soma_eco_fact_bac = ColsEco.soma_eco_fact_bac.value

meta_mun = ColsMetas.meta_mun.value
meta_bloco = ColsMetas.meta_bloco.value
meta_global = ColsMetas.meta_global.value

rank_vpl = ColsRanks.rank_vpl.value
rank_tir = ColsRanks.rank_tir.value
rank_economico = ColsRanks.rank_economico.value
rank_mun = ColsRanks.rank_mun.value
rank_bloco = ColsRanks.rank_bloco.value
rank_global = ColsRanks.rank_global.value

def round_cols(input: pd.DataFrame) -> pd.DataFrame:
    """
    Arredonda casas decimais
    """
    # Dicionário mapeando as colunas ao número de casas decimais
    colunas_arredondamento = {
        col_fluxo: 2,
        ic_e: 4,
        eco_pot: 2,
        eco_incr_conced: 2,
        meta_mun: 4,
        meta_bloco: 4,
        meta_global: 4}

    # Aplicando o arredondamento com base no dicionário
    for coluna, decimais in colunas_arredondamento.items():
        input[coluna] = input[coluna].round(decimais)

    return input


def calculate_tir_vpl(input: pd.DataFrame, taxa_desconto: float) -> pd.DataFrame:
    """
    Calcula TIR e VPL com Numpy Financial. 

    O parâmetro 'taxa_desconto' para o VPL vem do parameters.yml
    """
    # Função para calcular o VPL
    def calcular_vpl(taxa_desconto, fluxos):
        return npf.npv(taxa_desconto, fluxos)

    # Função para calcular a TIR
    def calcular_tir(fluxos):
        tir = npf.irr(fluxos)
        if np.isnan(tir):
            return 0  # Retorna 0 se a TIR for NaN devido a um fluxo negativo em todos os anos
        return npf.irr(fluxos)

    # Agrupar por BACIA e calcular TIR e VPL para cada grupo
    df_VPL_TIR = input.groupby([col_cod_mun,col_bloco,col_bacia]).apply(
        lambda x: pd.Series({
            col_vpl: calcular_vpl(taxa_desconto, x[col_fluxo].values),
            col_tir: calcular_tir(x[col_fluxo].values),
        })
    ).reset_index()

    return df_VPL_TIR


def ranking_economico(df_VPL_TIR: pd.DataFrame, taxa_desconto: float) -> pd.DataFrame:
    """
    Calcula Ranking Econômico (RANK_ECONOMICO) global com base na TIR para valores acima de 0,1, e VPL para o restante
    """
    # Ordenar os dados por VPL e TIR de todas as bacias
    df_VPL_TIR[rank_vpl] = df_VPL_TIR[col_vpl].rank(ascending=False, method='first').astype(int)
    df_VPL_TIR[rank_tir] = df_VPL_TIR[col_tir].rank(ascending=False, method='first').astype(int)

    # Função para criar uma chave de ordenação baseada em TIR e VPL
    def chave_ordenacao(row):
        if row[col_tir] >= taxa_desconto:
            # Priorizar a TIR para valores maiores ou iguais a taxa_desconto
            return (-row[col_tir], -row[col_vpl])  # Usamos VPL para desempate
        else:
            # Priorizar VPL se TIR for menor que taxa_desconto
            return (0, -row[col_vpl])  # Colocamos 0 para TIR como "neutra" e usar VPL como chave

    # Aplicar o método de ordenação e ordenar pela chave
    df_VPL_TIR['chave_ordenacao'] = df_VPL_TIR.apply(chave_ordenacao, axis=1)
    df_RANK_ECONOMICO = df_VPL_TIR.sort_values(by='chave_ordenacao', ascending=True).reset_index(drop=True)

    # Criar RANK_ECONOMICO após ordenar
    df_RANK_ECONOMICO[rank_economico] = df_RANK_ECONOMICO.index + 1
    df_RANK_ECONOMICO = df_RANK_ECONOMICO.drop(columns='chave_ordenacao')

    return df_RANK_ECONOMICO


def construir_cadeia(input, bacia):
    """
    Função para construir a cadeia de predecessoras para o ranking físico
    """
    cadeia = []
    while pd.notna(bacia):
        cadeia.append(bacia)
        # Avança para a próxima predecessora
        predecessora = input.loc[input[col_bacia] == bacia, col_bacia_predec].values
        bacia = predecessora[0] if len(predecessora) > 0 else None
    return cadeia[::-1]  # Inverte para ter a predecessora mais antiga primeiro
    

def ranking_fisico(input: pd.DataFrame, df_RANK_ECONOMICO: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula Ranking Físico Global (RANK_GLOBAL), do Bloco (RANK_BLOCO) e Município (RANK_MUN) com base no Ranking Econômico 
    e bacias predecessoras
    """
    # Fazendo o merge de ranking_bacias com input para capturar a coluna BACIA_PREDEC
    ranking_bacias = pd.merge(
        df_RANK_ECONOMICO, 
        input[[col_bacia, col_bacia_predec]].drop_duplicates(),  # Garante que não haja duplicatas no merge
        on=col_bacia, 
        how='left')

    # Constrói uma lista ordenada de bacias baseada nas predecessoras
    ordem_fisica = []
    ja_classificadas = set()

    # Itera pelas bacias ordenadas por RANK_ECONOMICO
    for bacia in ranking_bacias[col_bacia]:
        if bacia not in ja_classificadas:
            cadeia = construir_cadeia(ranking_bacias, bacia)
            # Adiciona na ordem correta as bacias da cadeia que ainda não foram classificadas
            for predec in cadeia:
                if predec not in ja_classificadas:
                    ordem_fisica.append(predec)
                    ja_classificadas.add(predec)

    # Mapeia a ordem física para uma nova coluna temporária que serve para o RANK_GLOBAL
    ranking_bacias['ordem_temp'] = ranking_bacias[col_bacia].map({bacia: idx for idx, bacia in enumerate(ordem_fisica, 1)})
    # Ordena de acordo com a ordem física estabelecida
    ranking_bacias = ranking_bacias.sort_values(by='ordem_temp').reset_index(drop=True)
    ranking_bacias[rank_global] = ranking_bacias.index + 1

    # Aplicar ranking por Bloco e Município com base em RANK_GLOBAL
    ranking_bacias[rank_bloco] = ranking_bacias.groupby(col_bloco)[rank_global].rank(method='first').astype(int)
    ranking_bacias[rank_mun] = ranking_bacias.groupby(col_cod_mun)[rank_global].rank(method='first').astype(int)

    # Remove a coluna temporária antes de retornar o DataFrame
    ranking_bacias.drop(columns=['ordem_temp'], inplace=True)

    return ranking_bacias


def calcula_ranking_bacias(input: pd.DataFrame, parametros: pd.DataFrame) -> pd.DataFrame:
    """
    Com o dataframe 'input', calcula TIR e VPL, elabora um ranking econômico por Bloco, depois um ranking físico por Bloco e Município considerando
    as bacias predecessoras

    O 'parametros' traz a variável col_tx_desc
    """
    # Inicializa o parâmetro taxa_desconto da TIR (valor único) vindo do input_parametros.xlsx
    taxa_desconto = parametros[col_tx_desc].iloc[0]

    input = round_cols(input)
    df_VPL_TIR = calculate_tir_vpl(input, taxa_desconto)
    df_RANK_ECONOMICO = ranking_economico(df_VPL_TIR, taxa_desconto)
    ranking_bacias = ranking_fisico(input, df_RANK_ECONOMICO)
    
    return ranking_bacias


def pre_processa_input(input: pd.DataFrame, ranking_bacias: pd.DataFrame) -> pd.DataFrame:
    """
    Principal função do pipeline. Traz os rankings para a base de input, e inicializa colunas auxiliares para os cálculos de 
    economias factíveis (ECO_FACT) e potenciais (ECO_POT) que são usadas nos índices de cobertura (IC)

    ic_bac_apoio = Índice de Cobertura da Bacia para o ano 0
    eco_pot_mun = Economias Potencias do Município
    eco_pot_blo = Economias Potencias do Bloco
    eco_pot_glo = Economias Potencias geral (de toda a concessão)
    eco_fact_bac = Economias Factíveis da Bacia (com cobertura)
    ic_mun = Índice de Cobertura do Município
    ic_blo = Índice de Cobertura do Bloco
    ic_glo = Índice de Cobertura Global
    ic_bac = Índice de Cobertura da Bacia (aqui é inicializado pelo ic_bac_apoio)
    ic_bac_tot = Índice de Cobertura da Bacia Total (considerando as eco_incr_conced)
    """
    def cria_col_soma(df, groupby_cols, agg_col, new_col_name):
        # Função auxiliar para realizar groupby, agregação e merge com renomeação.
        aggregated = df.groupby(groupby_cols).agg({agg_col: 'sum'}).rename(columns={agg_col: new_col_name})
        return pd.merge(df, aggregated, on=groupby_cols, how='left')

    # Traz os rankings para a base
    df = pd.merge(
        input,
        ranking_bacias[[col_bacia, col_tir, rank_economico, rank_global, rank_bloco, rank_mun]],
        on=col_bacia,
        how='left'
    )

    # Merge para adicionar IC_BAC_APOIO que é igual ao IC_E do ano 0
    df_filtered = df[df[col_ano] == 0][[col_bacia, ic_e]].drop_duplicates()
    df_filtered = df_filtered.rename(columns={ic_e: ic_bac_apoio})
    df = pd.merge(df, df_filtered, on=col_bacia, how='left')

    # Adiciona ECO_POT_MUN para cada município e ano
    df = cria_col_soma(df, [col_cod_mun, col_ano], eco_pot, eco_pot_mun)

    # Adiciona ECO_POT_BLOCO para cada bloco e ano
    df = cria_col_soma(df, [col_bloco, col_ano], eco_pot, eco_pot_blo)

    # Adiciona ECO_POT_GLOBAL para cada bloco e ano
    df = cria_col_soma(df, [col_ano], eco_pot, eco_pot_glo)

    # Inicializa P_ECO_FACT_BAC (economias potenciais da bacia)
    df[eco_fact_bac] = (df[eco_pot] * df[ic_bac_apoio]) + df[eco_incr_conced]

    # Soma P_ECO_FACT_BAC por município e calcula P_IC_MUN
    df = cria_col_soma(df, [col_cod_mun, col_ano], eco_fact_bac, soma_eco_fact_bac)
    df[ic_mun] = df[soma_eco_fact_bac] / df[eco_pot_mun]
    df.drop(columns=soma_eco_fact_bac, inplace=True)

    # Soma P_ECO_FACT_BAC por bloco e calcula P_IC_BLO
    df = cria_col_soma(df, [col_bloco, col_ano], eco_fact_bac, soma_eco_fact_bac)
    df[ic_blo] = df[soma_eco_fact_bac] / df[eco_pot_blo]
    df.drop(columns=soma_eco_fact_bac, inplace=True)

    # Soma P_ECO_FACT_BAC globalmente e calcula P_IC_GLO
    df = cria_col_soma(df, [col_ano], eco_fact_bac, soma_eco_fact_bac)
    df[ic_glo] = df[soma_eco_fact_bac] / df[eco_pot_glo]
    df.drop(columns=soma_eco_fact_bac, inplace=True)

    # Inicializa P_IC_BAC e P_IC_BAC_TOT
    df[ic_bac] = df[ic_bac_apoio].copy()
    df[ic_bac_tot] = np.where(df[eco_pot] != 0, df[eco_fact_bac] / df[eco_pot], 0)

    # Aqui adicione ou retire as colunas a serem deletadas do dataframe por não serem mais necessárias daqui pra frente
    df.drop(columns=[col_fluxo], inplace=True)

    return df