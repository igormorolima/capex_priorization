import pandas as pd
from priorizacao_capex.objects.utils.enums import *
from typing import Tuple

# Definindo atalhos para captar os nomes de colunas por meio do Enums
col_ano_inicio = ColsParams.ano_inicio_capex.value
col_threshold_tir = ColsParams.threshold_tir.value

col_bacia = ColsOutros.bacia.value
col_cod_mun = ColsOutros.cod_mun.value
col_bloco = ColsOutros.bloco.value
col_tir = ColsOutros.tir.value
col_ano = ColsOutros.ano.value
col_exec_predec = ColsOutros.exec_predec.value
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

rank_economico = ColsRanks.rank_economico.value
rank_mun = ColsRanks.rank_mun.value
rank_bloco = ColsRanks.rank_bloco.value
rank_global = ColsRanks.rank_global.value


def round_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Arredonda casas decimais para o output no final de tudo
    """
    # Dicionário mapeando as colunas ao número de casas decimais
    colunas_arredondamento = {
        ic_e: 3,
        eco_pot: 0,
        eco_incr_conced: 0,
        meta_mun: 3,
        meta_bloco: 3,
        meta_global: 3,
        ic_bac_apoio: 3,
        eco_pot_mun: 0,
        eco_pot_blo: 0,
        eco_pot_glo: 0,
        eco_fact_bac: 0,
        ic_mun: 3,
        ic_blo: 3,
        ic_glo: 3,
        ic_bac: 3,
        ic_bac_tot: 3,
        col_tir: 3}

    # Aplicando o arredondamento com base no dicionário
    for coluna, decimais in colunas_arredondamento.items():
        df[coluna] = df[coluna].round(decimais)

    return df


def atualiza_bacias(df: pd.DataFrame) -> pd.DataFrame:
    """
    Temos que recalcular as economias factíveis e o ic_bac da bacia pois a eco_pot aumenta todo ano (projeção populacional), mesmo que 
    a ic_bac_tot não mude
    """
    for bacia, df_bacia in df.groupby(col_bacia):
        # O ic_bac usa a eco_incr_conced como complemento para atingir o ic_bac_tot
        df_bacia[ic_bac] = (df_bacia[eco_pot] * df_bacia[ic_bac_tot] - df_bacia[eco_incr_conced]) / df_bacia[eco_pot]
        df_bacia[eco_fact_bac] = df_bacia[eco_pot] * df_bacia[ic_bac_tot]
        df.update(df_bacia)

    return df


def reprioriza_predecessoras(df: pd.DataFrame, rank_coluna: str, rank_atual: int) -> tuple[int, bool]:
    """
    Verifica as bacias predecessoras para decidir se alguma deve ser repriorizada com base no ranking econômico
    Retorna o valor `rank_atual` da predecessora a ser repriorizada, caso haja, e atualiza a coluna auxiliar 'flag_bacia_predec'
    """
    completa_predec = False

    # Filtra bacias predecessoras com flag True e ordena por rank_economico
    bacias_predecessoras = df[df["flag_bacia_predec"]].copy()
    bacias_predecessoras = bacias_predecessoras.sort_values(by=rank_economico)

    # Obter o ranking econômico da próxima bacia do loop de bacias
    prox_bacia = df[df[rank_coluna] == (rank_atual + 1)]
    rank_economico_prox_bacia = prox_bacia[rank_economico].iloc[0] if not prox_bacia.empty else None

    # Itera sobre bacias predecessoras para verificar se precisam ser repriorizadas
    for _, bacia_predecessora in bacias_predecessoras.iterrows():
        rank_economico_predecessora = bacia_predecessora[rank_economico]

        # Marca bacia predecessora como completa se atingiu ic_bac_tot >= 1.0
        if bacia_predecessora[ic_bac_tot] >= 1.0:
            df.loc[df[col_bacia] == bacia_predecessora[col_bacia], "flag_bacia_predec"] = False
            print(f"Bacia predecessora {bacia_predecessora[col_bacia]} marcada como completa")
            continue

        # Não repriorizar bacias predecessoras que ainda nem passaram pelo atinge_meta
        if bacia_predecessora[rank_coluna] > rank_atual:
            continue
        
        # Reprioriza se o ranking econômico da predecessora for melhor que o da próxima bacia
        if rank_economico_prox_bacia is not None and rank_economico_predecessora < rank_economico_prox_bacia:
            rank_atual = bacia_predecessora[rank_coluna]
            completa_predec = True
            print(f"Bacia predecessora {bacia_predecessora[col_bacia]} repriorizada")
            break

    return rank_atual, completa_predec


def processa_economias_bacia(df: pd.DataFrame, grupo_ranking: pd.DataFrame, Var_Eco_coluna: float, threshold_tir: float, completa_predec: bool) -> Tuple[float, bool]:
    """
    Processa as economias disponíveis na bacia e atualiza o índice de cobertura.
    """
    # Caso alguma bacia predecessora seja repriorizada para ser finalizada mudamos a EXEC_PREDEC para 100%
    tir = grupo_ranking[col_tir].iloc[0]
    exec_predec = grupo_ranking[col_exec_predec].iloc[0]
    if completa_predec or (exec_predec < 1.0 and tir >= threshold_tir):
        print("Reprioriza bacia predecessora:", grupo_ranking[col_bacia].iloc[0])
        grupo_ranking[col_exec_predec] = 1.0

    # Calcula as economias disponíveis que ainda podem ser adicionadas na bacia
    Var_Eco_Bac = (grupo_ranking[eco_pot].iloc[0] * grupo_ranking[col_exec_predec].iloc[0]) - grupo_ranking[eco_fact_bac].iloc[0]

    # Caso tenhamos economias insuficientes na bacia, usamos o máximo possível e atualizamos a Var_Eco_coluna
    if 0 < Var_Eco_Bac < Var_Eco_coluna:         
        # O IC_BAC_TOT será no máximo o permitido para a bacia atual, que será 100% ou o exec_predec atual
        grupo_ranking[ic_bac_tot] = grupo_ranking[col_exec_predec].iloc[0]
        Var_Eco_coluna -= Var_Eco_Bac
    # Caso tenhamos economias mais que suficientes na bacia, usamos apenas o necessário e zeramos a Var_Eco_coluna
    elif Var_Eco_Bac > Var_Eco_coluna:          
        grupo_ranking[ic_bac_tot] = (grupo_ranking[eco_fact_bac] + Var_Eco_coluna) / grupo_ranking[eco_pot]
        Var_Eco_coluna = 0

    # Salva alterações de ic_bac_tot
    df.loc[grupo_ranking.index, ic_bac_tot] = grupo_ranking[ic_bac_tot]

    return df, Var_Eco_coluna


def verifica_bacia_habilitada(df: pd.DataFrame, grupo_ranking: pd.DataFrame, rank_coluna: str) -> tuple[bool, int]:
    """
    Verifica se há uma bacia habilitada para repriorização durante a iteração atual de alguma predecessora.
    Retorno: (True, rank_coluna) se houver uma bacia habilitada selecionada, senão (False, None).
    """
    # Verifica se a iteração atual é uma predecessora não completa
    if not grupo_ranking["flag_bacia_predec"].iloc[0]:
        return False, None  # Não é predecessora

    # Filtra bacias com rank_economico menor que a bacia atual e rank_coluna maior que a iteração atual das bacias do ano
    bacias_candidatas = df[(df[rank_economico] < grupo_ranking[rank_economico].iloc[0]) & (df[rank_coluna] > grupo_ranking[rank_coluna].iloc[0])].copy()

    # Verifica habilitação de cada bacia filtrada
    def is_bacia_habilitada(bacia):
        """Verifica se a bacia é habilitada com base em ic_bac_tot de sua predecessora caso exista."""
        df_bac_predec = df[df[col_bacia] == bacia[col_bacia_predec]]
        if bacia[ic_bac_tot] == 1: # Bacia completada
            return False
        if df_bac_predec.empty:
            return True  # Bacia predecessora não encontrada
        if bacia[col_bacia_predec] == grupo_ranking[col_bacia].iloc[0]:
            return False # Bacia dependente da própria predecessora em análise, não precisa ser repriorizada neste momento
        # Visto que depende da predecessora em análise, verifica se está habilitada
        return df_bac_predec[ic_bac_tot].iloc[0] >= df_bac_predec[col_exec_predec].iloc[0] 

    bacias_habilitadas = bacias_candidatas[bacias_candidatas.apply(is_bacia_habilitada, axis=1)]

    if bacias_habilitadas.empty:
        return False, None  # Nenhuma bacia habilitada encontrada

    # Seleciona a bacia habilitada com o menor rank_economico
    bacia_habilitada = bacias_habilitadas.sort_values(by=rank_economico).iloc[0]

    # Verifica a condição de TIR, apenas repriorizamos caso a TIR da habilitada seja maior que a da predecessora em mais que 10%
    tir_bacia_atual = grupo_ranking[col_tir].iloc[0]
    tir_bacia_habilitada = bacia_habilitada[col_tir]

    if tir_bacia_habilitada >= tir_bacia_atual + 0.1:
        print(f"Bacia habilitada boa: {bacia_habilitada[col_bacia]} com rank {bacia_habilitada[rank_coluna]} sendo repriorizada")
        return True, bacia_habilitada[rank_coluna]

    return False, None


def atinge_meta(df_ano_atual: pd.DataFrame, df: pd.DataFrame, meta_coluna: str, p_ic_coluna: str, rank_coluna: str, eco_pot_coluna: str, threshold_tir: float) -> pd.DataFrame:
    """
    Itera sobre as bacias para atingir a meta especificada (meta_coluna) seguindo a sequência do ranking (rank_coluna), atualizando 
    o índice de cobertura da bacia (ic_bac_tot). 
    """
    # Cria e atualiza a coluna auxiliar local 'flag_bacia_predec', para identificar as bacias predecessoras ainda não completadas como True para execução posterior
    df['flag_bacia_predec'] = ((df[col_exec_predec] < 1.0) & (df[ic_bac_tot] < 1.0))
    completa_predec = False

    # Calcula-se o delta % que falta para atingir a meta com o índice atual (p_ic_coluna). O max() aqui serve para nos certificarmos 
    ## de pegarmos o maior valor presente do agrupamento atual, mas tecnicamente não faria diferença alguma pois os números são os mesmos.
    Var_IC_coluna = df[meta_coluna].max() - df[p_ic_coluna].max()
    # Calcula-se o número de economias do agrupamento atual (eco_pot_coluna) para atingir a meta, serve como um counter a ser zerado
    Var_Eco_coluna = Var_IC_coluna * df[eco_pot_coluna].max()

    # Itera sobre as bacias que fornecerão economias até zerar as economias necessárias para a meta (Var_Eco_coluna)
    rank_atual = 1
    while round(Var_Eco_coluna, 0) > 0 and rank_atual <= df[rank_coluna].max():
        grupo_ranking = df[df[rank_coluna] == rank_atual].copy()

        # Verificar se esta bacia está habilitada de acordo com a sua predecessora, caso exista, mesmo que em outro bloco, se não, passa para o próximo ranking
        if not grupo_ranking[col_bacia_predec].isna().all():
            df_bac_predec = df_ano_atual[df_ano_atual[col_bacia] == grupo_ranking[col_bacia_predec].iloc[0]]
            if not df_bac_predec[ic_bac_tot].iloc[0] >= df_bac_predec[col_exec_predec].iloc[0]:
                rank_atual += 1
                print("Bacia", grupo_ranking[col_bacia].iloc[0], "não habilitada devido a IC insuficiente da predecessora")
                continue

        # Se houver uma bacia habilitada boa o suficiente, prioriza a execução dela ao invés de executar predecessoras, e retorna ao loop anterior com o mesmo rank_atual
        habilitada, rank_selecionado = verifica_bacia_habilitada(df, grupo_ranking, rank_coluna)
        if habilitada:
            grupo_ranking = df[df[rank_coluna] == rank_selecionado].copy()
            df, Var_Eco_coluna = processa_economias_bacia(df, grupo_ranking, Var_Eco_coluna, threshold_tir, completa_predec)
            continue
        
        # Processa economias da bacia atual
        df, Var_Eco_coluna = processa_economias_bacia(df, grupo_ranking, Var_Eco_coluna, threshold_tir, completa_predec)

        # Caso necessário, retornar às bacias predecessoras e verificar repriorização caso vantajoso
        if round(Var_Eco_coluna, 0) > 0:
            rank_atual, completa_predec = reprioriza_predecessoras(df, rank_coluna, rank_atual)

        # Passa para o próximo ranking caso não haja repriorização
        if not completa_predec:
            rank_atual += 1

    # Atualiza eco_fact_bac
    df = atualiza_bacias(df)

    return df


def recalcula_IC(df: pd.DataFrame, eco_pot_coluna: str, p_ic_coluna: str) -> pd.DataFrame:
    """
    Atualiza o valor do índice de cobertura do agrupamento especificado (p_ic_coluna), fazendo a some de todas as contribuições de 
    economicas factíveis das bacias (soma_p_eco_fact_bac) e dividindo pelas economias potencias totais do agrupamento (eco_pot_coluna)
    """
    soma_p_eco_fact_bac = df[eco_fact_bac].sum()
    valor_p_ic_coluna = soma_p_eco_fact_bac / df[eco_pot_coluna].iloc[0]
    df[p_ic_coluna] = valor_p_ic_coluna
    
    return df


def processa_municipios(df_ano_atual: pd.DataFrame, threshold_tir: float) -> pd.DataFrame:
    """
    Itera por municípios e busca atingir suas metas (meta_mun), seguindo a sequência de bacias de acordo com o ranking por
    municípios (rank_mun), e recalcula o índice de cobertura (ic_mun)
    """
    print("Iterando por município")
    
    # Ordena os cod_mun pelo menor rank_global (desta forma incentivamos as melhores bacias predecessoras a serem executadas primeiros, 
    # antes que outro município precise dela)
    cod_mun_order = (
        df_ano_atual.groupby(col_cod_mun)[rank_global]
        .min()  # Calcula o mínimo de rank_global por cod_mun
        .sort_values()  # Ordena do menor para o maior
        .index  # Obtém os índices (cod_mun) na ordem desejada
    )

    # Itera pelos cod_mun na ordem definida
    for cod_mun in cod_mun_order:
        grupo_mun = df_ano_atual[df_ano_atual[col_cod_mun] == cod_mun].copy()
        grupo_mun = atinge_meta(df_ano_atual, grupo_mun, meta_mun, ic_mun, rank_mun, eco_pot_mun, threshold_tir)
        grupo_mun = recalcula_IC(grupo_mun, eco_pot_mun, ic_mun)
        df_ano_atual.update(grupo_mun)

    return df_ano_atual


def processa_blocos(df_ano_atual: pd.DataFrame, threshold_tir: float) -> pd.DataFrame:
    """
    Itera por blocos, primeiro recalcula índice de cobertura (ic_blo) após a iteração por municípios, depois busca atingir suas 
    metas (meta_bloco) seguindo a sequência de bacias de acordo com o ranking por blocos (rank_bloco), e por último atualiza 
    novamente o índice de cobertura
    """
    print("Iterando por bloco")

    # Ordena os blocos pelo menor rank_global (desta forma incentivamos as melhores bacias predecessoras a serem executadas primeiros, 
    # antes que outro bloco precise dela)
    bloco_order = (
        df_ano_atual.groupby(col_bloco)[rank_global]
        .min()  # Calcula o mínimo de rank_global por bloco
        .sort_values()  # Ordena do menor para o maior
        .index  # Obtém os índices (blocos) na ordem desejada
    )

    # Itera pelos bloco na ordem definida
    for bloco in bloco_order:
        grupo_bloco = df_ano_atual[df_ano_atual[col_bloco] == bloco].copy()
        grupo_bloco = recalcula_IC(grupo_bloco, eco_pot_blo, ic_blo)
        grupo_bloco = atinge_meta(df_ano_atual, grupo_bloco, meta_bloco, ic_blo, rank_bloco, eco_pot_blo, threshold_tir)
        grupo_bloco = recalcula_IC(grupo_bloco, eco_pot_blo, ic_blo)
        df_ano_atual.update(grupo_bloco)

    return df_ano_atual


def processa_global(df_ano_atual: pd.DataFrame, threshold_tir: float) -> pd.DataFrame:
    """
    Agora de forma global, primeiro recalcula índice de cobertura (ic_glo) após a iteração por blocos, depois busca atingir suas 
    metas (meta_global) seguindo a sequência de bacias de acordo com o ranking global (rank_global), e por último atualiza 
    novamente o índice de cobertura
    """
    print("Iteração global")

    df_ano_atual = recalcula_IC(df_ano_atual, eco_pot_glo, ic_glo)
    df_ano_atual = atinge_meta(df_ano_atual, df_ano_atual, meta_global, ic_glo, rank_global, eco_pot_glo, threshold_tir)
    df_ano_atual = recalcula_IC(df_ano_atual, eco_pot_glo, ic_glo)

    return df_ano_atual


def resultados_ano_anterior(df_resultados_ano: pd.DataFrame, ano: int, ano_inicio_capex: int) -> pd.DataFrame:
    """
    Inicializa o dataframe com valores do ano atual e com as colunas ic_bac_tot, ic_mun, ic_blo, ic_glo do ano anterior,
    e recalcula eco_fact_bac e ic_bac_tot
    """
    df_ano_atual = df_resultados_ano[df_resultados_ano[col_ano] == ano].copy()
    
    # Atualizo as colunas ic_bac_tot, ic_mun, ic_blo, ic_glo com os valores do ano anterior
    if ano > ano_inicio_capex:
        ano_anterior = df_resultados_ano[df_resultados_ano[col_ano] == ano - 1][[ic_bac_tot, ic_mun, ic_blo, ic_glo]]
        df_ano_atual[[ic_bac_tot, ic_mun, ic_blo, ic_glo]] = ano_anterior.values

        # Recalcula eco_fact_bac e ic_bac
        df_ano_atual = atualiza_bacias(df_ano_atual)
    
    return df_ano_atual


def atualiza_ICs_ano(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Recalcula ic_mun e ic_blo após atingimento de todas as metas no ano atual, iterando por todos os municípios e blocos
    É necessário atualizar o índice de município após atingimento de meta de bloco e geral, assim como atualizar o índice de bloco após atingimento de meta geral
    '''
    for cod_mun, grupo_mun in df.groupby(col_cod_mun):
        grupo_mun = recalcula_IC(grupo_mun, eco_pot_mun, ic_mun)
        df.update(grupo_mun)

    for bloco, grupo_bloco in df.groupby(col_bloco):
        grupo_bloco = recalcula_IC(grupo_bloco, eco_pot_blo, ic_blo)
        df.update(grupo_bloco)

    return df


def prioriza_bacias(df: pd.DataFrame, parametros: pd.DataFrame) -> pd.DataFrame:
    """
    Principal função do pipeline. Usa como input o dataframe 'df' que é o 'input_pre_processado' do pipeline de 'data_processing' para iterar pelo anos de 
    projeto a partir do ano 2 (ano_inicio_capex). Para cada ano traz os resultados obtidos do ano anterior, itera sobre os municípios, blocos e global 
    para atingir suas metas respectivas, de acordo com as alterações nos índices de cobertura das bacias a serem priorizadas e 
    considerando os rankings definidos anteriormente.
    
    O input 'parametros' traz as variaveis ano_inicio_capex e threshold_tir
    """
    # Inicializa os parâmetros ano_inicio_capex e threshold_tir (valores únicos) vindos do input_parametros.xlsx
    ano_inicio_capex = parametros[col_ano_inicio].iloc[0]
    threshold_tir = parametros[col_threshold_tir].iloc[0]

    # As obras de Capex geralmente se iniciam a partir do segundo ano
    df_ano = df[df[col_ano] >= ano_inicio_capex].copy()
    df_resultados_ano = df_ano.copy()

    # Itera sobre os anos e chama as funções que atingem as metas de município, bloco e geral e atualiza os ICs, ano a ano
    for ano in sorted(df_ano[col_ano].unique()):
        print("Processando ano:", ano)
        df_ano_atual = resultados_ano_anterior(df_resultados_ano, ano, ano_inicio_capex)
        df_ano_atual = processa_municipios(df_ano_atual, threshold_tir)
        df_ano_atual = processa_blocos(df_ano_atual, threshold_tir)
        df_ano_atual = processa_global(df_ano_atual, threshold_tir)
        df_ano_atual = atualiza_ICs_ano(df_ano_atual)
        df_resultados_ano.update(df_ano_atual)

    df.update(df_resultados_ano)
    df = round_cols(df)

    # Define colunas a serem exportadas na camada de Reporting
    df_report = df[[col_bacia, col_ano, ic_bac_tot]]

    return df, df_report