from enum import Enum

class ColsParams(Enum):
    taxa_desconto = 'TAXA_DESCONTO'
    ano_inicio_capex = 'ANO_INICIO_CAPEX'
    threshold_tir = 'THRESHOLD_TIR'

class ColsOutros(Enum):
    fluxo = 'FLUXO'
    bacia = 'BACIA'
    cod_mun = 'COD_MUN'
    bloco = 'BLOCO'
    tir = 'TIR'
    vpl = 'VPL'
    ano = 'ANO'
    bacia_predec = 'BACIA_PREDEC'
    exec_predec = 'EXEC_PREDEC'

class ColsIC(Enum):
    ic_e = 'IC_E'
    ic_bac_apoio = 'IC_BAC_APOIO'
    ic_bac = 'P_IC_BAC'
    ic_mun = 'P_IC_MUN'
    ic_blo = 'P_IC_BLO'
    ic_glo = 'P_IC_GLO'
    ic_bac_tot = 'P_IC_BAC_TOT'
    
class ColsEco(Enum):
    eco_pot = 'ECO_POT'
    eco_incr_conced = 'ECO_INCR_CONCED'
    eco_pot_mun = 'ECO_POT_MUN'
    eco_pot_blo = 'ECO_POT_BLOCO'
    eco_pot_glo = 'ECO_POT_GLOBAL'
    eco_fact_bac = 'P_ECO_FACT_BAC'
    soma_eco_fact_bac = 'SOMA_P_ECO_FACT_BAC'

class ColsMetas(Enum):
    meta_mun = 'META_MUN'
    meta_bloco = 'META_BLOCO'
    meta_global = 'META_GLOBAL'

class ColsRanks(Enum):
    rank_vpl = 'RANK_VPL'
    rank_tir = 'RANK_TIR'
    rank_economico = 'RANK_ECONOMICO'
    rank_mun = 'RANK_MUN'
    rank_bloco = 'RANK_BLOCO'
    rank_global = 'RANK_GLOBAL'


