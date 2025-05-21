# Instruções:

1) Popular o input conforme as colunas já definidas previamente em data/01-input/input_priorizacao.xlsx
    - Copiar os valores de ECO_INCR_CONCED para todos os anos a partir do primeiro ano em que há incremento do poder concedente para aquela bacia, ou seja, é um valor acumulativo
    - Copiar a bacia predecessora em BACIA_PREDEC em todos os anos da bacia que depende desta
    - Se não houver Meta Global ou de Bloco, deixar todos os valores 0%
    - Colocar a % de execução da bacia predecessora necessária para habilitar a execução das dependentes em EXEC_PREDEC, no resto deixar 100%

2) Executar kedro run --pipeline data_processing

3) Executar kedro run --pipeline model_priorization
