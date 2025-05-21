from kedro.pipeline import Pipeline, node, pipeline

from .nodes import pre_processa_input, calcula_ranking_bacias


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=calcula_ranking_bacias,
                inputs=["input", "parametros"],
                outputs="ranking_bacias",
                name="calcula_ranking_bacias_node",
            ),
            node(
                func=pre_processa_input,
                inputs=["input","ranking_bacias"],
                outputs="input_pre_processado",
                name="pre_processa_input_node",
            ),
        ])
