from kedro.pipeline import Pipeline, node, pipeline

from .nodes import prioriza_bacias


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=prioriza_bacias,
                inputs=["input_pre_processado", "parametros"],
                outputs=["bacias_priorizadas", "dataset_resumo"],
                name="prioriza_bacias_node",
            ),
        ])
