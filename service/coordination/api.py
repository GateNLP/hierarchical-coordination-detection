import json

from .config import APP_CONFIG

from .data_sources.csv_handler import read_post
from .data_sources.es_handler import build_es_query, process_es_data

from .utils.result_generator import generate_json_result
from .processing.coordination import calcCoordination

from logging import getLogger

datastore = APP_CONFIG.datastore.create_datastore()


logger = getLogger(__name__)


def calcCoordinationCSV(jobId: str) -> None:
    """
    Calculate coordination metrics with CSV.
    """

    options = jobId.split("_")[1]

    with datastore.csv_job_files(jobId) as (
        path_input,
        path_useless_hashtags,
        path_json_tmp,
        path_output_tmp,
    ):
        posts, raw_data = read_post(path_input, path_useless_hashtags)

        result = calcCoordination(posts, options[0])

        with open(path_json_tmp, "w", encoding="utf-8") as f:
            json.dump(generate_json_result(result, raw_data), f)

        # Write final results in the output file
        result.to_csv(path_output_tmp, encoding="utf-8", index=False)

    return None


def calcCoordinationES(jobId: str) -> None:
    """
    Calculate coordination metrics with Elasticsearch.
    """

    # Use context manager to handle job configuration and files
    with datastore.elastic_job_files(jobId) as (
        job_config,
        path_json_tmp,
        path_output_tmp,
    ):
        logger.debug(
            f"Processing Elasticsearch job with config: {job_config.to_dict()}"
        )

        # find the configuration for the Elasticsearch connection and index - if
        # either of these are invalid it will raise an exception
        es_config = APP_CONFIG.elasticsearch[job_config.elasticsearch]
        index_config = es_config.indexes[job_config.index]

        # Build query and search parameters
        final_query = build_es_query(job_config, index_config)

        search_params = {"query": final_query}
        if index_config.runtime_mappings:
            search_params["runtime_mappings"] = index_config.runtime_mappings

        # Search for posts in Elasticsearch
        posts, raw_data, post_data = process_es_data(
            es_config,
            index_config,
            job_config,
            search_params,
        )

        result = calcCoordination(posts, job_config.speed)

        with open(path_json_tmp, "w", encoding="utf-8") as f:
            json.dump(generate_json_result(result, raw_data, index_config, post_data, es_config, job_config), f)

        # Write final results in the output file
        result.to_csv(path_output_tmp, encoding="utf-8", index=False)

    return None

if __name__ == "__main__":
    calcCoordinationES("es_test")
    calcCoordinationCSV("1_test")
