from flask import Blueprint, request
from ..config import APP_CONFIG

from ..data_sources.es_handler import flatten, fld

post_bp = Blueprint("posts", __name__, url_prefix="/posts")

datastore = APP_CONFIG.datastore.create_datastore()

@post_bp.route("/<job_id>")
def sedPostParam(job_id):
    post_id = request.args.get("id", type=str)
    return sendPost(job_id, post_id)

@post_bp.route("/<job_id>/<post_id>")
def sendPost(job_id, post_id):
    job_config = datastore.fetch_elasticsearch_job(job_id)

    if not job_config:
        return dict(error="unknown job"), 404

    # find the configuration for the Elasticsearch connection and index - if
    # either of these are invalid it will raise an exception
    es_config = APP_CONFIG.elasticsearch[job_config.elasticsearch]
    index_config = es_config.indexes[job_config.index]

    with es_config.connect() as client:
        query = {"term": {index_config.post_id: {"value": post_id}}}

        data = client.search(
            index=index_config.index_name,
            _source=False,
            fields=index_config.field_names(),
            size=1,
            track_total_hits=False,
            query=query,
            runtime_mappings=index_config.runtime_mappings,
        )
        
        # get the single result out of the hits array
        post = data["hits"]["hits"][0]

        flatten(post["fields"])

        # use the field mapping from the config to convert
        # the document into a consistent JSON structure
        return {
            "text": fld(post,index_config.text),
            "timestamp": fld(post,index_config.timestamp),
            "screenName": fld(post,index_config.screen_name),
            "postId": fld(post,index_config.post_id),
            "userId": fld(post,index_config.user_id)
        }
