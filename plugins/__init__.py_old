from airflow.plugins_manager import AirflowPlugin
from idea_plugin import BigQueryToFeatherOperator

class IdeaPlugin(AirflowPlugin):
    name = "idea_plugin"
    operators = [BigQueryToFeatherOperator]
