# This function an HTTP starter function for Durable Functions.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable activity function (default name is "Hello")
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt
 
import azure.durable_functions as adf
import azure.functions as af
import logging

async def main(req: af.HttpRequest, starter: str) -> af.HttpResponse:
# body_json is the json arguments from the Body field in the Data Factory azure function activity.
    body_json = req.get_json()
# Create a client instnce to be used for starting, querying, terminating, and raising events to orchestration instances.
    client = adf.DurableOrchestrationClient(starter)
# functionName is the argument from the Function Name field in Data Factory azure function activity.
    function_name = req.route_params["functionName"]
# Uniquely identify the client instance.
    instance_id = await client.start_new(function_name, None, body_json)
    return client.create_check_status_response(req, instance_id)