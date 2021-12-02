# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
# Before running this sample, please:
# - create a Durable activity function (default name is "Hello")
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import json

import azure.functions as af
import azure.durable_functions as adf


def orchestrator_function(context: adf.DurableOrchestrationContext):
    body = context.get_input()
    logging.info(body)
    result1 = yield context.call_activity('AddressGeocode', body)
    #result2 = yield context.call_activity('GeocodeTimezone')
    return f"Orchestrator: {result1}"  #[result1,result2]

main = adf.Orchestrator.create(orchestrator_function)