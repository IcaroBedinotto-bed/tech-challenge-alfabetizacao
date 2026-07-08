from pprint import pprint

from techchallenge.profiling.profiling_service import ProfilingService

service = ProfilingService()

report = service.profile_bronze()

pprint(report)