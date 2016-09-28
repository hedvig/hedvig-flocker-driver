from flocker.node import BackendDescription, DeployerType
from hedvig_flocker_driver.hedvigdriver import *

def api_factory(cluster_id, **kwargs):
    #return GetHedvigStorageApi(kwargs[u"username"], kwargs[u"password"])
    return GetHedvigStorageApi("", "")

FLOCKER_BACKEND = BackendDescription(
    name=u"hedvig_flocker_driver",
    needs_reactor=False, needs_cluster_id=True,
    api_factory=api_factory, 
    required_config={u"username", u"password"},
    deployer_type=DeployerType.block)
