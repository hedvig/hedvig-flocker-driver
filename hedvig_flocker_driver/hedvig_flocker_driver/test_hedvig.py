
from twisted.trial.unittest import SynchronousTestCase
from uuid import uuid4
from bitmath import Byte, GiB
from flocker.testtools import skip_except
from hedvig_flocker_driver import hedvigdriver 
from flocker.node.agents.test.test_blockdevice import (
    make_iblockdeviceapi_tests
)

def GetTestHedvigStorage(test_case):
    hedvigClient = hedvigdriver.GetHedvigStorageApi("u1", "p1")
    test_case.addCleanup(hedvigClient._cleanup)
    return hedvigClient

#@skip_except(
#    supported_tests=[
#        'test_interface',
#        'test_reattach_detached_volume',
#	]
#)
class HedvigBlockDeviceAPIInterfaceTests(
        make_iblockdeviceapi_tests(
            blockdevice_api_factory=(
                lambda test_case: GetTestHedvigStorage(test_case)
            ),
            minimum_allocatable_size=int(GiB(8).to_Byte().value),
            device_allocation_unit=int(GiB(8).to_Byte().value),
            unknown_blockdevice_id_factory=lambda test: unicode(uuid4())
        )
):
    """
    Interface adherence Tests for ``HedvigBlockDeviceAPI``
    """
class HedvigBlockDeviceAPIImplementationTests(SynchronousTestCase):
    """
    Implementation specific tests for ``HedvigBlockDeviceAPI``.
    """
    def test_hedvig_api(self):
        """
        Test HedvigBlockDeviceAPI Login
        """

