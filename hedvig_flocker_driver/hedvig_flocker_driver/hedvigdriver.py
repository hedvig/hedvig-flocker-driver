
import sys
sys.path.append("/root/flocker/objstore/hedvig/common/")
import time
from uuid import UUID
import logging
import json
from zope.interface import implementer, Interface
import socket
from urlparse import urlparse

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol
from qbcommon.ttypes import *
from qbpages.ttypes import *
from hedvig.common.HedvigLogger import HedvigLogger
HedvigLogger.setDefaultLogFile("flocker-driver.log")
from hedvig.common.HedvigConfig import HedvigConfig
from hedvig.common.PagesProxy import PagesProxy
from hedvig.common.HedvigControllerProxy import HedvigControllerProxy
from hedvig.common.HedvigUtility import *
from subprocess import call
import subprocess
import os
from twisted.python.filepath import FilePath
import pdb


from flocker.node.agents.blockdevice import (
    AlreadyAttachedVolume, IBlockDeviceAPI,
    BlockDeviceVolume, UnknownVolume, UnattachedVolume
)


def GetHedvigStorageApi(username, password):
    return HedvigBlockDeviceAPI(username, password)


@implementer(IBlockDeviceAPI)
class HedvigBlockDeviceAPI(object):

    defaultVolumeBlkSize_ = 4096
    defaultCreatedBy_ = "Hedvig_Flocker_driver"
    defaultExportedBlkSize_ = 4096

    def __init__(self, username, password):
        """
        :returns: A ``BlockDeviceVolume``.
        """
        self.logger_ = HedvigLogger.getLogger("HedvigBlockDeviceAPI")
        self._username = username
        self._password = password
        self._instance_id = self.compute_instance_id()

    def allocation_unit(self):
        """
        return int: 1 GB
        """
        return 1024*1024*1024

    def _cleanup(self):
        """
        Remove all Hedvig volumes
        """
        volumes = self.list_volumes()
        for volume in volumes:
            self.destroy_volume(volume.blockdevice_id)

    def compute_instance_id(self):
        """
        Return current node's hostname
        """
        #return unicode(socket.gethostbyname(socket.getfqdn()))
        return unicode('10.8.4.10')

    def create_volume(self, dataset_id, size):
        """
        Create a new volume.
        :param UUID dataset_id: The Flocker dataset ID of the dataset on this
            volume.
        :param int size: The size of the new volume in bytes.
        :returns: A ``BlockDeviceVolume``.
        """
        """Driver entry point for creating a new volume."""
        name = str(dataset_id)
        description = str(dataset_id)
        volumeType = ''
        try:
            self.logger_.debug("create_volume: name:%s:description:%s:size:%s", name, description, size)
            vDiskInfo = VDiskInfo()
            vDiskInfo.vDiskName = name
            vDiskInfo.blockSize = HedvigBlockDeviceAPI.defaultVolumeBlkSize_
            vDiskInfo.size = size
            vDiskInfo.createdBy = HedvigBlockDeviceAPI.defaultCreatedBy_
            vDiskInfo.description = description
            vDiskInfo.replicationFactor = 3
            vDiskInfo.exportedBlockSize = HedvigBlockDeviceAPI.defaultExportedBlkSize_
            vDiskInfo.diskType = DiskType.BLOCK
            vDiskInfo.dedup = 'false'
            vDiskInfo.compressed = 'false'
            hedvigCreateVirtualDisk(vDiskInfo, self.logger_)
        except Exception as e:
            self.logger_.exception("error creating volume:name:%s:description:%s:size:%s", name, description, size)
            self.logger_.debug("error creating volume:name:%s:description:%s:size:%s", name, description, size)

    	return BlockDeviceVolume(
			    blockdevice_id=unicode(dataset_id),
			    size=size,
			    attached_to=None,
			    dataset_id=dataset_id)

    def destroy_volume(self, blockdevice_id):
        """
        Destroy an existing volume.

        :param unicode blockdevice_id: The unique identifier for the volume to
            destroy.

        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.

        :return: ``None``
        """
        if (findVDisk(str(blockdevice_id)) == None):
	    	raise UnknownVolume(blockdevice_id)
        pagesProxy = PagesProxy()
        pagesProxy.deleteVirtualDisk(str(blockdevice_id))
        # Remove when delete issue is fixed
        time.sleep(1)

    def attach_volume(self, blockdevice_id, attach_to):
        """
        Attach ``blockdevice_id`` to ``host``.

        :param unicode blockdevice_id: The unique identifier for the block
            device being attached.
        :param unicode attach_to: An identifier like the one returned by the
            ``compute_instance_id`` method indicating the node to which to
            attach the volume.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :raises AlreadyAttachedVolume: If the supplied ``blockdevice_id`` is
            already attached.
        :returns: A ``BlockDeviceVolume`` with a ``host`` attribute set to
            ``host``.
        """
        self.logger_.error("**** attach_volume called ****")
        volName = str(blockdevice_id)
        vdiskInfo = findVDisk(volName)
        if (vdiskInfo == None):
            raise UnknownVolume(blockdevice_id)

        computeHost = socket.getfqdn(attach_to)
        try:
            tgtHost = hedvigLookupTgt('wood', self.logger_)
            lunnum = hedvigGetLun(tgtHost, volName, self.logger_)
        except:
            raise Exception("Failed to get Lun number")
        if lunnum == -1:
                self.logger_.error("failed to add vDiskName:%s:tgtHost:%s", volName, tgtHost)
                raise Exception("Failed to add Lun")
        if ( self._is_attached(tgtHost, lunnum) != None):
		        raise AlreadyAttachedVolume(blockdevice_id)
        try:
                hedvigAddAccess(tgtHost, lunnum, socket.gethostbyname(socket.getfqdn(computeHost)), self.logger_)
                self.logger_.error("**** addAccess-1 done ****")
                hedvigAddAccess(tgtHost, lunnum, socket.gethostbyname(socket.getfqdn()), self.logger_)
                self.logger_.error("**** addAccess-2 done ****")
                targetName, portal = hedvigDoIscsiDiscovery(tgtHost, lunnum, self.logger_)
                self.logger_.error("**** iscsiDiscovery done ****")
        except Exception as e:
            	self.logger_.exception("volume assignment to connector failed :volume:%s:connector:%s", volName, attach_to)
                return None
        return BlockDeviceVolume(
			    blockdevice_id=unicode(blockdevice_id),
			    size=vdiskInfo.size,
			    attached_to=attach_to,
			    dataset_id=UUID(blockdevice_id))

    def detach_volume(self, blockdevice_id):
        """
        Detach ``blockdevice_id`` from whatever host it is attached to.

        :param unicode blockdevice_id: The unique identifier for the block
            device being detached.

        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :raises UnattachedVolume: If the supplied ``blockdevice_id`` is
            not attached to anything.
        :returns: ``None``
        """
        self.logger_.error("**** detach_volume called ****")
        volName = str(blockdevice_id)
        vdiskInfo = findVDisk(volName)
        if (vdiskInfo == None):
		    raise UnknownVolume(blockdevice_id)

        computeHost = self.compute_instance_id()
        try:
            tgtHost = hedvigLookupTgt('wood', self.logger_)
            lunnum = hedvigGetLun(tgtHost, volName, self.logger_)
        except:
            raise UnattachedVolume(blockdevice_id)
        if lunnum == -1:
            raise UnattachedVolume(blockdevice_id)
        if ( self._is_attached(tgtHost, lunnum) == None):
            raise UnattachedVolume(blockdevice_id)
        devicepath = self.get_device_path(blockdevice_id)
        try:
            self.logger_.error("**** trying logout ****")
            self.logout(blockdevice_id)
            self.logger_.error("**** logout done ****")
            hedvigRemoveAccess(tgtHost, lunnum, socket.gethostbyname(socket.getfqdn(computeHost)), devicepath.path,
                    self.logger_)
            self.logger_.error("**** removeAccess-1 done ****")
            hedvigRemoveAccess(tgtHost, lunnum, socket.gethostbyname(socket.getfqdn()), devicepath.path,
                    self.logger_)
            self.logger_.error("**** removeAccess-2 done ****")
        except Exception as e:
            print e
            raise Exception("Not able to detach volume with blockdevice_id: %s" % blockdevice_id)

    def logout(self, blockdevice_id):
        """
        """
        self.logger_.error("**** inside logout ****")
        volName = str(blockdevice_id)
        if (findVDisk(volName) == None):
            raise UnknownVolume(blockdevice_id)
        tgtHost = hedvigLookupTgt('wood', self.logger_)
        lunnum = hedvigGetLun(tgtHost, volName, self.logger_)
        try:
            self.logger_.error("**** inside logout: iscsiDiscovery ****")
            targetName, portal = hedvigDoIscsiDiscovery(tgtHost, lunnum, self.logger_)
        except Exception as e:
            targetName = None
        if (targetName == None):
            raise UnattachedVolume(blockdevice_id)
        self.logger_.error("**** inside logout: hedvigIscsiLogout ****")
        hedvigDoIscsiLogout(targetName, portal, self.logger_)
        self.logger_.error("**** inside logout: hedvigIscsiLogout done ****")

    def _is_attached(self, tgtHost, lunnum):
        attached_to = None
        try:
		    targetName, portal = hedvigDoIscsiDiscovery(tgtHost, lunnum, self.logger_)
        except:
		    portal = None
        if (portal is not None):
		    attached_to = self.compute_instance_id()
        return attached_to

    def list_volumes(self):
        """
        List all the block devices available via the back end API.

        :returns: A ``list`` of ``BlockDeviceVolume``s.
        """
        pagesProxy = PagesProxy()
        vdiskInfos = pagesProxy.listVDisks()
        tgtHost = hedvigLookupTgt('wood', self.logger_)
        volumes = []
        for vdiskInfo in vdiskInfos:
            try:
                vDiskInfo = pagesProxy.describeVDisk(vdiskInfo.vDiskName)
            except Exception as e:
                continue
            try:
                lunnum = hedvigGetLun(tgtHost, vdiskInfo.vDiskName, self.logger_)
                attached_to = self._is_attached(tgtHost, lunnum)
            except Exception as e:
                attached_to=None
            volumes.append(BlockDeviceVolume(
                blockdevice_id=unicode(vdiskInfo.vDiskName),
                size=vdiskInfo.size,
                attached_to=attached_to,
                dataset_id=UUID(vdiskInfo.vDiskName)))
        return volumes

    def _find_path(self, tgtHost, lunnum):
        p = subprocess.Popen(["ls", "/dev/disk/by-path/"], stdout=subprocess.PIPE)
        (retOut, retErr) = p.communicate()
        for line in retOut.strip(" ").split("\n"):
            if (tgtHost in line):
                lun = line[line.rfind("-")+1:]
                if (int(lun) == lunnum):
                    link = os.readlink("/dev/disk/by-path/" + line)
                    return "/dev/" + link[link.rfind("/")+1:]


    def temp(self):
        hedvigLookupTgt('wood', self.logger_)

    def get_device_path(self, blockdevice_id):
        """
        Return the device path that has been allocated to the block device on
        the host to which it is currently attached.

        :param unicode blockdevice_id: The unique identifier for the block
            device.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :raises UnattachedVolume: If the supplied ``blockdevice_id`` is
            not attached to a host.
        :returns: A ``FilePath`` for the device.
        """
        volName = str(blockdevice_id)
        if (findVDisk(volName) == None):
	    	raise UnknownVolume(blockdevice_id)
        tgtHost = hedvigLookupTgt('wood', self.logger_)
        lunnum = hedvigGetLun(tgtHost, volName, self.logger_)
        try:
            targetName, portal = hedvigDoIscsiDiscovery(tgtHost, lunnum, self.logger_)
        except Exception as e:
            targetName = None
        if (targetName == None):
            raise UnattachedVolume(blockdevice_id)
        path = self._find_path(tgtHost, lunnum)
        if (path != None):
            return FilePath(path)
        hedvigDoIscsiLogout(targetName, portal, self.logger_)
        hedvigDoIscsiLogin(targetName, portal, self.logger_)
        # May need to retry
        for i in range (1, 10):
            path = self._find_path(tgtHost, lunnum)
            if (path != None):
                return FilePath(path)
            i += 1
            time.sleep(1)

def testHedvig():
    hdev = HedvigBlockDeviceAPI('','')
    #hdev.temp()
    #tgtHost = hedvigLookupTgt('wood')
    hdev.create_volume(UUID('7962b8ae-35cb-4b4e-aaa8-e04dc8aec2b8'), 1000000000)
    #hdev.attach_volume(9, 'seamusclnt1.hedviginc.com')

#testHedvig()
