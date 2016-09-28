
import sys
sys.path.append("/root/flocker/objstore/hedvig/common/")
import time
from uuid import UUID
import logging
import json
from zope.interface import implementer, Interface
import socket
from urlparse import urlparse

import common.hedvigospyc
from common.HedvigCommon import *
from common.HedvigLogger import HedvigLogger
from common.HedvigConfig import HedvigConfig
from subprocess import call
import subprocess
import os
from twisted.python.filepath import FilePath
from twisted.python.constants import Values, ValueConstant
import pdb
import uuid

from flocker.node.agents.blockdevice import (
    AlreadyAttachedVolume, IBlockDeviceAPI, IProfiledBlockDeviceAPI,
    BlockDeviceVolume, UnknownVolume, UnattachedVolume
)


def GetHedvigStorageApi(username, password):
    return HedvigBlockDeviceAPI(username, password)

class VolumeProfiles(Values):
    """
    :ivar GOLD: The profile for fast storage.
    :ivar SILVER: The profile for intermediate/default storage.
    :ivar BRONZE: The profile for cheap storage.
    :ivar DEFAULT: The default profile if none is specified.
    """
    GOLD = 'gold'
    SILVER = 'silver'
    BRONZE = 'bronze'
    DEFAULT = 'gold'

@implementer(IBlockDeviceAPI)
@implementer(IProfiledBlockDeviceAPI)
class HedvigBlockDeviceAPI(object):

    defaultVolumeBlkSize_ = 4096
    defaultCreatedBy_ = "Hedvig_Flocker_driver"
    defaultExportedBlkSize_ = 4096
    hedvigConfigFilePath_ = "/var/log/hedvig/config.xml"

    def __init__(self, username, password):
        """
        :returns: A ``BlockDeviceVolume``.
        """
        HedvigConfig.getInstance().initConfig(HedvigBlockDeviceAPI.hedvigConfigFilePath_)
        self.logger_ = HedvigLogger.getLogger(loggerName = "HedvigBlockDeviceAPI", logFileName = "flocker-driver.log")
        self._username = username
        self._password = password
        self._instance_id = self.compute_instance_id()
        # 8 threads
        HedvigOpCb.initHedvigOS(8)
        self._tgtHost = hedvigLookupTgt(hedvigGetClusterName(self.logger_), self.logger_)
        self._volumes = {}
        self._volumesPath = {}

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
        HedvigOpCb.shutdownHedvigOS();

    def compute_instance_id(self):
        """
        Return current node's hostname
        """
        return unicode(socket.gethostbyname(socket.getfqdn()))

    def _create_hedvig_volume_specs(self, dataset_id, size, profile_name):
        """
        Create specs for new volume.
        :param UUID dataset_id: The Flocker dataset ID of the dataset on this
            volume.
        :param int size: The size of the new volume in bytes.
        :profile_name: GOLD/SILVER/BRONZE
        :returns: A ``BlockDeviceVolume``.
        """

        name = str(dataset_id)
        description = str(dataset_id) + '_' + str(profile_name)
        volumeType = ''
        vDiskInfo = hos.VDiskInfo()
        vDiskInfo.vDiskName = name
        vDiskInfo.blockSize = HedvigBlockDeviceAPI.defaultVolumeBlkSize_
        vDiskInfo.size = size
        vDiskInfo.createdBy = HedvigBlockDeviceAPI.defaultCreatedBy_
        vDiskInfo.description = description
        vDiskInfo.replicationFactor = 3
        vDiskInfo.exportedBlockSize = HedvigBlockDeviceAPI.defaultExportedBlkSize_
        vDiskInfo.diskType = hos.DiskType.BLOCK

        if (profile_name.lower() == VolumeProfiles.GOLD):
                vDiskInfo.cacheEnable = True
                vDiskInfo.dedup = False
                vDiskInfo.compressed = False
                vDiskInfo.residence = 1
        elif (profile_name.lower() == VolumeProfiles.SILVER):
                vDiskInfo.cacheEnable = True
                vDiskInfo.dedup = True
                vDiskInfo.compressed = True
                vDiskInfo.residence = 1
        elif (profile_name.lower() == VolumeProfiles.BRONZE):
                vDiskInfo.cacheEnable = False
                vDiskInfo.dedup = True
                vDiskInfo.compressed = True
                vDiskInfo.residence = 1
        elif (profile_name.lower() == VolumeProfiles.DEFAULT):
                vDiskInfo.cacheEnable = False
                vDiskInfo.dedup = True
                vDiskInfo.compressed = True
                vDiskInfo.residence = 1
        else:
            self.logger_.exception("Invalid profile:%s", profile_name)
        return vDiskInfo;

    def _create_hedvig_volume_with_profile(self, vDiskInfo, dataset_id):
        """
        """
        try:
            hedvigCreateVirtualDiskWithInfo(vDiskInfo, self.logger_)
        except Exception as e:
            self.logger_.exception("error creating volume:name:%s", vDiskInfo.vDiskName)
            raise e
        vol = BlockDeviceVolume(
			    blockdevice_id=unicode(dataset_id),
			    size=vDiskInfo.size,
			    attached_to=None,
			    dataset_id=dataset_id)
        self._volumes[vDiskInfo.vDiskName] = vol
        return vol

    def create_volume_with_profile(self, dataset_id, size, profile_name):
        """
        Create a new volume.
        :param UUID dataset_id: The Flocker dataset ID of the dataset on this
            volume.
        :param int size: The size of the new volume in bytes.
        :param string profile_name: One of three options (GOLD, SILVER, BRONZE)
        :returns: A ``BlockDeviceVolume``.
        """

        """ Driver entry point for creating a new volume """
        self.logger_.info("create_volume: name:%s:size:%s", dataset_id, size)
        vDiskInfo = self._create_hedvig_volume_specs(dataset_id, size, profile_name);
        return self._create_hedvig_volume_with_profile(vDiskInfo, dataset_id)

    def create_volume(self, dataset_id, size):
        """
        Create a new volume.
        :param UUID dataset_id: The Flocker dataset ID of the dataset on this
            volume.
        :param int size: The size of the new volume in bytes.
        :returns: A ``BlockDeviceVolume``.
        """
        """ Driver entry point for creating a new volume Use DEFAULT profile """
        self.logger_.info("create_volume: name:%s:size:%s", dataset_id, size)
        vDiskInfo = self._create_hedvig_volume_specs(dataset_id, size, VolumeProfiles.DEFAULT);
        return self._create_hedvig_volume_with_profile(vDiskInfo, dataset_id)

    def destroy_volume(self, blockdevice_id):
        """
        Destroy an existing volume.

        :param unicode blockdevice_id: The unique identifier for the volume to
            destroy.

        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.

        :return: ``None``
        """
        self.logger_.info("destroy_volume: name:%s", blockdevice_id)
        volName = str(blockdevice_id)
        try:
            vDiskInfo = hedvigDescribeVDisk(volName, self.logger_)
        except Exception as e:
	    	raise UnknownVolume(blockdevice_id)
        if (volName != vDiskInfo.vDiskName):    
	    	raise UnknownVolume(blockdevice_id)
        try:
            lunnum = hedvigGetLun(self._tgtHost, volName, self.logger_)
            if lunnum > -1:
                hedvigDeleteLun(self._tgtHost, volName, lunnum, self.logger_)
            hedvigDeleteVirtualDisk(volName, self.logger_)
            if (volName in self._volumesPath):
                self._volumesPath.pop(volName)
            if (volName in self._volumes):
                self._volumes.pop(volName)
        except Exception as ex:
            self.logger_.error("Got Exception in destroy_volume:%s", ex)

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
        self.logger_.info("Attching blockdevice_id:%s", blockdevice_id)
        volName = str(blockdevice_id)
        try:
            vDiskInfo = hedvigDescribeVDisk(volName, self.logger_)
        except Exception as ex:
            raise UnknownVolume(unicode(volName))

        if (volName != vDiskInfo.vDiskName):    
            raise UnknownVolume(blockdevice_id)

        """
        if (volName in self._volumesPath):
            return BlockDeviceVolume(
                    blockdevice_id=unicode(volName),
                    size=vDiskInfo.size,
                    attached_to=unicode(attach_to),
                    dataset_id=UUID(volName))
        """
        computeHost = socket.getfqdn(attach_to)
        try:
            lunnum = hedvigGetLun(self._tgtHost, volName, self.logger_)
        except Exception as ex:
            self.logger_.error("Failed to get Lun number2:%s", ex)
            raise Exception("Failed to get Lun number")
        if lunnum <= -1:
            self.logger_.error("failed to add vDiskName:%s:tgtHost:%s lun:%s", volName, self._tgtHost, str(lunnum))
            raise Exception("Failed to add Lun")
        if ( self._is_attached(lunnum) != None):
            self.get_device_path(blockdevice_id)
            vol = BlockDeviceVolume(
                    blockdevice_id=unicode(volName),
                    size=vDiskInfo.size,
                    attached_to=unicode(attach_to),
                    dataset_id=UUID(volName))
            self._volumes.pop(volName)
            self._volumes[volName] = vol
            raise AlreadyAttachedVolume(unicode(volName))
        try:
                hedvigAddAccess(vDiskInfo.vDiskName, self._tgtHost, lunnum, socket.gethostbyname(socket.getfqdn(computeHost)), self.logger_)
                hedvigAddAccess(vDiskInfo.vDiskName, self._tgtHost, lunnum, socket.gethostbyname(socket.getfqdn()), self.logger_)
                targetName, portal = hedvigDoIscsiDiscovery(self._tgtHost, lunnum, self.logger_)
        except Exception as e:
            	self.logger_.exception("volume assignment to connector failed :volume:%s:connector:%s", volName, attach_to)
                return None
        vol = self._volumes[volName]
        vol = BlockDeviceVolume(
			    blockdevice_id=unicode(volName),
			    size=vDiskInfo.size,
			    attached_to=unicode(attach_to),
			    dataset_id=UUID(volName))
        if (volName in self._volumes):
            self._volumes.pop(volName)
            self._volumes[volName] = vol
        return vol

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
        self.logger_.info("Detaching blockdevice_id:%s", blockdevice_id)
        volName = str(blockdevice_id)
        try:
            vDiskInfo = hedvigDescribeVDisk(volName, self.logger_)
        except Exception as e:
	    	raise UnknownVolume(blockdevice_id)
        if (volName != vDiskInfo.vDiskName):    
            raise UnknownVolume(blockdevice_id)

        computeHost = self.compute_instance_id()
        try:
            lunnum = hedvigGetLun(self._tgtHost, volName, self.logger_)
        except:
            raise UnattachedVolume(blockdevice_id)
        if lunnum <= -1:
            raise UnattachedVolume(blockdevice_id)
        if ( self._is_attached(lunnum) == None):
            if (volName in self._volumesPath):
                self._volumesPath.pop(volName)
            if (volName in self._volumes):
                self._volumes.pop(volName)
            raise UnattachedVolume(blockdevice_id)
        devicepath = self.get_device_path(blockdevice_id)
        try:
            self.logout(blockdevice_id)
            hedvigRemoveAccess(volName, self._tgtHost, lunnum, socket.gethostbyname(socket.getfqdn(computeHost)), 
                    devicepath, self.logger_)
            hedvigRemoveAccess(volName, self._tgtHost, lunnum, socket.gethostbyname(socket.getfqdn()), 
                    devicepath, self.logger_)
        except Exception as e:
            print e
            raise Exception("Not able to detach volume with blockdevice_id: %s" % blockdevice_id)
        if (volName in self._volumesPath):
            self._volumesPath.pop(volName)

    def logout(self, blockdevice_id):
        """
        """
        self.logger_.info("LOGOUT:%s", blockdevice_id)
        volName = str(blockdevice_id)
        try:
            vDiskInfo = hedvigDescribeVDisk(volName, self.logger_)
            if (volName != vDiskInfo.vDiskName):    
                raise UnknownVolume(blockdevice_id)
            lunnum = hedvigGetLun(self._tgtHost, volName, self.logger_)
        except Exception as ex:
            self.logger_.error("Got Exception in logout:%s", ex)
        try:
            targetName, portal = hedvigDoIscsiDiscovery(self._tgtHost, lunnum, self.logger_)
        except Exception as e:
            targetName = None
        if (targetName == None):
            raise UnattachedVolume(blockdevice_id)
        try:
        	hedvigDoIscsiLogout(targetName, portal, self.logger_)
        except Exception as e:
                pass

    def _is_attached(self, lunnum):
        attached_to = None
        try:
		    targetName, portal = hedvigDoIscsiDiscovery(self._tgtHost, lunnum, self.logger_)
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
        """
        if (len (self._volumes) != 0):
            self.logger_.info("list_volumes:size:%d", len(self._volumes))
            return self._volumes.values()
        """
        try:
            vDisks = hedvigListVDisks(self.logger_)
        except Exception as ex:
            self.logger_.error("Got Exception in list_volumes:%s", ex)
            return

        volumes = []
        for vDiskName in vDisks:
            try:
                vDiskInfo = hedvigDescribeVDisk(vDiskName, self.logger_)
            except Exception as e:
                continue
            if (vDiskName == vDiskInfo.vDiskName):    
                try:
                    val = UUID(vDiskInfo.vDiskName)
                except Exception as e:
                    continue
                try:
                    lunnum = hedvigGetLun(self._tgtHost, vDiskInfo.vDiskName, self.logger_)
                    attached_to = self._is_attached(lunnum)
                except Exception as e:
                    attached_to=None
                vol = BlockDeviceVolume(
                        blockdevice_id=unicode(vDiskInfo.vDiskName),
                        size=vDiskInfo.size,
                        attached_to=attached_to,
                        dataset_id=UUID(vDiskInfo.vDiskName))
                volumes.append(vol)
                self._volumes[vDiskInfo.vDiskName] = vol
        return volumes

    def _find_path(self, lunnum):
        p = subprocess.Popen(["ls", "/dev/disk/by-path/"], stdout=subprocess.PIPE)
        (retOut, retErr) = p.communicate()
        for line in retOut.strip(" ").split("\n"):
            if (self._tgtHost in line):
                lun = line[line.rfind("-")+1:]
                if (int(lun) == lunnum):
                    link = os.readlink("/dev/disk/by-path/" + line)
                    return "/dev/" + link[link.rfind("/")+1:]


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
        self.logger_.info("get_device_path:%s", blockdevice_id)
        volName = str(blockdevice_id)
        if (volName in self._volumesPath):
            path = self._volumesPath[volName]
            return FilePath(path)
        try:
            vDiskInfo = hedvigDescribeVDisk(volName, self.logger_)
        except Exception as e:
	    	raise UnknownVolume(blockdevice_id)
        if (volName != vDiskInfo.vDiskName):    
	    	raise UnknownVolume(blockdevice_id)
        try:
            lunnum = hedvigGetLun(self._tgtHost, volName, self.logger_)
            targetName, portal = hedvigDoIscsiDiscovery(self._tgtHost, lunnum, self.logger_)
        except Exception as ex:
            self.logger_.error("Got Exception in get_device_path:%s", ex)
            targetName = None
        if (targetName == None):
            raise UnattachedVolume(unicode(blockdevice_id))
        path = self._find_path(lunnum)
        if (path != None):
            return FilePath(path)
        try:
        	hedvigDoIscsiLogout(targetName, portal, self.logger_)
        	hedvigDoIscsiLogin(targetName, portal, self.logger_)
        except Exception as e:
            pass

        # May need to retry
        for i in range (1, 10):
            path = self._find_path(lunnum)
            if (path != None):
                self._volumesPath[volName] = path
                return FilePath(path)
            i += 1
            time.sleep(1)

def testHedvig():
    hdev = HedvigBlockDeviceAPI('','')
    id = uuid.uuid4()
    hdev.create_volume(id, 1000000000)
    #UUID('7962b8ae-35cb-4b4e-aaa8-e04dc8aec2b1'), 1000000000)
    #print hdev.list_volumes()
    hdev.attach_volume(id, hdev.compute_instance_id())
    #print hdev.list_volumes()
    hdev.get_device_path(id)
    hdev.detach_volume(id)
    #hdev.destroy_volume(id)
    try:
        hdev.attach_volume(id, hdev.compute_instance_id())
    except Exception as e:
        print "ignoring"

    #hdev.get_device_path(id)
    #hdev.detach_volume(UUID('7962b8ae-35cb-4b4e-aaa8-e04dc8aec2b8'))
    #hdev.destroy_volume(id)
    #time.sleep(5)
    """
    """
    print "Shutting down"
    hdev._cleanup()

#testHedvig()
