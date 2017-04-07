#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
Driver for Linux servers running Portworx.

"""

import math
import os
import socket
import subprocess
import sys

from oslo_concurrency import processutils
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import excutils
from oslo_utils import importutils
from oslo_utils import units
import six

from cinder import exception
from cinder.i18n import _
from cinder.image import image_utils
from cinder import interface
from cinder import objects
from cinder import utils
from cinder.volume import driver
from cinder.volume import utils as volutils

LOG = logging.getLogger(__name__)

volume_opts = [
]

CONF = cfg.CONF
CONF.register_opts(volume_opts)


@interface.volumedriver
class PortworxVolumeDriver(driver.VolumeDriver):
    """Executes commands relating to Volumes."""

    VERSION = '0.1.0'

    # ThirdPartySystems wiki page
    CI_WIKI_NAME = "Cinder_Jenkins"

    def __init__(self, vg_obj=None, *args, **kwargs):
        # Parent sets db, host, _execute and base config
        super(PortworxVolumeDriver, self).__init__(*args, **kwargs)
        self.hostname = socket.gethostname()
        self.backend_name = "portworx"

    def do_setup(self, context):
        """Performs driver initialization steps that could raise exceptions."""
        return

    def check_for_setup_error(self):
        return

    def clone_image(self, volume, image_location, image_id, image_metadata, image_service):
        return

    def copy_image_to_volume(self, context, volume, image_service, image_id):
        return

    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        return

    def attach_volume(self, context, volume, instance_uuid, host_name, mountpoint):
        """Attaches a volume. If a mountpoint is passed in, perform a mount as well."""
        LOG.debug('TRYING to attach volume : %s', volume['id'])
        str = "sudo /opt/pwx/bin/pxctl host attach %s" % (volume['id'])
        LOG.debug('TRYING to execute : %s',str)
        try:
            retcode = subprocess.call(str, shell=True)
            if retcode < 0:
                  print >>sys.stderr, "Child was terminated by signal", -retcode
            else:
                  print >>sys.stderr, "Child returned", retcode
        except OSError as e:
                  print >>sys.stderr, "Exception in attaching vol", retcode
        LOG.info('Successfully attached volume: %s', volume['id'])
        if mount_point is None:
            return
        else:
            self.mount_volume(self, context, volume, instance_uuid, host_name, mountpoint)

    def mount_volume(self, context, volume, instance_uuid, host_name, mountpoint):
        """Mount a drive to the volume."""
        LOG.debug('TRYING to mount a drive to volume : %s', volume['id'])
        str = "sudo /opt/pwx/bin/pxctl host mount %s %s" % (volume['id'], mountpoint)
        LOG.debug('TRYING to execute : %s',str)
        try:
            retcode = subprocess.call(str, shell=True)
            if retcode < 0:
                  print >>sys.stderr, "Child was terminated by signal", -retcode
            else:
                  print >>sys.stderr, "Child returned", retcode
        except OSError as e:
                  print >>sys.stderr, "Exception in mounting vol", retcode
        LOG.info('Successfully mounted volume: %s', volume['id'])
        return

    def _update_volume_stats(self):
        """Retrieve stats info from volume group."""

        LOG.debug("Updating volume stats")
        return

        total_capacity = 0
        free_capacity = 0

        total_volumes = 0

        self._stats = "unknown"

    def check_for_setup_error(self):
        """Verify that requirements are in place to use LVM driver."""
        return

    def create_volume(self, volume):
        """Creates a logical volume."""
        LOG.debug('TRYING to create volume : %s', volume['id'])
        str = "sudo /opt/pwx/bin/pxctl volume create %s --size %s" % (volume['name'], volume['size'])
        LOG.debug('TRYING to execute : %s',str)
        try:
            retcode = subprocess.call(str, shell=True)
            if retcode < 0:
                  print >>sys.stderr, "Child was terminated by signal", -retcode
            else:
                  print >>sys.stderr, "Child returned", retcode
        except OSError as e:
                  print >>sys.stderr, "Exception in create vol", retcode
        LOG.info('Successfully created volume: %s', volume['id'])
        return

    def update_migrated_volume(self, ctxt, volume, new_volume,
                               original_volume_status):
        return

    def create_volume_from_snapshot(self, volume, snapshot):
        """Creates a volume from a snapshot."""
        return

    def detach_volume(self, context, volume, attachment=None):
        """Detaches a volume."""
        LOG.debug('TRYING to detach volume : %s', volume['id'])
        str = "sudo /opt/pwx/bin/pxctl host detach -a %s" % (volume['id'])
        LOG.debug('TRYING to execute : %s',str)
        try:
            retcode = subprocess.call(str, shell=True)
            if retcode < 0:
                  print >>sys.stderr, "Child was terminated by signal", -retcode
            else:
                  print >>sys.stderr, "Child returned", retcode
        except OSError as e:
                  print >>sys.stderr, "Exception in detaching vol", retcode
        LOG.info('Successfully detached volume: %s', volume['id'])
        return

    def delete_volume(self, volume):
        """Deletes a logical volume."""
        LOG.debug('TRYING to delete volume : %s', volume['id'])
        str = "sudo /opt/pwx/bin/pxctl volume delete volume-%s" % (volume['id'])
        LOG.debug('TRYING to execute : %s',str)
        try:
            retcode = subprocess.call(str, shell=True)
            if retcode < 0:
                  print >>sys.stderr, "Child was terminated by signal", -retcode
            else:
                  print >>sys.stderr, "Child returned", retcode
        except OSError as e:
                  print >>sys.stderr, "Exception in delete vol", retcode
        LOG.info('Successfully deleted  volume: %s', volume['id'])
        return True

    def create_snapshot(self, snapshot):
        """Creates a snapshot."""
        volume_name = snapshot['volume_name']
        snap_name = snapshot['name']
        LOG.debug('TRYING to create snapshot for volume : %s', volume_name)
        str = "sudo /opt/pwx/bin/pxctl snap create --name %s %s" % (snap_name, volume_name)
        LOG.debug('TRYING to execute create snap: %s',str)
        try:
            retcode = subprocess.call(str, shell=True)
            if retcode < 0:
                  print >>sys.stderr, "Child was terminated by signal", -retcode
            else:
                  print >>sys.stderr, "Child returned", retcode
        except OSError as e:
                  print >>sys.stderr, "Exception in create snap", retcode
        LOG.info('Successfully created snapshot: %s', snap_name)
        return True

    def delete_snapshot(self, snapshot):
        snap_name = snapshot['name']
        LOG.debug('TRYING to delete snapshot : %s', snap_name)
        str = "sudo /opt/pwx/bin/pxctl snap delete %s" % (snap_name)
        LOG.debug('TRYING to execute delete snap: %s',str)
        try:
            retcode = subprocess.call(str, shell=True)
            if retcode < 0:
                  print >>sys.stderr, "Child was terminated by signal", -retcode
            else:
                  print >>sys.stderr, "Child returned", retcode
        except OSError as e:
                  print >>sys.stderr, "Exception in delete snap", retcode

        """Deletes a snapshot."""
        LOG.info('Successfully deleted snapshot: %s', snap_name)
        return True


    def local_path(self, volume):
        return "/dev/pxd/whatever"

    def copy_image_to_volume(self, context, volume, image_service, image_id):
        """Fetch the image from image_service and write it to the volume."""

    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        """Copy the volume to the specified image."""

    def create_cloned_volume(self, volume, src_vref):
        """Creates a clone of the specified volume."""
        return

    def clone_image(self, context, volume,
                    image_location, image_meta,
                    image_service):
        return None, False

    def backup_volume(self, context, backup, backup_service):
        """Create a new backup from an existing volume."""

    def restore_backup(self, context, backup, volume, backup_service):
        """Restore an existing backup to a new or existing volume."""

    def get_volume_stats(self, refresh=False):
        """Get volume status.
        """
        # Start with some required info
        data = dict(
            volume_backend_name=self.backend_name,
            vendor_name='Portworx',
            driver_version=self.VERSION,
            storage_protocol='',
            total_capacity_gb='infinite',
            free_capacity_gb='infinite',
        )

        return data

    def extend_volume(self, volume, new_size):
        """Extend an existing volume's size."""

    def manage_existing(self, volume, existing_ref):
        """Manages an existing LV.

        Renames the LV to match the expected name for the volume.
        Error checking done by manage_existing_get_size is not repeated.
        """

    def manage_existing_object_get_size(self, existing_object, existing_ref,
                                        object_type):
        """Return size of an existing LV for manage existing volume/snapshot.

        existing_ref is a dictionary of the form:
        {'source-name': <name of LV>}
        """


    def manage_existing_get_size(self, volume, existing_ref):
        return 0

    def manage_existing_snapshot_get_size(self, snapshot, existing_ref):
        return 0

    def manage_existing_snapshot(self, snapshot, existing_ref):
        return 0

    def get_manageable_volumes(self, cinder_volumes, marker, limit, offset,
                               sort_keys, sort_dirs):
        return 0

    def get_manageable_snapshots(self, cinder_snapshots, marker, limit, offset,
                                 sort_keys, sort_dirs):
        return 0

    def retype(self, context, volume, new_type, diff, host):
        """Retypes a volume, allow QoS and extra_specs change."""
        return True

    def migrate_volume(self, ctxt, volume, host, thin=False, mirror_count=0):
        """Optimize the migration if the destination is on the same server.

        If the specified host is another back-end on the same server, and
        the volume is not attached, we can do the migration locally without
        going through iSCSI.
        """


    def get_pool(self, volume):
        return self.backend_name

    # #######  Interface methods for DataPath (Target Driver) ########

    def ensure_export(self, context, volume):
        volume_path = "/dev/%s/%s" % (self.configuration.volume_group,
                                      volume['name'])
        return model_update

    def create_export(self, context, volume, connector, vg=None):
        return 0


    def remove_export(self, context, volume):
        return 0

    def initialize_connection(self, volume, connector):
        return True

    def validate_connector(self, connector):
        return True

    def terminate_connection(self, volume, connector, **kwargs):
        return True
