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
Driver for Linux servers running LVM.

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

from cinder.brick.local_dev import lvm as lvm
from cinder import exception
from cinder.i18n import _
from cinder.image import image_utils
from cinder import interface
from cinder import objects
from cinder import utils
from cinder.volume import driver
from cinder.volume import utils as volutils

LOG = logging.getLogger(__name__)

# FIXME(jdg):  We'll put the lvm_ prefix back on these when we
# move over to using this as the real LVM driver, for now we'll
# rename them so that the config generation utility doesn't barf
# on duplicate entries.
volume_opts = [
    cfg.StrOpt('volume_group',
               default='cinder-volumes',
               help='Name for the VG that will contain exported volumes'),
    cfg.IntOpt('lvm_mirrors',
               default=0,
               help='If >0, create LVs with multiple mirrors. Note that '
                    'this requires lvm_mirrors + 2 PVs with available space'),
    cfg.StrOpt('lvm_type',
               default='default',
               choices=['default', 'thin', 'auto'],
               help='Type of LVM volumes to deploy; (default, thin, or auto). '
                    'Auto defaults to thin if thin is supported.'),
    cfg.StrOpt('lvm_conf_file',
               default='/etc/cinder/lvm.conf',
               help='LVM conf file to use for the LVM driver in Cinder; '
                    'this setting is ignored if the specified file does '
                    'not exist (You can also specify \'None\' to not use '
                    'a conf file even if one exists).'),
    cfg.FloatOpt('lvm_max_over_subscription_ratio',
                 # This option exists to provide a default value for the
                 # LVM driver which is different than the global default.
                 default=1.0,
                 help='max_over_subscription_ratio setting for the LVM '
                      'driver.  If set, this takes precedence over the '
                      'general max_over_subscription_ratio option.  If '
                      'None, the general option is used.'),
    cfg.BoolOpt('lvm_suppress_fd_warnings',
                default=False,
                help='Suppress leaked file descriptor warnings in LVM '
                     'commands.')
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
        return

    def _sizestr(self, size_in_g):
        return '%sg' % size_in_g

    def _volume_not_present(self, volume_name):
        return self.vg.get_volume(volume_name) is None

    def _clear_volume(self, volume, is_snapshot=False):
        # zero out old volumes to prevent data leaking between users
        # TODO(ja): reclaiming space should be done lazy and low priority
        if is_snapshot:
            # if the volume to be cleared is a snapshot of another volume
            # we need to clear out the volume using the -cow instead of the
            # directly volume path.  We need to skip this if we are using
            # thin provisioned LVs.
            # bug# lp1191812
            dev_path = self.local_path(volume) + "-cow"
        else:
            dev_path = self.local_path(volume)

        # TODO(jdg): Maybe we could optimize this for snaps by looking at
        # the cow table and only overwriting what's necessary?
        # for now we're still skipping on snaps due to hang issue
        if not os.path.exists(dev_path):
            msg = (_('Volume device file path %s does not exist.')
                   % dev_path)
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        size_in_g = (volume.get('volume_size') if is_snapshot
                     else volume.get('size'))
        if size_in_g is None:
            msg = (_("Size for volume: %s not found, cannot secure delete.")
                   % volume['id'])
            LOG.error(msg)
            raise exception.InvalidParameterValue(msg)

        # clear_volume expects sizes in MiB, we store integer GiB
        # be sure to convert before passing in
        vol_sz_in_meg = size_in_g * units.Ki

        volutils.clear_volume(
            vol_sz_in_meg, dev_path,
            volume_clear=self.configuration.volume_clear,
            volume_clear_size=self.configuration.volume_clear_size)

    def _escape_snapshot(self, snapshot_name):
        # Linux LVM reserves name that starts with snapshot, so that
        # such volume name can't be created. Mangle it.
        if not snapshot_name.startswith('snapshot'):
            return snapshot_name
        return '_' + snapshot_name

    def _unescape_snapshot(self, snapshot_name):
        # Undo snapshot name change done by _escape_snapshot()
        if not snapshot_name.startswith('_snapshot'):
            return snapshot_name
        return snapshot_name[1:]

    def _create_volume(self, name, size, lvm_type, mirror_count, vg=None):
        vg_ref = self.vg
        if vg is not None:
            vg_ref = vg

        vg_ref.create_volume(name, size, lvm_type, mirror_count)

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
                  print >>sys.stderr, "Exception in create", retcode
        LOG.info('Successfully created volume: %s', volume['id'])

    def update_migrated_volume(self, ctxt, volume, new_volume,
                               original_volume_status):
        return

    def create_volume_from_snapshot(self, volume, snapshot):
        """Creates a volume from a snapshot."""
        return

    def detach_volume(self, context, volume, attachment=None):
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
                  print >>sys.stderr, "Exception in create", retcode
        LOG.info('Successfully deleted  volume: %s', volume['id'])
        return True

    def create_snapshot(self, snapshot):
        """Creates a snapshot."""

    def delete_snapshot(self, snapshot):
        """Deletes a snapshot."""
        LOG.info('Successfully deleted snapshot: %s', snapshot['id'])
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

    def _get_manageable_resource_info(self, cinder_resources, resource_type,
                                      marker, limit, offset, sort_keys,
                                      sort_dirs):

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
