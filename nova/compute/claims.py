# Copyright (c) 2012 OpenStack Foundation
# All Rights Reserved.
#
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
Claim objects for use with resource tracking.
"""

from oslo_log import log as logging

from nova import claims
from nova.i18n import _
from nova.i18n import _LW
from nova import objects
from nova.virt import hardware


LOG = logging.getLogger(__name__)


class NopClaim(claims.ClaimBase):
    """For use with compute drivers that do not support resource tracking."""

    def __init__(self, *args, **kwargs):
        super(NopClaim, self).__init__(migration=kwargs.pop('migration', None))

    def _claim_test(self, limits):
        """NopClaim does no test on resources."""
        pass

    def _prepare_request(self):
        pass

    def _prepare_resources(self):
        pass


class Claim(claims.ClaimBase):
    """A declaration that a compute host operation will require free resources.
    Claims serve as marker objects that resources are being held until the
    update_available_resource audit process runs to do a full reconciliation
    of resource usage.

    This information will be used to help keep the local compute hosts's
    ComputeNode model in sync to aid the scheduler in making efficient / more
    correct decisions with respect to host selection.
    """

    # NOTE(CHANGE)
    def __init__(self, context, instance, tracker, resources, overhead=None,
                 limits=None, scheduler_claim=None):
        self.tracker = tracker
        self.resources = resources
        self.overhead = overhead or {'memory_mb': 0}
        self.context = context
        # NOTE(CHANGE)
        self.scheduler_claim = scheduler_claim

        super(Claim, self).__init__(limits=limits, instance=instance)

    def _prepare_request(self):
        self._request.disk_gb = self.instance.root_gb + \
                          self.instance.ephemeral_gb
        self._request.memory_mb = self.instance.memory_mb + \
                            self.overhead['memory_mb']
        self._request.vcpus = self.instance.vcpus
        self._request.numa_topology = self.instance.numa_topology

        pci_requests = objects.InstancePCIRequests.get_by_instance_uuid(
            self.context, self.instance.uuid)
        if pci_requests:
            self._request.pci_requests = pci_requests.requests

    def _prepare_resources(self):
        self._resources.total_memory_mb = self.resources['memory_mb']
        self._resources.used_memory_mb = self.resources['memory_mb_used']
        self._resources.total_disk_gb = self.resources['local_gb']
        self._resources.used_disk_gb = self.resources['local_gb_used']
        self._resources.total_vcpus = self.resources['vcpus']
        self._resources.used_vcpus = self.resources['vcpus_used']

        host_topology = self.resources.get('numa_topology')
        if host_topology:
            self._resources.host_topology = \
                    objects.NUMATopology.obj_from_db_obj(host_topology)

        if self._request.pci_requests:
            self._resources.host_pci_stats = self.tracker.pci_tracker.stats

    def _test_pci(self):
        if self.pci_requests:
            devs = self.tracker.pci_tracker.claim_instance(self.context,
                                                           self.instance)
            if not devs:
                return _('Claim pci failed.')

    def _test_ext_resources(self, limits):
        return self.tracker.ext_resources_handler.test_resources(
            self.instance, limits)

    def abort(self):
        # NOTE(CHANGE)
        self.tracker.abort_instance_claim(self.context, self.instance,
                self.scheduler_claim)


class MoveClaim(Claim):
    """Claim used for holding resources for an incoming move operation.

    Move can be either a migrate/resize, live-migrate or an evacuate operation.
    """
    def __init__(self, context, instance, instance_type, image_meta, tracker,
                 resources, overhead=None, limits=None):
        self.instance_type = instance_type
        self.image_meta = image_meta
        super(MoveClaim, self).__init__(context, instance, tracker,
                                        resources, overhead=overhead,
                                        limits=limits)

    def _prepare_request(self):
        self._request.disk_gb = self.instance_type.root_gb + \
                          self.instance_type.ephemeral_gb
        self._request.memory_mb = self.instance_type.memory_mb + \
                            self.overhead['memory_mb']
        self._request.vcpus = self.instance_type.vcpus

        image_meta = objects.ImageMeta.from_dict(self.image_meta)
        self._request.numa_topology = hardware.numa_get_constraints(
            self.instance_type, image_meta)

        pci_requests = objects.InstancePCIRequests.get_by_instance_uuid(
            self.context, self.instance.uuid)
        if pci_requests:
            self._request.pci_requests = pci_requests.requests

    def _test_pci(self):
        pci_requests = objects.InstancePCIRequests.\
                       get_by_instance_uuid_and_newness(
                           self.context, self.instance.uuid, True)
        if pci_requests.requests:
            claim = self.tracker.pci_tracker.stats.support_requests(
                pci_requests.requests)
            if not claim:
                return _('Claim pci failed.')

    def _test_ext_resources(self, limits):
        return self.tracker.ext_resources_handler.test_resources(
            self.instance_type, limits)

    def abort(self):
        self.tracker.drop_move_claim(
            self.context,
            self.instance, instance_type=self.instance_type)

    def create_migration_context(self):
        if not self.migration:
            LOG.warn(_LW("Can't create a migration_context record "
                         "without a migration object specified."),
                     instance=self.instance)
            return

        mig_context = objects.MigrationContext(
            context=self.context, instance_uuid=self.instance.uuid,
            migration_id=self.migration.id,
            old_numa_topology=self.instance.numa_topology,
            new_numa_topology=self.claimed_numa_topology)
        return mig_context
