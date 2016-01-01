# Copyright (c) 2016 OpenStack Foundation
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
Claim objects for use with scheduler resource consumption.
"""

from oslo_log import log as logging

from nova import claims
from nova.i18n import _
from nova.virt import hardware


LOG = logging.getLogger(__name__)


class Claim(claims.ClaimBase):
    """A declaration that a host state operation will require free resources.
    Claims serve as marker objects that resources are being checked before the
    consume_from_request process runs to do a full reconciliation of resource
    usage.
    """

    def __init__(self, spec_obj, host_state, limits=None):
        self.spec_obj = spec_obj.obj_clone()
        self.host_state = host_state
        self.instance_cells = None
        super(Claim, self).__init__(limits)

    def _prepare_request(self):
        self._request.disk_gb = self.spec_obj.root_gb + \
                          self.spec_obj.ephemeral_gb
        self._request.memory_mb = self.spec_obj.memory_mb
        self._request.vcpus = self.spec_obj.vcpus
        self._request.numa_topology = self.spec_obj.numa_topology

        pci_requests = self.spec_obj.pci_requests
        if pci_requests and self.host_state.pci_stats:
            self._request.pci_requests = pci_requests.requests

    def _prepare_resources(self):
        self._resources.total_memory_mb = self.host_state.total_usable_ram_mb
        self._resources.used_memory_mb = self._resources.total_memory_mb - \
                                   self.host_state.free_ram_mb
        self._resources.total_disk_gb = self.host_state.total_usable_disk_gb
        self._resources.used_disk_gb = self._resources.total_disk_gb - \
                                 self.host_state.free_disk_mb / 1024
        self._resources.total_vcpus = self.host_state.vcpus_total
        self._resources.used_vcpus = self.host_state.vcpus_used
        self._resources.host_topology, _fmt = \
            hardware.host_topology_and_format_from_host(self.host_state)
        self._resources.host_pci_stats = self.host_state.pci_stats

    def _test_pci(self):
        pci_stats = self._resources.host_pci_stats
        pci_requests = self.pci_requests
        claimed_numa_topology = self.claimed_numa_topology

        if claimed_numa_topology:
            self.instance_cells = claimed_numa_topology.cells
        if pci_requests and pci_stats:
            if not pci_stats.support_requests(
                    pci_requests,
                    self.instance_cells):
                return _('Claim pci failed.')
