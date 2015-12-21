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

from nova.compute import claims
from nova import exception
from nova.i18n import _
from nova.i18n import _LI
from nova.virt import hardware


LOG = logging.getLogger(__name__)


class Claim(claims.Claim):
    """A declaration that a host state operation will require free resources.
    Claims serve as marker objects that resources are being checked before the
    consume_from_request process runs to do a full reconciliation of resource
    usage.

    WARNING: This claim is now a verbatim copy of the code from compute/claims.

    TODO(Yingxin): Refactor claim code to eliminate replication.
    """

    def __init__(self, spec_obj, host_state, limits=None):
        # NOTE(Yingxin): The super() isn't called here because the claim code
        # is currently a verbatim copy from compute/claims.

        # Stash a copy of the request at the current point of time
        self.spec_obj = spec_obj.obj_clone()
        self.host_state = host_state
        self.instance_cells = None
        self.claimed_numa_topology = None

        # Check claim at constructor to avoid mess code
        # Raise exception ComputeResourcesUnavailable if claim failed
        self._claim_test(self.host_state, limits)

    @property
    def disk_gb(self):
        return self.spec_obj.root_gb + self.spec_obj.ephemeral_gb

    @property
    def memory_mb(self):
        return self.spec_obj.memory_mb

    @property
    def vcpus(self):
        return self.spec_obj.vcpus

    @property
    def numa_topology(self):
        return self.spec_obj.numa_topology

    @property
    def pci_requests(self):
        pci_requests = self.spec_obj.pci_requests
        if pci_requests and self.host_state.pci_stats:
            pci_requests = pci_requests.requests
        else:
            pci_requests = None
        return pci_requests

    def abort(self):
        raise NotImplemented("There is no need to abort a claim in"
                             " host state.")

    def _claim_test(self, resources, limits=None):
        """Test if this claim can be satisfied given available resources and
        optional oversubscription limits

        This should be called before the compute node actually consumes the
        resources required to execute the claim.

        :param resources: available local host state resources
        :returns: Return true if resources are available to claim.
        """
        if not limits:
            limits = {}

        # If an individual limit is None, the resource will be considered
        # unlimited:
        memory_mb_limit = limits.get('memory_mb')
        disk_gb_limit = limits.get('disk_gb')
        vcpus_limit = limits.get('vcpu')
        numa_topology_limit = limits.get('numa_topology')

        LOG.info(_LI("Attempting claim: memory %(memory_mb)d MB, "
                     "disk %(disk_gb)d GB, vcpus %(vcpus)d CPU"),
                 {'memory_mb': self.memory_mb, 'disk_gb': self.disk_gb,
                  'vcpus': self.vcpus})

        reasons = [self._test_memory(resources, memory_mb_limit),
                   self._test_disk(resources, disk_gb_limit),
                   self._test_vcpus(resources, vcpus_limit),
                   self._test_numa_topology(resources, numa_topology_limit),
                   self._test_pci()]
        reasons = reasons + self._test_ext_resources(limits)
        reasons = [r for r in reasons if r is not None]
        if len(reasons) > 0:
            raise exception.ComputeResourcesUnavailable(reason=
                    "; ".join(reasons))

        LOG.info(_LI('Claim successful'))

    def _test_memory(self, resources, limit):
        type_ = _("memory")
        unit = "MB"
        total = resources.total_usable_ram_mb
        used = resources.total_usable_ram_mb - resources.free_ram_mb
        requested = self.memory_mb

        return self._test(type_, unit, total, used, requested, limit)

    def _test_disk(self, resources, limit):
        type_ = _("disk")
        unit = "GB"
        total = resources.total_usable_disk_gb
        used = resources.total_usable_disk_gb - resources.free_disk_mb / 1024
        requested = self.disk_gb

        return self._test(type_, unit, total, used, requested, limit)

    def _test_vcpus(self, resources, limit):
        type_ = _("vcpu")
        unit = "VCPU"
        total = resources.vcpus_total
        used = resources.vcpus_used
        requested = self.vcpus

        return self._test(type_, unit, total, used, requested, limit)

    def _test_pci(self):
        pci_requests = self.pci_requests
        claimed_numa_topology = self.claimed_numa_topology
        if claimed_numa_topology:
            self.instance_cells = claimed_numa_topology.cells
        if pci_requests:
            if not self.host_state.pci_stats.support_requests(
                    pci_requests,
                    self.instance_cells):
                return _('Claim pci failed.')

    def _test_ext_resources(self, limits):
        return []

    def _test_numa_topology(self, resources, limit):
        host_topology, _fmt = hardware.host_topology_and_format_from_host(
                                self.host_state)
        requested_topology = self.numa_topology
        pci_requests = self.pci_requests
        if host_topology and requested_topology:
            pci_stats = None
            if pci_requests:
                pci_stats = self.host_state.pci_stats
            self.claimed_numa_topology = (
                    hardware.numa_fit_instance_to_host(
                        host_topology, requested_topology,
                        limits=limit,
                        pci_requests=pci_requests,
                        pci_stats=pci_stats))
            if not self.claimed_numa_topology:
                if pci_requests:
                    return (_("Requested instance NUMA topology together with"
                              " requested PCI devices cannot fit the given"
                              " host NUMA topology"))
                else:
                    return (_("Requested instance NUMA topology cannot fit "
                          "the given host NUMA topology"))

    def _test(self, type_, unit, total, used, requested, limit):
        """Test if the given type of resource needed for a claim can be safely
        allocated.
        """
        LOG.info(_LI('Total %(type)s: %(total)d %(unit)s, used: %(used).02f '
                    '%(unit)s'),
                  {'type': type_, 'total': total, 'unit': unit, 'used': used})

        if limit is None:
            # treat resource as unlimited:
            LOG.info(_LI('%(type)s limit not specified, defaulting to '
                        'unlimited'), {'type': type_})
            return

        free = limit - used

        # Oversubscribed resource policy info:
        LOG.info(_LI('%(type)s limit: %(limit).02f %(unit)s, '
                     'free: %(free).02f %(unit)s'),
                  {'type': type_, 'limit': limit, 'free': free, 'unit': unit})

        if requested > free:
            return (_('Free %(type)s %(free).02f '
                      '%(unit)s < requested %(requested)d %(unit)s') %
                      {'type': type_, 'free': free, 'unit': unit,
                       'requested': requested})
