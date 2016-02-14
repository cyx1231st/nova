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
Claim objects for use with resource tracking.
"""

import abc
import six

from oslo_log import log as logging

from nova import exception
from nova.i18n import _
from nova.i18n import _LI
from nova.virt import hardware


LOG = logging.getLogger(__name__)


class _Request(object):
    """A request class used by Claim classes to represent the requested
    resources to claim. Each kinds of Claim has its own version to generate the
    _Request object.
    """

    __slots__ = ['disk_gb',
                 'memory_mb',
                 'vcpus',
                 'numa_topology',
                 'pci_requests']

    def __init__(self):
        self.disk_gb = 0
        self.memory_mb = 0
        self.vcpus = 0
        self.numa_topology = None
        self.pci_requests = None


class _Resources(object):
    """A resource class used by Claim classes to represent the available
    resources to be claimed. Each kinds of Claim has its own version to
    generate the _Resources object.
    """

    __slots__ = ['total_memory_mb',
                 'used_memory_mb',
                 'total_disk_gb',
                 'used_disk_gb',
                 'total_vcpus',
                 'used_vcpus',
                 'host_topology',
                 'host_pci_stats']

    def __init__(self):
        self.total_memory_mb = 0
        self.used_memory_mb = 0
        self.total_disk_gb = 0
        self.used_disk_gb = 0
        self.total_vcpus = 0
        self.used_vcpus = 0
        self.host_topology = None
        self.host_pci_stats = None


@six.add_metaclass(abc.ABCMeta)
class ClaimBase(object):
    """A declaration which requires free resources, and checks whether the
    resources can meet the request. This abstract class only implements the
    basic part of a complete claim class, and should be extended to be used by
    scheduler or compute node.
    """

    def __init__(self, limits=None, migration=None, instance=None):
        super(ClaimBase, self).__init__()
        if instance:
            self.instance = instance.obj_clone()
        else:
            self.instance = None

        self.migration = migration
        self.claimed_numa_topology = None

        self._request = _Request()
        self._prepare_request()

        self._resources = _Resources()
        self._prepare_resources()

        # Check claim at constructor to avoid mess code
        # Raise exception ComputeResourcesUnavailable if claim failed
        self._claim_test(limits)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            LOG.debug("Aborting claim: %s" % self, instance=self.instance)
            self.abort()

    def __str__(self):
        return "[Claim: %d MB memory, %d GB disk, %d vcpus]" % (self.memory_mb,
                self.disk_gb, self.vcpus)

    @property
    def disk_gb(self):
        return self._request.disk_gb

    @property
    def memory_mb(self):
        return self._request.memory_mb

    @property
    def vcpus(self):
        return self._request.vcpus

    @property
    def numa_topology(self):
        return self._request.numa_topology

    @property
    def pci_requests(self):
        return self._request.pci_requests

    @abc.abstractmethod
    def _prepare_request(self):
        """Prepare resource request for claim."""

    @abc.abstractmethod
    def _prepare_resources(self):
        """Prepare available resources."""

    def _test_pci(self):
        pass

    def _test_ext_resources(self, limits):
        return []

    def _test_numa_topology(self, limit):
        host_topology = self._resources.host_topology
        requested_topology = self.numa_topology
        pci_requests = self.pci_requests
        if host_topology and requested_topology:
            pci_stats = None
            if pci_requests:
                pci_stats = self._resources.host_pci_stats
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

    def _claim_test(self, limits=None):
        """Test if this claim can be satisfied given available resources and
        optional oversubscription limits

        This should be called before the openstack service actually consumes
        the resources required to execute the claim.
        """
        if not limits:
            limits = {}

        # NOTE(CHANGE): Remove verbose log
        LOG.debug("Attempting claim: memory %(memory_mb)d MB, "
                  "disk %(disk_gb)d GB, vcpus %(vcpus)d CPU" %
                  {'memory_mb': self.memory_mb, 'disk_gb': self.disk_gb,
                   'vcpus': self.vcpus}, instance=self.instance)

        reasons = [self._test(_("memory"), "MB",
                              self._resources.total_memory_mb,
                              self._resources.used_memory_mb,
                              self.memory_mb,
                              limits.get('memory_mb')),
                   self._test(_("disk"), "GB",
                              self._resources.total_disk_gb,
                              self._resources.used_disk_gb,
                              self.disk_gb,
                              limits.get('disk_gb')),
                   self._test(_("vcpu"), "VCPU",
                              self._resources.total_vcpus,
                              self._resources.used_vcpus,
                              self.vcpus,
                              limits.get('vcpu')),
                   self._test_numa_topology(limits.get('numa_topology')),
                   self._test_pci()]
        reasons = reasons + self._test_ext_resources(limits)
        reasons = [r for r in reasons if r is not None]
        if len(reasons) > 0:
            raise exception.ComputeResourcesUnavailable(reason=
                    "; ".join(reasons))

        # NOTE(CHANGE): Remove verbose log
        LOG.debug('Claim successful', instance=self.instance)

    def _test(self, type_, unit, total, used, requested, limit):
        """Test if the given type of resource needed for a claim can be safely
        allocated.
        """
        # NOTE(CHANGE): Remove verbose log
        LOG.debug('Total %(type)s: %(total)d %(unit)s, used: %(used).02f '
                  '%(unit)s' %
                  {'type': type_, 'total': total, 'unit': unit, 'used': used},
                  instance=self.instance)

        if limit is None:
            # treat resource as unlimited:
            LOG.info(_LI('%(type)s limit not specified, defaulting to '
                         'unlimited'), {'type': type_}, instance=self.instance)
            return

        free = limit - used

        # Oversubscribed resource policy info:
        # NOTE(CHANGE): Remove verbose log
        LOG.debug('%(type)s limit: %(limit).02f %(unit)s, '
                  'free: %(free).02f %(unit)s' %
                  {'type': type_, 'limit': limit, 'free': free, 'unit': unit},
                  instance=self.instance)

        if requested > free:
            return (_('Free %(type)s %(free).02f '
                      '%(unit)s < requested %(requested)d %(unit)s') %
                      {'type': type_, 'free': free, 'unit': unit,
                       'requested': requested})

    def abort(self):
        """Compute operation requiring claimed resources has failed or
        been aborted.
        """
        pass
