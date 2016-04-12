# Copyright (c) 2016 OpenStack Foundation
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

import copy
import random

from oslo_log import log as logging

from nova import objects
from nova.objects import base
from nova.objects import fields
from nova.virt import hardware
# from nova.pci import stats as pci_stats

LOG = logging.getLogger(__name__)


def _getattr_dict(dict_obj, field):
    return dict_obj.get(field)


@base.NovaObjectRegistry.register
class HostState(base.NovaObject):
    """The cache object to be synchronized between servers and clients. """
    # Version 1.0: Initial version
    VERSION = '1.0'

    fields = {
        'micro_version': fields.IntegerField(),

        'total_usable_ram_mb': fields.IntegerField(),
        'free_ram_mb': fields.IntegerField(),

        'total_usable_disk_gb': fields.IntegerField(),
        'disk_mb_used': fields.IntegerField(),
        'free_disk_mb': fields.IntegerField(),

        'vcpus_total': fields.IntegerField(),
        'vcpus_used': fields.IntegerField(),

        'numa_topology': fields.StringField(nullable=True),
        'pci_stats': fields.StringField(nullable=True),

        'host': fields.StringField(nullable=True),
        'host_ip': fields.IPAddressField(nullable=True),
        'hypervisor_type': fields.StringField(),
        'hypervisor_version': fields.IntegerField(),
        'hypervisor_hostname': fields.StringField(nullable=True),
        'cpu_info': fields.StringField(nullable=True),
        'supported_instances': fields.ListOfListOfStringsField(),

        'num_instances': fields.IntegerField(),
        'num_io_ops': fields.IntegerField(),

        'metrics': fields.ObjectField('MonitorMetricList'),
        'cpu_allocation_ratio': fields.FloatField(),
        'ram_allocation_ratio': fields.FloatField(),
        'disk_allocation_ratio': fields.FloatField(),
    }

    def _from_compute(self, compute):
        self.total_usable_ram_mb = compute.memory_mb
        self.free_ram_mb = compute.free_ram_mb
        self.total_usable_disk_gb = compute.local_gb
        self.disk_mb_used = compute.local_gb_used * 1024

        free_gb = compute.free_disk_gb
        least_gb = compute.disk_available_least
        if least_gb is not None:
            free_gb = min(least_gb, free_gb)
        self.free_disk_mb = free_gb * 1024
        self.vcpus_total = compute.vcpus
        self.vcpus_used = compute.vcpus_used

        self.numa_topology = compute.numa_topology

        # TODO(Yingxin): Handle pci status
        self.pci_stats = None
        # self.pci_stats = pci_stats.PciDeviceStats(
        #        compute.pci_device_pools)

        self.host = compute.host
        self.host_ip = copy.deepcopy(compute.host_ip)
        self.hypervisor_type = compute.hypervisor_type
        self.hypervisor_version = compute.hypervisor_version
        self.hypervisor_hostname = compute.hypervisor_hostname
        self.cpu_info = compute.cpu_info
        if compute.supported_hv_specs:
            self.supported_instances = [spec.to_list() for spec
                                        in compute.supported_hv_specs]
        else:
            self.supported_instances = []

        stats = compute.stats or {}

        self.num_instances = int(stats.get('num_instances', 0))
        self.num_io_ops = int(stats.get('io_workload', 0))

        self.metrics = objects.MonitorMetricList.from_json(compute.metrics)

        self.cpu_allocation_ratio = compute.cpu_allocation_ratio
        self.ram_allocation_ratio = compute.ram_allocation_ratio
        self.disk_allocation_ratio = compute.disk_allocation_ratio

    @classmethod
    def from_primitives(cls, context, compute, version=None):
        if version is None:
            version = random.randint(0, 1000000)
        state = cls(context, micro_version=version)
        state._from_compute(compute)
        return state

    incremental_fields = {'total_usable_ram_mb',
                          'free_ram_mb',
                          'total_usable_disk_gb',
                          'disk_mb_used',
                          'free_disk_mb',
                          'vcpus_total',
                          'vcpus_used',
                          'num_instances',
                          'num_io_ops',
                          'micro_version',
                          }
    overwrite_fields = {'host',
                        'host_ip',
                        'hypervisor_type',
                        'hypervisor_version',
                        'hypervisor_hostname',
                        'cpu_info',
                        'supported_instances',
                        'cpu_allocation_ratio',
                        'ram_allocation_ratio',
                        'disk_allocation_ratio',
                        'numa_topology',
                        }

    # NOTE(Yingxin): Others are special handled fields: pci_stats, metrics.
    # Temporarly add numa_topology to overwrite_fields

    def update_from_compute(self, context, compute):
        new_version = self.micro_version
        new_state = HostState.from_primitives(
                context, compute, version=new_version)
        incremental_updates = {}
        overwrite_updates = {}
        special_updates = {}

        for field in self.incremental_fields:
            new = getattr(new_state, field)
            old = getattr(self, field)
            change = new - old
            if change:
                setattr(self, field, new)
                incremental_updates[field] = change

        for field in self.overwrite_fields:
            new = getattr(new_state, field)
            old = getattr(self, field)
            if new != old:
                new = copy.deepcopy(new)
                setattr(self, field, new)
                overwrite_updates[field] = new

        # NOTE(Yingxin): Override __eq__ in MonitorMetricList or keep this.
        new = new_state.metrics
        new_list = new.to_list()
        old_list = self.metrics.to_list()
        if new_list != old_list:
            new = copy.deepcopy(new)
            self.metrics = new
            special_updates['metrics'] = new

        # TODO(Yingxin): Incremental update pci status
        # TODO(Yingxin): Incremental update numa_topology

        if incremental_updates or overwrite_updates or special_updates:
            incremental_updates['micro_version'] = 1
            self.micro_version = self.micro_version + 1

            cache_update = {'expected_version': self.micro_version,
                            'incremental_updates': incremental_updates,
                            'overwrite_updates': overwrite_updates,
                            'special_updates': special_updates}

            return cache_update
        else:
            return None

    def _process_incremental_fields(self, incremental_fields, change, sign):
        if isinstance(change, dict):
            _getattr = _getattr_dict
        else:
            _getattr = getattr

        if sign:
            for field in incremental_fields:
                setattr(self, field,
                        getattr(self, field) + _getattr(change, field))
        else:
            for field in incremental_fields:
                setattr(self, field,
                        getattr(self, field) - _getattr(change, field))

    def process_update(self, cache_update):
        expected_version = cache_update['expected_version']
        incremental_updates = cache_update['incremental_updates']
        overwrite_updates = cache_update['overwrite_updates']
        special_updates = cache_update['special_updates']

        self._process_incremental_fields(incremental_updates.keys(),
                                         incremental_updates, True)
        for field in overwrite_updates.keys():
            setattr(self, field, overwrite_updates[field])

        if 'metrics' in special_updates:
            setattr(self, 'metrics', special_updates['metrics'])

        # TODO(Yingxin) Incremental update pci_stats
        # TODO(Yingxin) Incremental update numa_topology

        if expected_version is None:
            return True
        elif self.micro_version != expected_version:
            return False
        else:
            return True

    def process_claim(self, claim, apply_claim):
        self._process_incremental_fields(objects.CacheClaim.incremental_fields,
                                         claim, apply_claim)

        # TODO(Yingxin): Overcommit numa topology here, because it is possible
        # that a remote commit is conflict with a local claim record.
        if claim.numa_topology is not None:
            self.numa_topology = hardware.get_host_numa_usage_from_instance(
                    self, claim.numa_topology, not apply_claim)

        if claim.pci_requests is not None:
            # TODO(Yingxin) Overcommit pci_stats:
            # if claim['pci_requests']:
            #     self.pci_stats.apply_requests(claim['pci_requests'],
            #                                   claim['numa_topology'].cells)
            pass

    def __repr__(self):
        return ("HostState(%s, %s) total_usable_ram_mb:%s free_ram_mb:%s "
                "total_usable_disk_gb:%s free_disk_mb:%s disk_mb_used:%s "
                "vcpus_total:%s vcpus_used:%s "
                "numa_topology:%s pci_stats:%s "
                "num_io_ops:%s num_instances:%s" %
                (self.host, self.hypervisor_hostname,
                 self.total_usable_ram_mb, self.free_ram_mb,
                 self.total_usable_disk_gb, self.free_disk_mb,
                 self.disk_mb_used,
                 self.vcpus_total, self.vcpus_used,
                 self.numa_topology, self.pci_stats,
                 self.num_io_ops, self.num_instances))


@base.NovaObjectRegistry.register
class CacheClaim(base.NovaObject):
    """The claim object sent from clients, and tracked by ClaimRecords. """
    # Version 1.0: Initial version
    VERSION = '1.0'

    fields = {
        'seed': fields.IntegerField(nullable=False),
        'origin_host': fields.StringField(nullable=False),
        'target_host': fields.StringField(nullable=False),
        'instance_uuid': fields.UUIDField(nullable=False),

        'free_ram_mb': fields.IntegerField(nullable=False),
        'disk_mb_used': fields.IntegerField(nullable=False),
        'vcpus_used': fields.IntegerField(nullable=False),
        'num_instances': fields.IntegerField(nullable=False),
        'num_io_ops': fields.IntegerField(nullable=False),

        'pci_requests': fields.ObjectField('InstancePCIRequest',
                                           nullable=True),
        'numa_topology': fields.ObjectField('InstanceNUMATopology',
                                            nullable=True),
    }

    incremental_fields = {'free_ram_mb',
                          'disk_mb_used',
                          'vcpus_used',
                          'num_instances',
                          'num_io_ops',
                          }

    @classmethod
    def from_primitives(cls, seed, origin_host,
                        target_host, instance_uuid, claim):
        obj = cls()

        obj.seed = seed
        obj.origin_host = origin_host
        obj.target_host = target_host
        obj.instance_uuid = instance_uuid

        obj.free_ram_mb = - claim.memory_mb
        obj.disk_mb_used = claim.disk_gb * 1024
        obj.vcpus_used = claim.vcpus
        obj.num_instances = 1
        obj.num_io_ops = 1
        obj.pci_requests = claim.pci_requests
        obj.numa_topology = claim.numa_topology

        return obj

    def to_cache_update(self, proceed):
        incremental_updates = {}
        for field in self.incremental_fields:
            val = getattr(self, field)
            if not proceed:
                val = -val
            if val:
                incremental_updates[field] = val

        # TODO(Yingxin) Incremental update pci_stats
        # TODO(Yingxin) Incremental update numa_topology

        return {'expected_version': None,
                'incremental_updates': incremental_updates,
                'overwrite_updates': {},
                'special_updates': {}}


@base.NovaObjectRegistry.register
class ClaimReply(base.NovaObject):
    # Version 1.0: Initial version
    VERSION = '1.0'

    fields = {
        'seed': fields.IntegerField(nullable=False),
        'origin_host': fields.StringField(nullable=False),
        'target_host': fields.StringField(nullable=False),
        'instance_uuid': fields.UUIDField(nullable=False),
        'proceed': fields.BooleanField(nullable=False),
    }

    @classmethod
    def from_claim(cls, claim, proceed):
        obj = cls()
        obj.seed = claim.seed
        obj.origin_host = claim.origin_host
        obj.target_host = claim.target_host
        obj.instance_uuid = claim.instance_uuid
        obj.proceed = proceed
        return obj

# TODO(Yingxin): It still seems impossible to implement a mix-typed dict field
# that can work with oslo_messaging, need to look deeper and figure out how.

"""
@base.NovaObjectRegistry.register
class CacheCommit(base.NovaObject):
    # Version 1.0: Initial version
    VERSION = '1.0'

    fields = {
        'cache_refresh': fields.ObjectField('HostState', nullable=True),
        'cache_update': fields.ObjectField('CacheUpdate', nullable=True),
        'claim_replies': fields.ListOfObjectsField('ClaimReply',
                                                    nullable=True),
    }

    @classmethod
    def from_primitives(cls, cache_update=None, claim_replies=None):
        obj = cls()
        obj.cache_update = cache_update
        obj.claim_replies = claim_replies or []
        obj.cache_refresh = None
        return obj

    @classmethod
    def from_updates(cls, expected_version, incrementals,
                     overwrites, specials):
        cache_update = objects.CacheUpdate.from_primitives(
                expected_version, incrementals, overwrites, specials)
        return cls.from_primitives(cache_update = cache_update)


@base.NovaObjectRegistry.register
class CacheUpdate(base.NovaObject):
    # Version 1.0: Initial version
    VERSION = '1.0'

    fields = {
        'expected_version': fields.IntegerField(nullable=False),
        'incremental_updates': fields.DictOfIntegersField(nullable=True),
        # TODO(Yingxin): It still seems impossible to implement a mix-typed
        # dict, need to look deeper and figure out how.
        'overwrite_updates': fields.RelaxedDictField(nullable=True),
        'special_updates': fields.RelaxedDictField(nullable=True),
    }

    @classmethod
    def from_primitives(cls, expected_version, incrementals,
                        overwrites, specials):
        obj = cls()
        obj.expected_version = expected_version
        obj.incremental_updates = incrementals
        obj.overwrite_updates = overwrites
        obj.special_updates = specials
        return obj
"""
