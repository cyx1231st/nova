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
import random

from nova import objects
from nova.objects import base
from nova.objects import fields
from nova.scheduler import claims
from nova.virt import hardware
# from nova.pci import stats as pci_stats


@base.NovaObjectRegistry.register
class HostState(base.NovaObject):
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
        self.pci_stats = None
        # self.pci_stats = pci_stats.PciDeviceStats(
        #        compute.pci_device_pools)

        self.host = compute.host
        self.host_ip = compute.host_ip
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

    @classmethod
    def from_primitives(cls, context, compute, version=None):
        if version is None:
            version = random.randint(0, 1000000)
        state = cls(context, micro_version=version)
        state._from_compute(compute)
        return state

    _special = {'pci_stats', 'metrics'}
    _integer_fields = {'total_usable_ram_mb',
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
    _reset_fields = {'host',
                     'host_ip',
                     'hypervisor_type',
                     'hypervisor_version',
                     'hypervisor_hostname',
                     'cpu_info',
                     'supported_instances',
                     'cpu_allocation_ratio',
                     'ram_allocation_ratio',
                     'numa_topology',
                     }

    def update_from_compute(self, context, compute):
        new_version = self.micro_version
        new_state = HostState.from_primitives(
                context, compute, version=new_version)
        commit = {}

        for field in self._integer_fields:
            new = getattr(new_state, field)
            old = getattr(self, field)
            change = new - old
            if change:
                setattr(self, field, new)
                commit[field] = change

        for field in self._reset_fields:
            new = getattr(new_state, field)
            old = getattr(self, field)
            if new != old:
                setattr(self, field, new)
                commit[field] = new

        new = new_state.metrics
        new_list = new.to_list()
        old_list = self.metrics.to_list()
        if new_list != old_list:
            self.metrics = new
            commit['metrics'] = new

        # TODO() proceed pci_stats
        # TODO() increment numa_topology

        if commit:
            commit['micro_version'] = 1
            self.micro_version = self.micro_version + 1
            commit['version_expected'] = self.micro_version
            return commit
        else:
            return None

    _special_keys = {'version_expected'}

    def process_commit(self, commit):
        result = True
        item = commit

        keys = set(item.keys())

        changed_keys = keys & self._integer_fields
        for field in changed_keys:
            setattr(self, field, getattr(self, field) + item[field])

        reset_keys = keys & self._reset_fields
        for field in reset_keys:
            setattr(self, field, item[field])

        if 'metrics' in keys:
            setattr(self, 'metrics', item['metrics'])

        # TODO() pci_stats
        # TODO() increment numa_topology

        if self.micro_version != item['version_expected']:
            result = False

        return result

    def claim(self, req, limits):
        claim = claims.Claim(req, self, limits)
        return claim.to_dict()

    def process_claim(self, claim, apply_claim):
        # TODO() apply pci_requests and cells to pci_stats
        # if claim['pci_requests']:
        #     self.pci_stats.apply_requests(claim['pci_requests'],
        #                                   claim['numa_topology'].cells)

        if apply_claim:
            self.free_ram_mb += claim['free_ram_mb']
            self.disk_mb_used += claim['disk_mb_used']
            self.vcpus_used += claim['vcpus_used']
            self.num_instances += claim['num_instances']
            self.num_io_ops += claim['num_io_ops']
            self.numa_topology = hardware.get_host_numa_usage_from_instance(
                    self, claim['numa_topology'])
        else:
            self.free_ram_mb -= claim['free_ram_mb']
            self.disk_mb_used -= claim['disk_mb_used']
            self.vcpus_used -= claim['vcpus_used']
            self.num_instances -= claim['num_instances']
            self.num_io_ops -= claim['num_io_ops']
            self.numa_topology = hardware.get_host_numa_usage_from_instance(
                    self, claim['numa_topology'], True)

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
