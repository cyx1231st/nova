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
Tests For Claim
"""

import mock
from oslo_utils import versionutils

from nova.compute import task_states
from nova.compute import vm_states
from nova import exception
from nova import objects
from nova.pci import stats as pci_stats
from nova.scheduler import claims
from nova.scheduler import host_manager
from nova import test
from nova.tests import uuidsentinel


class ClaimTestCase(test.NoDBTestCase):
    """Test case for scheduler Claim class.

    NOTE(Yingxin): Claim related tests are moved from test_host_manager.
    """

    def test_claim_failed_with_simple_resources(self):
        memory_mb = 1024
        ram_ratio = 1
        vcpus = 1
        cpu_ratio = 1
        disk_gb = 4
        disk_ratio = 1

        stats = {
            'num_instances': '5',
            'num_proj_12345': '3',
            'num_proj_23456': '1',
            'num_vm_%s' % vm_states.BUILDING: '2',
            'num_vm_%s' % vm_states.SUSPENDED: '1',
            'num_task_%s' % task_states.RESIZE_MIGRATING: '1',
            'num_task_%s' % task_states.MIGRATING: '2',
            'num_os_type_linux': '4',
            'num_os_type_windoze': '1',
            'io_workload': '42',
        }
        hyper_ver_int = versionutils.convert_version_to_int('6.0.0')
        compute = objects.ComputeNode(
            stats=stats, memory_mb=memory_mb, free_disk_gb=0, local_gb=disk_gb,
            local_gb_used=1, free_ram_mb=0, vcpus=vcpus, vcpus_used=0,
            disk_available_least=None,
            updated_at=None, host_ip='127.0.0.1',
            hypervisor_type='htype',
            hypervisor_hostname='hostname', cpu_info='cpu_info',
            supported_hv_specs=[],
            hypervisor_version=hyper_ver_int, numa_topology=None,
            pci_device_pools=None, metrics=None,
            cpu_allocation_ratio=cpu_ratio, ram_allocation_ratio=ram_ratio)

        host = host_manager.HostState("fakehost", "fakenode")
        host.update(compute=compute)
        host.limits['memory_mb'] = memory_mb * ram_ratio
        host.limits['vcpu'] = vcpus * cpu_ratio
        host.limits['disk_gb'] = disk_gb * disk_ratio

        fake_numa_topology = objects.InstanceNUMATopology(
            cells=[objects.InstanceNUMACell()])
        spec_obj = objects.RequestSpec(
            instance_uuid=uuidsentinel.fake_spec,
            flavor=objects.Flavor(root_gb=2, ephemeral_gb=2,
                                  memory_mb=2048, vcpus=2),
            numa_topology=fake_numa_topology,
            pci_requests=objects.InstancePCIRequests(requests=[]))

        try:
            claims.Claim(spec_obj, host, host.limits)
        except exception.ComputeResourcesUnavailable as e:
            self.assertIn("Free vcpu 1.00 VCPU < requested 2 VCPU",
                    e.message)
            self.assertIn("Free memory 0.00 MB < requested 2048 MB",
                    e.message)
            self.assertIn("Free disk 0.00 GB < requested 4 GB",
                    e.message)
        else:
            self.fail("_locked_consume_from_request should raise"
                      " ComputeResourcesUnavailable.")

    def test_claim_numa_topology_fails(self):
        stats = {
            'num_instances': '5',
            'num_proj_12345': '3',
            'num_proj_23456': '1',
            'num_vm_%s' % vm_states.BUILDING: '2',
            'num_vm_%s' % vm_states.SUSPENDED: '1',
            'num_task_%s' % task_states.UNSHELVING: '1',
            'num_task_%s' % task_states.RESCUING: '2',
            'num_os_type_linux': '4',
            'num_os_type_windoze': '1',
            'io_workload': '42',
        }

        hyper_ver_int = versionutils.convert_version_to_int('6.0.0')
        compute = objects.ComputeNode(
            stats=stats, memory_mb=0, free_disk_gb=0, local_gb=0,
            local_gb_used=0, free_ram_mb=0, vcpus=0, vcpus_used=0,
            disk_available_least=None,
            updated_at=None, host_ip='127.0.0.1',
            hypervisor_type='htype',
            hypervisor_hostname='hostname', cpu_info='cpu_info',
            supported_hv_specs=[],
            hypervisor_version=hyper_ver_int, numa_topology=None,
            pci_device_pools=None, metrics=None,
            cpu_allocation_ratio=16.0, ram_allocation_ratio=1.5)

        host = host_manager.HostState("fakehost", "fakenode")
        host.update(compute=compute)
        limit_topo = objects.NUMATopologyLimits(
            cpu_allocation_ratio=1, ram_allocation_ratio=1)
        host.limits['numa_topology'] = limit_topo
        host.numa_topology = objects.NUMATopology(
                cells=[objects.NUMACell(id=1, cpuset=set([1, 2]), memory=512,
                                        memory_usage=0, cpu_usage=0,
                                        mempages=[], siblings=[],
                                        pinned_cpus=set([])),
                       objects.NUMACell(id=2, cpuset=set([3, 4]), memory=512,
                                        memory_usage=0, cpu_usage=0,
                                        mempages=[], siblings=[],
                                        pinned_cpus=set([]))]
                )
        huge_instance = objects.InstanceNUMATopology(
                cells=[objects.InstanceNUMACell(
                    id=1, cpuset=set([1, 2, 3, 4, 5]), memory=2048)])
        spec_obj = objects.RequestSpec(
            instance_uuid=uuidsentinel.fake_spec,
            flavor=objects.Flavor(root_gb=2, ephemeral_gb=2,
                                  memory_mb=2048, vcpus=2),
            numa_topology=huge_instance,
            pci_requests=objects.InstancePCIRequests(requests=[]))

        try:
            claims.Claim(spec_obj, host, host.limits)
        except exception.ComputeResourcesUnavailable as e:
            self.assertIn("Requested instance NUMA topology cannot"
                          " fit the given host NUMA topology",
                          e.message)
        else:
            self.fail("_locked_consume_from_request should raise"
                      " ComputeResoruceUnavailable.")

    def test_claim_pci_fails(self):
        fake_requests = [{'request_id': 'fake_request1', 'count': 3,
                          'spec': [{'vendor_id': '8086'}]}]
        fake_requests_obj = objects.InstancePCIRequests(
                                requests=[objects.InstancePCIRequest(**r)
                                          for r in fake_requests],
                                instance_uuid='fake-uuid')
        req_spec = objects.RequestSpec(
            instance_uuid=uuidsentinel.fake_spec,
            project_id='12345',
            numa_topology=None,
            pci_requests=fake_requests_obj,
            flavor=objects.Flavor(root_gb=0,
                                  ephemeral_gb=0,
                                  memory_mb=1024,
                                  vcpus=1))
        host = host_manager.HostState("fakehost", "fakenode")
        self.assertIsNone(host.updated)
        fake_updated = mock.sentinel.fake_updated
        host.updated = fake_updated
        host.pci_stats = pci_stats.PciDeviceStats()
        with mock.patch.object(host.pci_stats, 'support_requests',
                               return_value=False):
            try:
                claims.Claim(req_spec, host, host.limits)
            except exception.ComputeResourcesUnavailable as e:
                self.assertIn("Claim pci failed.", e.message)
            else:
                self.fail("_locked_consume_from_request should raise"
                          " ComputeResoruceUnavailable.")
        self.assertEqual(fake_updated, host.updated)
