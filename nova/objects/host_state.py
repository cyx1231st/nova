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

from nova.objects import base
from nova.objects import fields


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

    @classmethod
    def from_primitives(cls, context, compute):
        state = cls(context, micro_version=0)
        state._from_compute(compute)
        
