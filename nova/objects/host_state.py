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
        micro_version = random.randint(0, 1000000)
        state = cls(context, micro_version=micro_version)
        state._from_compute(compute)
        return state

    _special = {'micro_version'}
    _normal = set(fields.keys()) - _special

    def update_from_compute(self, context, compute):
        new_state = HostState.from_primitives(context, compute)
        commit = {}

        for field in self._normal:
            new = getattr(new_state, field)
            old = getattr(self, field)
            change = new - old
            if change:
                setattr(self, field, new)
                commit[field] = change

        if commit:
            commit['version_changed'] = 1
            self.micro_version = self.micro_version + 1
            commit['version_expected'] = self.micro_version
            return commit
        else:
            return None

    _special_keys = {'version_expected'}

    def process_commit(self, commit):
        for item in commit:
            keys = set(item.keys()) - self._special_keys
            for field in keys:
                setattr(self, field, getattr(self, field) + item[field])
            if self.micro_version != item['version_expected']:
                return False
        return True
