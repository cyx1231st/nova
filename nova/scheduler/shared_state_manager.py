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
Manage shared cache in the current zone.
"""


from nova.pci import stats as pci_stats
from nova.scheduler import host_manager


class SharedHostState(host_manager.HostState):
    def __init__(self, remote_manager, aggregates, inst_dict):
        if not remote_manager.is_activated():
            raise RuntimeError("Manager %s is assumed active in "
                               "SharedHostState.__init__()!"
                               % remote_manager.host)
        self. _manager = remote_manager

        self.host_state = remote_manager.host_state
        self.aggregates = aggregates or []
        self.instances = inst_dict or {}
        self.limits = {}

        # TODO(Yingxin): Remove after implemented
        self.pci_stats = pci_stats.PciDeviceStats()
        # TODO(Yingxin): Doesn't support nodename yet
        self.nodename = self.host

    # NOTE(Yingxin): Do not implement __setattr__ to make self.host_state
    # readonly by filters and weighers
    def __getattr__(self, name):
        return getattr(self.host_state, name)

    def consume_from_request(self, spec_obj):
        return self._manager.consume_cache(spec_obj, self.limits)

    def __repr__(self):
        return self.host_state.__repr__()


class SharedHostManager(host_manager.HostManager):
    def __init__(self):
        super(SharedHostManager, self).__init__()
        # NOTE(Yingxin): SharedHostManager can detect available compute nodes
        # automatically based on ServiceGroup.
        self.filter_cls_map.pop('ComputeFilter', None)
        compute_filter = self.filter_obj_map.pop('ComputeFilter', None)
        if compute_filter and compute_filter in self.default_filters:
            self.default_filters.remove(compute_filter)

    def load_cache_manager(self, cache_manager):
        self.cache_manager = cache_manager

    def get_all_host_states(self, context):
        managers = self.cache_manager.get_active_managers()
        host_states = [SharedHostState(manager,
                                       self._get_aggregates_info(manager.host),
                                       self._get_instance_info(
                                           context,
                                           manager))
                       for manager in managers]
        return host_states
