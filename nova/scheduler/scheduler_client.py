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

from oslo_log import log as logging
import oslo_messaging as messaging

from nova.compute import rpcapi as compute_rpcapi
from nova.i18n import _LI, _LE, _LW
from nova import objects
from nova import servicegroup

LOG = logging.getLogger(__name__)


class APIProxy(object):
    def __init__(self, host):
        self.host = host
        self.servicegroup_api = servicegroup.API()
        self.compute_rpcapi = compute_rpcapi.ComputeAPI()

    def service_is_up(self, service):
        return self.servicegroup_api.service_is_up(service)

    def report_host_state(self, context, compute):
        return self.compute_rpcapi.report_host_state(
                context, compute, self.host)


class SchedulerClients(object):
    def __init__(self, host):
        self.host = host
        self.api = APIProxy(host)
        self.clients = {}

    def periodically_refresh_clients(self, context):
        service_refs = {service.host: service
                        for service in objects.ServiceList.get_by_binary(
                            context, 'nova-compute')}
        service_keys_db = set(service_refs.keys())
        service_keys_cache = set(self.clients.keys())

        new_keys = service_keys_db - service_keys_cache
        old_keys = service_keys_cache - service_keys_db

        for new_key in new_keys:
            client_obj = SchedulerClient(service_refs[new_key].host,
                                         self.api)
            self.clients[new_key] = client_obj
            LOG.info(_LI("Added new client: %s") % new_key)

        for old_key in old_keys:
            client_obj = self.clients[old_key]
            if not client_obj.host_state:
                LOG.error(_LE("Remove client: %s") % old_key)
                del self.clients[old_key]

        for client in self.clients.values():
            client.sync(context, service_refs.get(client.host, None))

    def notify_scheduler(self, context, host_name):
        LOG.info(_LI("Get notified from host %s") % host_name)
        client_obj = self.clients.get(host_name, None)
        if not client_obj:
            client_obj = SchedulerClient(host_name, self.api)
            self.clients[host_name] = client_obj
            LOG.warning(_LW("Added temp client %s from notification.")
                        % host_name)
        client_obj.refresh_state(context)


class SchedulerClient(object):
    def __init__(self, host, api):
        self.host = host
        self.api = api
        self.host_state = None
        self.seems_disabled = True

    def _handle_seems_disabled(self):
        if self.host_state:
            if self.seems_disabled:
                LOG.warn(_LW("Service nova-compute %s is down!")
                         % self.host)
                self.disable()
            else:
                LOG.warn(_LW("Service nova-compute %s seems down!")
                         % self.host)
                self.seems_disabled = True
        else:
            self.disable()

    def sync(self, context, service):
        if not service:
            LOG.warn(_LW("No db entry of nova-compute %s!") % self.host)
            self._handle_seems_disabled()
        elif service['disabled']:
            LOG.warn(_LW("Service nova-compute %s is disabled!") % self.host)
            self.disable()
        elif self.api.service_is_up(service):
            self.seems_disabled = False
            if not self.host_state:
                self.refresh_state(context)
            else:
                # normal situation
                pass
        else:
            # is down in db
            self._handle_seems_disabled()

    def refresh_state(self, context):
        try:
            self.host_state = self.api.report_host_state(context, self.host)
            if not self.host_state:
                LOG.error(_LW("Host %s is not ready yet.") % self.host)
                self.disable()
            else:
                LOG.info(_LI("Client %s is ready!") % self.host)
                LOG.info(_LI("Host state: %s.") % self.host_state)
        except messaging.MessagingTimeout:
            LOG.error(_LE("Client state fetch timeout: %s!") % self.host)
            self.disable()

    def disable(self):
        self.host_state = None
        self.seems_disabled = True
