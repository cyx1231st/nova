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

from nova.i18n import _LI, _LE, _LW
from nova import objects
from nova import servicegroup

LOG = logging.getLogger(__name__)


class APIProxyBase(object):
    def __init__(self, host):
        self.host = host
        self.servicegroup_api = servicegroup.API()

    def service_is_up(self, service):
        return self.servicegroup_api.service_is_up(service)


class RemoteManagerBase(object):
    DISABLED = "DISABLED"
    STANDBY = "STANDBY"
    ACTIVE = "ACTIVE"
    STATES = {DISABLED, STANDBY, ACTIVE}

    _TRANCIENT = "TRANCIENT"
    _FALLENOUT = "FALLENOUT"
    _SIDE_AFFECTS = {_TRANCIENT, _FALLENOUT}

    def __init__(self, host, api, manager, nodename=None):
        self.host = host
        self.nodename = host
        self.state = None

        self.api = api
        self.manager = manager
        self._side_affects = set()

        self.standby()

    def _disable(self):
        pass

    def _activate(self, item, seed):
        pass

    def _refresh(self, context):
        pass

    def _do_periodical(self):
        pass

    def disable(self):
        if self.state != self.DISABLED:
            LOG.info(_LI("Disable remote %s.") % self.host)
            self.state = self.DISABLED
            self._side_affects.clear()
            self._disable()
        else:
            pass

    def standby(self):
        self.state = self.STANDBY
        self._side_affects.clear()
        self._side_affects.add(self._TRANCIENT)

    def is_disabled(self):
        return self.state == self.DISABLED

    def is_activated(self):
        return self.state == self.ACTIVE

    def expect_active(self, context):
        if not self.is_activated():
            LOG.error(_LE("Remote %s is not active, refreshing...")
                      % self.host)
            self.refresh(context)
            return False
        else:
            return True


    def activate(self, item=None, seed=None):
        LOG.info(_LE("Remote %s is activated!") % self.host)
        self.disable()
        self.state = self.ACTIVE
        self._activate(item, seed)

    def refresh(self, context):
        if self._FALLENOUT not in self._side_affects:
            if self.is_disabled():
                self.standby()
            else:
                self._side_affects.add(self._FALLENOUT)
            self._side_affects.add(self._TRANCIENT)
            LOG.info(_LI("Remote %s is to be refreshed!") % self.host)
            self._refresh(context)

    def _handle_trancient(self):
        if self.is_disabled():
            return
        try:
            self._side_affects.remove(self._TRANCIENT)
            LOG.info(_LI("Keep trancient remote %s") % self.host)
        except KeyError:
            LOG.info(_LI("Disable trancient remote %s") % self.host)
            self.disable()

    def sync(self, context, service):
        if not service:
            LOG.info(_LI("Remote %s has no service record!") % self.host)
            self._handle_trancient()
        elif service['disabled']:
            LOG.info(_LI("Remote %s is disabled!") % self.host)
            self.disable()
        elif self.api.service_is_up(service):
            if not self.is_activated():
                LOG.info(_LI("Remote %s is up!") % self.host)
                self.refresh(context)
            else:
                if self._TRANCIENT in self._side_affects:
                    self._side_affects.remove(self._TRANCIENT)
                if self._FALLENOUT in self._side_affects:
                    self._side_affects.remove(self._FALLENOUT)
                    self.refresh(context)
                self._do_periodical()
        else:
            if not self.is_disabled():
                LOG.info(_LI("Remote %s heartbeat timeout!") % self.host)
                self._handle_trancient()


class CacheManagerBase(object):
    API_PROXY = APIProxyBase
    REMOTE_MANAGER = RemoteManagerBase
    SERVICE_NAME = None

    def __init__(self, host):
        self.host = host
        self.api = self.API_PROXY(host)
        self.remotes = {}

    def periodically_refresh_remotes(self, context):
        service_refs = {service.host: service
                        for service in objects.ServiceList.get_by_binary(
                            context, self.SERVICE_NAME)}
        service_keys_db = set(service_refs.keys())
        service_keys_cache = set(self.remotes.keys())

        new_keys = service_keys_db - service_keys_cache
        old_keys = service_keys_cache - service_keys_db

        for new_key in new_keys:
            remote_obj = self.REMOTE_MANAGER(service_refs[new_key].host,
                                             self.api, self)
            self.remotes[new_key] = remote_obj
            LOG.info(_LI("Added new remote %s from db.") % new_key)

        for old_key in old_keys:
            remote_obj = self.remotes[old_key]
            if remote_obj.is_disabled():
                LOG.error(_LE("Remove non-exist remote %s") % old_key)
                del self.remotes[old_key]
            else:
                LOG.info(_LI("Keep non-exist remote %s") % old_key)

        for remote in self.remotes.values():
            remote.sync(context, service_refs.get(remote.host, None))

    def _get_remote(self, host, label):
        remote_obj = self.remotes.get(host, None)
        if not remote_obj:
            remote_obj = self.REMOTE_MANAGER(host, self.api, self)
            self.remotes[host] = remote_obj
            LOG.warn(_LW("Added new remote %(host)s labeled %(label)s"
                     % {'host': host, 'label': label}))
        return remote_obj

    def notified_by_remote(self, context, remote_host):
        LOG.info(_LI("Get notified by remote %s") % remote_host)
        remote_obj = self._get_remote(remote_host, "notified")
        remote_obj.refresh(context)
