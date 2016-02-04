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

import bisect
from eventlet import queue
from functools import partial

from oslo_log import log as logging

import nova
from nova.i18n import _LI, _LE, _LW
from nova import objects
from nova import servicegroup
from nova import utils

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

    _TRANCIENT = "TRANCIENT"
    _FALLENOUT = "FALLENOUT"

    def __init__(self, host, api, manager):
        self.host = host
        self.state = None

        self.api = api
        self.manager = manager
        self._side_affects = set()

        self.standby()

    def _disable(self):
        raise NotImplementedError(
                "RemoteManagerBase._disable not implemented!")

    def _activate(self, item, seed):
        raise NotImplementedError(
                "RemoteManagerBase._activate not implemented!")

    def _refresh(self, context):
        raise NotImplementedError(
                "RemoteManagerBase._refresh not implemented!")

    def _do_periodical(self):
        pass

    def standby(self):
        LOG.info(_LI("Remote %s is in standby mode!") % self.host)
        self._side_affects.clear()
        self._side_affects.add(self._TRANCIENT)
        self.state = self.STANDBY

    def is_disabled(self):
        return self.state == self.DISABLED

    def disable(self):
        if self.state != self.DISABLED:
            self._side_affects.clear()
            self.manager.active_remotes.pop(self.host, None)
            self.state = self.DISABLED
            LOG.info(_LI("Remote %s is disabled!") % self.host)
            self._disable()
        else:
            pass

    def is_activated(self):
        return self.state == self.ACTIVE

    def expect_active(self, context):
        # NOTE(Yingxin): This should be only used in methods called by RPC,
        # in order to notify remote services even if the servicegroup record is
        # not available. The misuse of this method can cause fake active
        # state.
        if not self.is_activated():
            LOG.error(_LE("Remote %s is not active, refreshing...")
                      % self.host)
            self.refresh(context)
            return False
        else:
            return True

    def activate(self, item=None, seed=None):
        # TODO(Yingxin): remove extra arguments
        self._activate(item, seed)
        if self._FALLENOUT in self._side_affects:
            self._side_affects.remove(self._FALLENOUT)
        self.manager.active_remotes[self.host] = self
        self.state = self.ACTIVE
        LOG.info(_LI("Remote %s is refreshed and activated!") % self.host)

    def refresh(self, context, force=False):
        if force or self._FALLENOUT not in self._side_affects:
            if self.is_disabled():
                self.standby()
            else:
                self._side_affects.add(self._TRANCIENT)
            self._side_affects.add(self._FALLENOUT)
            LOG.info(_LI("Remote %s is to be refreshed!") % self.host)
            self._refresh(context)

    def _handle_trancient(self):
        # TODO(Yingxin): change to timeout and disable
        if not self.is_activated():
            self.disable()
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
            LOG.info(_LI("Remote %s service is disabled!") % self.host)
            self.disable()
        elif self.api.service_is_up(service):
            if not self.is_activated():
                LOG.info(_LI("Remote %s service is up!") % self.host)
                self._side_affects.clear()
                self.refresh(context)
            else:
                if self._TRANCIENT in self._side_affects:
                    self._side_affects.remove(self._TRANCIENT)
                if self._FALLENOUT in self._side_affects:
                    LOG.error(_LE("Remote %s is still not ready, "
                                  "refresh again!") % self.host)
                    self._side_affects.remove(self._FALLENOUT)
                    self.refresh(context)
                self._do_periodical()
        else:
            if not self.is_disabled():
                LOG.info(_LI("Remote %s service heartbeat timeout!")
                         % self.host)
                self._handle_trancient()


class CacheManagerBase(object):
    API_PROXY = APIProxyBase
    REMOTE_MANAGER = RemoteManagerBase
    SERVICE_NAME = None

    def __init__(self, host):
        self.host = host
        self.api = self.API_PROXY(host)
        self.remotes = {}
        self.active_remotes = {}

    def _do_periodical(self):
        pass

    def _get_remote(self, host, label):
        remote_obj = self.remotes.get(host, None)
        if not remote_obj:
            remote_obj = self.REMOTE_MANAGER(host, self.api, self)
            self.remotes[host] = remote_obj
            LOG.warn(_LW("Added new remote %(host)s labeled %(label)s")
                     % {'host': host, 'label': label})
        return remote_obj

    def notified_by_remote(self, context, remote_host):
        LOG.info(_LI("Get notified by remote %s") % remote_host)
        remote_obj = self._get_remote(remote_host, "notified")
        remote_obj.refresh(context, force=True)

    def periodically_refresh_remotes(self, context):
        service_refs = {service.host: service
                        for service in objects.ServiceList.get_by_binary(
                            context, self.SERVICE_NAME)}
        service_keys_db = set(service_refs.keys())
        service_keys_cache = set(self.remotes.keys())

        new_keys = service_keys_db - service_keys_cache
        old_keys = service_keys_cache - service_keys_db

        for new_key in new_keys:
            LOG.info(_LI("Add new remote %s from db.") % new_key)
            remote_obj = self.REMOTE_MANAGER(service_refs[new_key].host,
                                             self.api, self)
            self.remotes[new_key] = remote_obj

        for old_key in old_keys:
            remote_obj = self.remotes[old_key]
            if remote_obj.is_disabled():
                LOG.error(_LE("Remove non-exist remote %s") % old_key)
                del self.remotes[old_key]
            else:
                LOG.info(_LI("Keep non-exist remote %s") % old_key)

        for remote in self.remotes.values():
            remote.sync(context, service_refs.get(remote.host, None))

        self._do_periodical()

    def get_active_managers(self):
        return self.active_remotes.values()


class ClaimRecords(object):
    def __init__(self, label="?"):
        self.claims = {}
        self.old_claims = {}
        self.abort_callback = None
        self.label = label

    def track(self, seed, claim):
        if not self.abort_callback:
            LOG.error(_LE("ClaimRecords %s is disabled!") % self.label)
            return
        self.claims[seed] = claim

    def timeout(self):
        if not self.abort_callback:
            LOG.error(_LE("ClaimRecords %s is disabled!") % self.label)
            return
        timeout_claims = self.old_claims.values()
        if timeout_claims:
            LOG.warn(_LW("ClaimRecords %(label)s timeout claims %(claims)s")
                     % {'label': self.label, 'claims': timeout_claims})
            for claim in timeout_claims:
                self.abort_callback(claim)
        self.old_claims = self.claims
        self.claims = {}

    def pop(self, seed):
        if not self.abort_callback:
            LOG.error(_LE("ClaimRecords %s is disabled!") % self.label)
            return
        claim = self.claims.pop(seed, None)
        old_claim = self.old_claims.pop(seed, None)
        return claim or old_claim

    def reset(self, cache=None):
        self.claims.clear()
        self.old_claims.clear()
        if cache:
            self.abort_callback = \
                    partial(cache.process_claim, proceed = False)
        else:
            self.abort_callback = None


class MessageWindow(object):
    def __init__(self, capacity=7, label="?"):
        if capacity < 1:
            LOG.error(_LE("Window capacity %s < 1, set to 1!") % capacity)
            capacity = 1
        self.capacity = capacity
        self.window = []
        self.seed = None
        self.label = label

    def reset(self, seed=None):
        self.window = []
        self.seed = seed

    def try_reset(self, seed):
        if self.seed is not None and \
                seed < self.seed and self.seed - seed < self.capacity:
            LOG.warn(_LE("Reset failed, seed: %(new)s, %(old)s")
                     % {'new': seed, 'old': self.seed})
            return False
        else:
            return True

    def proceed(self, seed):
        if not self.seed:
            LOG.error(_LE("MessageWindow %s is disabled!") % self.label)
            return False

        if seed <= self.seed:
            index = bisect.bisect_left(self.window, seed)
            if seed == self.seed or self.window[index] != seed:
                LOG.error(_LE("Deprecated message#%d, ignore!") % seed)
                return False
            else:
                LOG.warn(_LW("A lost message#%d!") % seed)
                del self.window[index]
        elif seed == self.seed + 1:
            self.seed = seed
        else:  # seed > self.seed + 1
            if seed - self.seed > self.capacity:
                LOG.error(_LE("A gient gap between %(from)d and %(to)d, "
                    "refresh state!") % {'from': self.seed, 'to': seed})
                self.reset()
                raise KeyError()
            else:
                for i in range(self.seed + 1, seed):
                    self.window.append(i)
                self.seed = seed

        if self.window:
            LOG.info(_LI("Missing commits: %s.") % self.window)
            if self.seed - self.window[0] >= self.capacity:
                LOG.error(_LE("Lost exceed window capacity %d, abort!")
                          % self.capacity)
                self.reset()
                raise KeyError()

        return True


class MessagePipe(object):
    def __init__(self, consume_callback, async_mode=True, label="?"):
        self.async_mode = async_mode
        self.queue = None
        self.thread = None
        self.consume_callback = consume_callback
        self.context = nova.context.get_admin_context()
        self.enabled = False
        self.label = label

    def _dispatch_msgs(self):
        while True:
            if not self.enabled:
                LOG.error(_LE("MessagePipe %s is disabled, cannot spawn!")
                          % self.label)
                return
            msgs = []
            msgs.append(self.queue.get())
            for i in range(self.queue.qsize(), 0, -1):
                msgs.append(self.queue.get_nowait())
            self.consume_callback(context=self.context,
                                  messages=msgs)

    def activate(self, initial_msg=None):
        if self.async_mode:
            self.queue = queue.Queue()
            if self.thread:
                self.thread.kill()
            self.thread = utils.spawn(self._dispatch_msgs)
        self.enabled = True
        if initial_msg is not None:
            self.put(initial_msg)

    def disable(self):
        self.queue = None
        if self.thread:
            self.thread.kill()
        self.enabled = False

    def put(self, msg):
        if not self.enabled:
            LOG.error(_LE("MessagePipe %(label)s is disabled, "
                          "cannot put msg %(msg)s!")
                      % {'label': self.label, 'msg': msg})
            return
        if self.async_mode:
            self.queue.put_nowait(msg)
        else:
            self.consume_callback(context=self.context,
                                  messages=[msg])
