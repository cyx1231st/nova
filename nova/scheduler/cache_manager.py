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
import random
import sys
import traceback

from oslo_log import log as logging

import nova
from nova.i18n import _, _LI, _LE, _LW
from nova import objects
from nova import servicegroup
from nova import utils

LOG = logging.getLogger(__name__)


class APIProxyBase(object):
    """The api interface of entire shared state manager. """
    def __init__(self, host):
        self.host = host
        self.servicegroup_api = servicegroup.API()
    # TODO(Yingxin): Refactor code to get rid of conductor service when send
    # scheduler decisions to SchedulerServers with claim.

    # TODO(Yingxin): Abstract methods notify_remote(s) and send_messages to
    # APIProxyBase, so that all the communications between scheduler clients
    # and servers are sent through APIProxy.

    # TODO(Yingxin): Implement message driver to transmit all the scheduling
    # messages to remote, the default is oslo_messaging driver. It is better to
    # provide a mechanism to switch to other message drivers using stand-along
    # distributed message queue to transmit messages.

    def service_is_up(self, service):
        return self.servicegroup_api.service_is_up(service)


class RemoteManagerBase(object):
    """The remote scheduler to sync cache or the remote compute node to sync
    cache from.
    """

    DISABLED = "DISABLED"
    STANDBY = "STANDBY"
    ACTIVE = "ACTIVE"

    # TODO(Yingxin): Improve to a more accurate trancient handling based on
    # timestamp.
    _TRANCIENT = "TRANCIENT"
    # TODO(Yingxin): Improve fallenout model to eliminate unecessary refresh
    # attempts but send important refreshes to remote. However, there seems
    # still chances that it may make wrong decisions.
    _FALLENOUT = "FALLENOUT"

    def __init__(self, host, api, manager):
        self.host = host
        self.state = None

        self.api = api
        self.manager = manager
        self._side_affects = set()

        self.standby()
        self.seed = random.randint(0, 1000000)
        LOG.info(_LI("Remote %(host)s start at seed %(seed)d")
                 % {'host': self.host, 'seed': self.seed})

    def _disable(self):
        raise NotImplementedError(
            "RemoteManagerBase._disable not implemented!")

    def _activate(self, **kwargs):
        raise NotImplementedError(
            "RemoteManagerBase._activate not implemented!")

    def _refresh(self, context):
        raise NotImplementedError(
            "RemoteManagerBase._refresh not implemented!")

    def _do_periodical(self):
        pass

    def increase_seed(self):
        self.seed += 1

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
        # not available yet. The misuse of this method can cause fake active
        # state.
        if not self.is_activated():
            LOG.warn(_LW("Remote %s is not active, refreshing...")
                     % self.host)
            self.refresh(context)
            return False
        else:
            return True

    def activate(self, **kwargs):
        self._activate(**kwargs)
        if self._FALLENOUT in self._side_affects:
            self._side_affects.remove(self._FALLENOUT)
        self.manager.active_remotes[self.host] = self
        self.state = self.ACTIVE
        LOG.info(_LI("Remote %s is refreshed and activated!") % self.host)

    def refresh(self, context, force=False):
        """Notify remote to refresh cache due to cache content fallenout or
        service restart, force=True means refresh immediately.
        """

        if force or self._FALLENOUT not in self._side_affects:
            if self.is_disabled():
                self.standby()
            else:
                self._side_affects.add(self._TRANCIENT)
            self._side_affects.add(self._FALLENOUT)
            LOG.info(_LI("Remote %s is to be refreshed!") % self.host)
            self._refresh(context)

    def _handle_trancient(self):
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
        """Sync service state from service group and do periodical jobs to
        clean or timeout some records.
        """

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
    """The class manages RemoteManagers. """
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
        """Get notified by remote, clean and restart inner states. """
        LOG.info(_LI("Get notified by remote %s") % remote_host)
        remote_obj = self._get_remote(remote_host, "notified")
        remote_obj.refresh(context, force=True)

    def periodically_refresh_remotes(self, context):
        """Maintain the managed RemoteManagers periodically. """
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
    """The class records the sent claims, and remove them when got confirmed
    or timeout.
    """

    def __init__(self, label="?"):
        self.claims = {}
        self.old_claims = {}
        self.abort_callback = None
        self.label = label

    def track(self, claim):
        if not self.abort_callback:
            LOG.error(_LE("ClaimRecords %s is disabled!") % self.label)
            return
        self.claims[claim.seed] = claim

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

    def reset(self, abort_callback=None):
        self.claims.clear()
        self.old_claims.clear()
        self.abort_callback = abort_callback


class MessageWindow(object):
    """This class tracks remote messages based on their seed, so any message
    reordering or lost can be detected.
    """

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
        """If MessageWindow a message's seed record larger than seed, return
        False.
        """
        if self.seed is not None and \
                seed < self.seed and self.seed - seed < self.capacity:
            LOG.warn(_LE("Reset failed, seed: %(new)s, %(old)s")
                     % {'new': seed, 'old': self.seed})
            return False
        else:
            return True

    def proceed(self, seed):
        """Raise KeyError() if a message is confirmed lost. """
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
    """This class provides a mechanism to process messages asynchronously, so
    the messages can be merged to reduce pressure of message system. This
    mechanism can also be disabled for better debugging messages between
    clients and servers.
    """

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
            try:
                self.consume_callback(context=self.context,
                                      messages=msgs)
            except Exception as e:
                trace_info = traceback.format_exception(*sys.exc_info())
                LOG.error(_LE("MessagePipe %(l)s unexpected exception %(e)s, "
                              "traceinfo: %(info)s, messages: %(msg)s!") %
                          {'l': self.label, 'e': e, 'info': trace_info,
                           'msg': msgs})

    def activate(self, initial_msg=None):
        self.enabled = True
        if self.async_mode:
            self.queue = queue.Queue()
            if self.thread:
                self.thread.kill()
            self.thread = utils.spawn(self._dispatch_msgs)
        if initial_msg is not None:
            self.put(initial_msg)

    def disable(self):
        self.queue = None
        self.enabled = False
        if self.thread:
            self.thread.kill()

    def put(self, msg):
        if not self.enabled:
            LOG.error(_LE("MessagePipe %(label)s is disabled, "
                          "cannot put msg %(msg)s!")
                      % {'label': self.label, 'msg': msg})
            return
        if self.async_mode:
            if self.thread.dead:
                LOG.error(_LE("MessagePipe %s thread is killed "
                              "unexpectedly, respawn!") % self.label)
                self.thread = utils.spawn(self._dispatch_msgs)
            self.queue.put_nowait(msg)
        else:
            self.consume_callback(context=self.context,
                                  messages=[msg])


def build_commit_from_cache(host_state):
    """Build the CacheCommit to send from servers to clients. """
    return {'cache_refresh': host_state}


def build_commit(claim_reply=None, cache_update=None):
    """Build the CacheCommit to send from servers to clients. """
    if claim_reply:
        claim_replies = [claim_reply]
    else:
        claim_replies = []
    if not cache_update:
        cache_update = {'expected_version': None,
                        'incremental_updates': {},
                        'overwrite_updates': {},
                        'special_updates': {}}
    return {'claim_replies': claim_replies, 'cache_update': cache_update}


def merge_commit(base_commit, append_commit):
    """Merge the CacheCommits to reduce the pressure of message system. """
    if 'cache_refresh' in append_commit:
        raise RuntimeError(_("Commit merging does not support "
                             "refreshing commit %s!") % append_commit)

    # NOTE(Yingxin): Do not modify any content in append_commit, or there will
    # be strange behaviors that are difficult to debug.

    append_replies = append_commit['claim_replies']
    base_commit['claim_replies'].extend(append_replies)

    append_update = append_commit['cache_update']
    base_update = base_commit['cache_update']

    append_version = append_update['expected_version']
    base_version = base_update['expected_version']
    is_reversed = False

    if append_version is None:
        pass
    elif base_version is None:
        base_update['expected_version'] = append_version
    elif base_version < append_version:
        base_update['expected_version'] = append_version
    elif base_version > append_version:
        is_reversed = True
    else:
        raise RuntimeError(_LE("Detected same expected_versions: %(l)s, %(r)!")
                           % {'l': base_commit, 'r': append_commit})

    append_incrementals = append_update['incremental_updates']
    base_incrementals = base_update['incremental_updates']
    append_keys = set(append_incrementals.keys())
    overlap_keys = append_keys & set(base_incrementals.keys())
    new_keys = append_keys - overlap_keys
    for key in overlap_keys:
        base_incrementals[key] += append_incrementals[key]
    for key in new_keys:
        base_incrementals[key] = append_incrementals[key]

    append_overwrites = append_update['overwrite_updates']
    base_overwrites = base_update['overwrite_updates']
    append_keys = set(append_overwrites.keys())
    base_keys = set(base_overwrites.keys())
    overlap_keys = append_keys & base_keys
    if is_reversed:
        overwrite_keys = append_keys - overlap_keys
    else:
        overwrite_keys = append_keys
    for key in overwrite_keys:
        base_overwrites[key] = append_overwrites[key]

    append_specials = append_update['special_updates']
    base_specials = append_update['special_updates']
    if 'metrics' in append_specials:
        if not is_reversed or 'metrics' not in base_specials:
            base_specials['metrics'] = append_specials['metrics']
    # TODO(Yingxin) Incremental update pci_stats
    # TODO(Yingxin) Incremental update numa_topology

    return base_commit
