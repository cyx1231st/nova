Eventually consistent host state README
=======================================

It introduces a new scheduler host manager driver to apply an eventually
consistent update model to scheduler host states. It excludes the involvement
of database during decision making like caching scheduler. But the scheduler
caches are always *almost* up-to-date because they receive incremental updates
directly from compute nodes, and the schedulers know whether or not their
decisions are successful compute nodes. The throughput of this prototype
implementation is boosted to at least *510%* compared with the legacy
implementation in the same 1000-node simulation.

To run and test this prototype, clone this repo in devstack and set
`scheduler_host_manager=shared_host_manager` in "nova.conf". Do not forget to
update "nova/nova.egg-info/entry_points.txt" according to the modified
"setup.cfg". Also note: Multiple scheduler instances can only run on separate
machines, and this prototype supports filter schedulers and caching schedulers
running at the same time.

To understand its whole design, the detailed specification is in
https://review.openstack.org/#/c/306844/

Performance evaluation in real OpenStack environment
----------------------------------------------------

A generic tool is implemented to evaluate the performance of filter scheduler,
caching scheduler and this prototype in various configuations and in real
OpenStack environment: https://github.com/cyx1231st/nova-scheduler-bench

A very detailed performance analysis is made based on this tool to compare the
performance with existing implementations. It is
http://lists.openstack.org/pipermail/openstack-dev/2016-June/098202.html

Performance modeling
--------------------

Another benchmarking tool is implemented to verify the general idea of this
design by modeling the implementation:
https://github.com/cyx1231st/placement-bench

The detailed analysis about why it should have great improvement lies in
http://lists.openstack.org/pipermail/openstack-dev/2016-March/087889.html


OpenStack Nova README
=====================

OpenStack Nova provides a cloud computing fabric controller,
supporting a wide variety of virtualization technologies,
including KVM, Xen, LXC, VMware, and more. In addition to
its native API, it includes compatibility with the commonly
encountered Amazon EC2 and S3 APIs.

OpenStack Nova is distributed under the terms of the Apache
License, Version 2.0. The full terms and conditions of this
license are detailed in the LICENSE file.

Nova primarily consists of a set of Python daemons, though
it requires and integrates with a number of native system
components for databases, messaging and virtualization
capabilities.

To keep updated with new developments in the OpenStack project
follow `@openstack <http://twitter.com/openstack>`_ on Twitter.

To learn how to deploy OpenStack Nova, consult the documentation
available online at:

   http://docs.openstack.org

For information about the different compute (hypervisor) drivers
supported by Nova, read this page on the wiki:

   https://wiki.openstack.org/wiki/HypervisorSupportMatrix

In the unfortunate event that bugs are discovered, they should
be reported to the appropriate bug tracker. If you obtained
the software from a 3rd party operating system vendor, it is
often wise to use their own bug tracker for reporting problems.
In all other cases use the master OpenStack bug tracker,
available at:

   http://bugs.launchpad.net/nova

Developers wishing to work on the OpenStack Nova project should
always base their work on the latest Nova code, available from
the master GIT repository at:

   https://git.openstack.org/cgit/openstack/nova

Developers should also join the discussion on the mailing list,
at:

   http://lists.openstack.org/cgi-bin/mailman/listinfo/openstack-dev

Any new code must follow the development guidelines detailed
in the HACKING.rst file, and pass all unit tests. Further
developer focused documentation is available at:

   http://docs.openstack.org/developer/nova/

For information on how to contribute to Nova, please see the
contents of the CONTRIBUTING.rst file.

-- End of broadcast
