import json
import errno
import six
import os
import tempfile

from mgr_module import MgrModule
import orchestrator

from . import remotes

try:
    import remoto
    import remoto.process
except ImportError as e:
    remoto = None
    remoto_import_error = str(e)

# todo
# - batch osd creation
# - osd spec for filestore, wal-db, etc...
# - handling dmcrypt and zapping etc...
# - better handling of ssh_config like cleaning up and reusing tmp ssh_config file

# setup
#  1. prepare bootstrap keys
#       (a) create bootstrap keys, OR
#       (b) allow 'profile mgr' to create keys
#       ==> bootstrap keys automatically generated

# todo next
#  1. remotes:write_conf: specialize for no overwrite
#  2. clean up temp file

# cd: figure mon ip (from config, explicit, or resolve host)
# cd: check if hostname is compatible
# cd: distro.mon.add(distro, args, monitor_keyring)
# cd: catch mon errors/check mon statsu

# add things to guard against doulbe creation (e.g. DONE files etc..., error on
# creating dirs that are expected to not exist, etc...)

# mon create: uid/gid etc...

# mon create: clean up tmp files mon map etc..

# todo
#  0: use `orchestrator add host`
#  1. non-default bootstrap keyring path in ceph-volume
#  2. use cluster names (see ceph-volume: --cluster-fsid)
#  3. lots better error handling and error reporting
#  4. read/write completion life cycle
#  5. separate prepare/activate for containerized osd deployment: invoke docker
#  6: add_host test connectiveity
# - report when adding mgr to node with existing mgr (current silent success)
# - how to get the cluster name: from the cli? we should be able to pull it out
# of the cct_->conf unless we actually want to do something different here. for
# now make this global and add thsi comment to that.
# https://github.com/alfredodeza/remoto/commit/3e33494409ff754d1bba467d32dae68523e06ede

# here is an example of where it'd be nice to generate keys in the
# manager... we have to run out to the remote node and use the bootstrap
# key there. since we have it anyway, i'm not sure what the problem is.
# it'd make things much cleaner!



# there is a ton of stuff to do, but the POC is at a point where it's probably a
# good idea to prioritize what to look at in the next couple days.

# what's been done
#  - harvested ceph-deploy for its organs
#  - register hosts
#  - fetch device inventory from remote hosts
#  - create new osd on a remote host with a single device (limited spec)

# next: depth vs breadth. when to commit or abort

# replace, remove, upgrade, add mon, rgw, mds, etc...

# priority todo?
# - ceph bootstrap
#      - packages
#      - bootstrap keys, conf (how to generate?: cd reads locall here...)
# **** - inventory bootstrap (e.g. from ansible...?)
# - private keys
#      - probably need alfredo help here
# - placement
#      - explicit?
# - container scenario? ceph-volume support via systemd?

# random todos
# - worker pool for background completion and parallelism
# - setup logging (better logging for remoto)
# - error reporting is not good
# - update progress module
# - completely untenable installation of dependencies
#      - need source installation (not egg)
#      - ceph/remoto vs alfreo/remoto ceph_path vs check_env
# - node locking and operation ordering

# thanks ceph-deploy
class SSHReadCompletion(orchestrator.ReadCompletion):
    def __init__(self, result):
        super(SSHReadCompletion, self).__init__()
        self._result = result

    @property
    def result(self):
        return self._result

    @property
    def is_complete(self):
        return True

class SSHWriteCompletion(orchestrator.WriteCompletion):
    def __init__(self, result):
        super(SSHWriteCompletion, self).__init__()
        self._result = result

    @property
    def is_persistent(self):
        return True

    @property
    def is_effective(self):
        return True

    @property
    def is_errored(self):
        return False

class SSHOrchestrator(MgrModule, orchestrator.Orchestrator):
    def __init__(self, *args, **kwargs):
        super(SSHOrchestrator, self).__init__(*args, **kwargs)
        self._cluster_fsid = None

    @staticmethod
    def can_run():
        if remoto is not None:
            return True, ""
        else:
            return False, "loading remoto library:{}".format(
                    remoto_import_error)

    def available(self):
        """
        The SSH orchestrator is always available.

        TODO:
          - changes that might affect this are: no registered hosts, no ssh
            keys, cannot SSH into one of the hosts, etc...
        """
        return self.can_run()

    def wait(self, completions):
        self.log.error("wait: completions={}".format(completions))
        for c in completions:
            assert c.is_complete
        return True

    def _get_cluster_fsid(self):
        """
        Fetch (and cache) the cluster fsid.
        """
        if not self._cluster_fsid:
            self._cluster_fsid = self.get("mon_map")["fsid"]
        assert isinstance(self._cluster_fsid, six.string_types)
        return self._cluster_fsid

    def _require_hosts(self, hosts):
        """
        Raise an error if any of the given hosts are unregistered.
        """
        if isinstance(hosts, six.string_types):
            hosts = [hosts]
        unregistered_hosts = []
        for host in hosts:
            if not self.get_store("host.{}".format(host)):
                unregistered_hosts.append(host)
        if unregistered_hosts:
            raise RuntimeError("Host(s) {} not registered".format(
                ", ".join(map(lambda h: "'{}'".format(h),
                    unregistered_hosts))))

    def _get_connection(self, host):
        """
        Setup a connection for running remote commands.

        :param host: the remote host
        :return: the connection object
        """
        ssh_config = self.get_store("conf.ssh_config")
        if ssh_config:
            self.ssh_config_file = tempfile.NamedTemporaryFile()
            self.ssh_config_file.write(ssh_config.encode('utf-8'))
            self.ssh_config_file.flush()
            ssh_options = "-F {}".format(self.ssh_config_file.name)
        else:
            ssh_options = None

        conn = remoto.Connection(host,
                logger=self.log,
                detect_sudo=True,
                ssh_options=ssh_options)

        conn.import_module(remotes)

        return conn

    def _executable_path(self, conn, executable):
        """
        Remote validator that accepts a connection object to ensure that a certain
        executable is available returning its full path if so.

        Otherwise an exception with thorough details will be raised, informing the
        user that the executable was not found.
        """
        executable_path = conn.remote_module.which(executable)
        if not executable_path:
            raise ExecutableNotFound(executable, conn.hostname)
        return executable_path

    def _get_host_inventory(self, conn):
        """
        Query storage devices on a remote node.
        """
        ceph_volume_executable = self._executable_path(conn, 'ceph-volume')
        command = [
            ceph_volume_executable,
            "inventory",
            "--format=json"
        ]
        out, err, code = remoto.process.check(conn, command)
        try:
            out = out[-1]

            # FIXME: invalid json hack for ceph-volume output
            out = out.decode('utf-8')
            out = out.replace("'", '"')
            out = out.replace("False", "\"false\"")
            out = out.replace("True", "\"true\"")

            return json.loads(out)
        except ValueError:
            return {}

    def _build_ceph_conf(self):
        """
        Build a minimal `ceph.conf` containing the current monitor hosts.

        Notes:
          - ceph-volume complains if no section header (e.g. global) exists
          - other ceph-cli tools complained about no EOF newline
        """
        mon_map = self.get("mon_map")
        mon_addrs = map(lambda m: m["addr"], mon_map["mons"])
        mon_hosts = ", ".join(mon_addrs)
        return "[global]\nmon host = {}\n".format(mon_hosts)

    def _ensure_ceph_conf(self, conn):
        """
        Install ceph.conf on remote note if it doesn't exist.
        """
        path = "/etc/ceph/ceph.conf"
        conf = self._build_ceph_conf()
        conn.remote_module.write_conf(path, conf)

    def _get_bootstrap_key(self, service_type):
        """
        Fetch a bootstrap key for a service type.

        :param service_type: name (e.g. mds, osd, mon, ...)
        """
        identity_dict = {
            'admin' : 'client.admin',
            'mds' : 'client.bootstrap-mds',
            'mgr' : 'client.bootstrap-mgr',
            'osd' : 'client.bootstrap-osd',
            'rgw' : 'client.bootstrap-rgw',
            'mon' : 'mon.'
        }

        identity = identity_dict[service_type]

        ret, out, err = self.mon_command({
            "prefix": "auth get",
            "entity": identity
        })

        if ret == -errno.ENOENT:
            self.log.error("entity not found")

        if ret != 0:
            return None

        return out

    def _bootstrap_mgr(self, conn):
        """
        Bootstrap a manager.

          1. install a copy of ceph.conf
          2. install the manager bootstrap key

        :param conn: remote host connection
        """
        self._ensure_ceph_conf(conn)
        keyring = self._get_bootstrap_key("mgr")
        keyring_path = "/var/lib/ceph/bootstrap-mgr/ceph.keyring"
        conn.remote_module.write_keyring(keyring_path, keyring)
        return keyring_path

    #  - cache flag: has node has already been bootstrapped?
    #  - use non-local bootstrap path in ceph-volume
    def _bootstrap_osd(self, conn):
        """
        Bootstrap an osd.

          1. install a copy of ceph.conf
          2. install the osd bootstrap key

        :param conn: remote host connection
        """
        self._ensure_ceph_conf(conn)
        keyring = self._get_bootstrap_key("osd")
        keyring_path = "/var/lib/ceph/bootstrap-osd/ceph.keyring"
        conn.remote_module.write_keyring(keyring_path, keyring)
        return keyring_path

    # TODO: grab mon map here rather than remotely (then don't need admin key?)
    def _create_mon(self, host):
        """
        Create a new monitor on the given host.

        :param host: host name
        """
        conn = self._get_connection(host)

        self._ensure_ceph_conf(conn)

        uid = conn.remote_module.path_getuid("/var/lib/ceph")
        gid = conn.remote_module.path_getgid("/var/lib/ceph")

        # install client admin key on target mon host
        # TODO: remove when we are able to install monmap from mgr blob
        admin_keyring = self._get_bootstrap_key("admin")
        admin_keyring_path = '/etc/ceph/ceph.client.admin.keyring'
        conn.remote_module.write_keyring(admin_keyring_path, admin_keyring, uid, gid)

        mon_path = "/var/lib/ceph/mon/ceph-{name}".format(name=host)
        conn.remote_module.create_mon_path(mon_path, uid, gid)

        # bootstrap key
        # TODO: remove after creation
        conn.remote_module.safe_makedirs("/var/lib/ceph/tmp")
        monitor_keyring = self._get_bootstrap_key("mon")
        mon_keyring_path = "/var/lib/ceph/tmp/ceph-{name}.mon.keyring".format(name=host)
        conn.remote_module.write_file(
            mon_keyring_path,
            monitor_keyring,
            0o600,
            None,
            uid,
            gid
        )

        # monitor map
        monmap_path = "/var/lib/ceph/tmp/ceph.{name}.monmap".format(name=host)
        remoto.process.run(conn,
            ['ceph', 'mon', 'getmap', '-o', monmap_path],
        )

        user_args = []
        if uid != 0:
            user_args = user_args + [ '--setuser', str(uid) ]
        if gid != 0:
            user_args = user_args + [ '--setgroup', str(gid) ]

        remoto.process.run(conn,
            ['ceph-mon', '--mkfs', '-i', host,
             '--monmap', monmap_path, '--keyring', mon_keyring_path
            ] + user_args
        )

        remoto.process.run(conn,
            ['systemctl', 'enable', 'ceph.target'],
            timeout=7,
        )

        remoto.process.run(conn,
            ['systemctl', 'enable', 'ceph-mon@{name}'.format(name=host)],
            timeout=7,
        )

        remoto.process.run(conn,
            ['systemctl', 'start', 'ceph-mon@{name}'.format(name=host)],
            timeout=7,
        )

        conn.exit()

    def _create_mgr(self, host):
        """
        Create a new manager instance on a host.

        :param host: host name.
        """
        conn = self._get_connection(host)
        bootstrap_keyring_path = self._bootstrap_mgr(conn)

        mgr_path = "/var/lib/ceph/mgr/ceph-{name}".format(name=host)
        conn.remote_module.safe_makedirs(mgr_path)
        keyring_path = os.path.join(mgr_path, "keyring")

        command = [
            'ceph',
            '--name', 'client.bootstrap-mgr',
            '--keyring', bootstrap_keyring_path,
            'auth', 'get-or-create', 'mgr.{name}'.format(name=host),
            'mon', 'allow profile mgr',
            'osd', 'allow *',
            'mds', 'allow *',
            '-o',
            keyring_path
        ]

        out, err, ret = remoto.process.check(conn, command)
        if ret != 0:
            raise Exception("oops")

        remoto.process.run(conn,
            ['systemctl', 'enable', 'ceph-mgr@{name}'.format(name=host)],
            timeout=7
        )

        remoto.process.run(conn,
            ['systemctl', 'start', 'ceph-mgr@{name}'.format(name=host)],
            timeout=7
        )

        remoto.process.run(conn,
            ['systemctl', 'enable', 'ceph.target'],
            timeout=7
        )

        conn.exit()

    def add_host(self, host):
        """
        Add a host to be managed by the orchestrator.

        :param host: host name
        """
        key = "host.{}".format(host)
        self.set_store(key, host)
        return SSHWriteCompletion("added host: {}".format(host))

    def remove_host(self, host):
        """
        Remove a host from orchestrator management.

        :param host: host name
        """
        key = "host.{}".format(host)
        self.set_store(key, None)
        return SSHWriteCompletion("removed host: {}".format(host))

    def get_hosts(self):
        """
        Return a list of hosts managed by the orchestrator.
        """
        nodes = []
        for host_info in six.iteritems(self.get_store_prefix("host.")):
            node = orchestrator.InventoryNode(host_info[1], [])
            nodes.append(node)
        return SSHReadCompletion(nodes)

    def get_inventory(self, node_filter=None):
        """
        Return the storage inventory of nodes matching the given filter.

        :param node_filter: node filter
        """
        nodes = []
        for host_info in six.iteritems(self.get_store_prefix("host.")):
            host = host_info[1]
            conn = self._get_connection(host)

            devices = []
            for dev_info in self._get_host_inventory(conn):
                dev = orchestrator.InventoryDevice()
                dev.blank = dev_info["available"]
                dev.id = dev_info["path"]
                dev.size = dev_info["sys_api"]["size"]
                devices.append(dev)

            node = orchestrator.InventoryNode(host, devices)
            nodes.append(node)
            conn.exit()

        return SSHReadCompletion(nodes)

    def create_osds(self, spec):
        """
        Create a new osd.

        The orchestrator CLI currently handles a narrow form of drive
        specification defined by a single block device using bluestore.

        :param spec: osd specification
        """
        assert spec.format == "bluestore"
        assert spec.node is not None
        assert isinstance(spec.drive_group.devices, list)
        assert len(spec.drive_group.devices) == 1
        assert isinstance(spec.drive_group.devices[0], six.string_types)
        host = spec.node

        self._require_hosts(host)
        conn = self._get_connection(host)
        self._bootstrap_osd(conn)

        ceph_volume_executable = self._executable_path(conn, 'ceph-volume')
        command = [
            ceph_volume_executable,
            'lvm',
            'create',
            '--cluster-fsid', self._get_cluster_fsid(),
            '--%s' % spec.format,
            '--data', spec.drive_group.devices[0]
        ]

        remoto.process.run(conn, command)
        conn.exit()

        return SSHWriteCompletion("done")

    def update_mons(self, num, hosts):
        """
        Adjust the number of cluster monitors.

        Notes:
          - updates limited to adding monitors
        """
        # check that the request is for adding managers
        mon_map = self.get("mon_map")
        num_mons = len(mon_map["mons"])
        if num == num_mons:
            return SSHWriteCompletion("requested monitors present")
        if num < num_mons:
            raise NotImplementedError("cannot remove monitors")

        # check that all the hosts are registered
        hosts = list(set(hosts))
        self._require_hosts(hosts)

        # explicit placement: enough hosts provided?
        num_new_mons = num - num_mons
        if len(hosts) < num_new_mons:
            raise RuntimeError("provide one host per new monitor")

        # current limitation: add one new monitor at a time
        assert num_new_mons == 1
        self._create_mon(hosts[0])

        return SSHWriteCompletion("done")

    def update_mgrs(self, num, hosts):
        """
        Adjust the number of cluster managers.

        Notes:
          - updates limited to adding managers
        """
        # check that the request is for adding managers
        mgr_map = self.get("mgr_map")
        num_mgrs = 1 if mgr_map["active_name"] else 0
        num_mgrs += len(mgr_map["standbys"])
        if num == num_mgrs:
            return SSHWriteCompletion("requested managers present")
        if num < num_mgrs:
            raise NotImplementedError("cannot remove managers")

        # check that all the hosts are registered
        hosts = list(set(hosts))
        self._require_hosts(hosts)

        # explicit placement: enough hosts provided?
        num_new_mgrs = num - num_mgrs
        if len(hosts) < num_new_mgrs:
            raise RuntimeError("provide one host per new manager")

        for i in range(num_new_mgrs):
            self._create_mgr(hosts[i])

        return SSHWriteCompletion("done")
