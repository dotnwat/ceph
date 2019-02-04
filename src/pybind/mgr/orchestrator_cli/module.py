import errno
import json

try:
    from typing import Dict
except ImportError:
    pass  # just for type checking.

from mgr_module import MgrModule, HandleCommandResult

import orchestrator


class NoOrchestrator(Exception):
    pass


class OrchestratorCli(orchestrator.OrchestratorClientMixin, MgrModule):
    MODULE_OPTIONS = [
        {'name': 'orchestrator'}
    ]
    COMMANDS = [
        {
            'cmd': "orchestrator device ls "
                   "name=host,type=CephString,req=false "
                   "name=format,type=CephChoices,strings=json|plain,req=false ",
            "desc": "List devices on a node",
            "perm": "r"
        },
        {
            'cmd': "orchestrator service ls "
                   "name=host,type=CephString,req=false "
                   "name=svc_type,type=CephString,req=false "
                   "name=svc_id,type=CephString,req=false "
                   "name=format,type=CephChoices,strings=json|plain,req=false ",
            "desc": "List services known to orchestrator" ,
            "perm": "r"
        },
        {
            'cmd': "orchestrator service status "
                   "name=host,type=CephString,req=false "
                   "name=svc_type,type=CephString "
                   "name=svc_id,type=CephString "
                   "name=format,type=CephChoices,strings=json|plain,req=false ",
            "desc": "Get orchestrator state for Ceph service",
            "perm": "r"
        },
        {
            'cmd': "orchestrator osd create "
                   "name=svc_arg,type=CephString,req=false ",
            "desc": "Create an OSD service. Either --svc_arg=host:drives or -i <drive_group>",
            "perm": "rw"
        },
        {
            'cmd': "orchestrator osd rm "
                   "name=svc_id,type=CephString,n=N ",
            "desc": "Remove an OSD service",
            "perm": "rw"
        },
        {
            'cmd': "orchestrator mds add "
                   "name=svc_arg,type=CephString ",
            "desc": "Create an MDS service",
            "perm": "rw"
        },
        {
            'cmd': "orchestrator mds rm "
                   "name=svc_id,type=CephString ",
            "desc": "Remove an MDS service",
            "perm": "rw"
        },
        {
            'cmd': "orchestrator rgw add "
                   "name=svc_arg,type=CephString ",
            "desc": "Create an RGW service",
            "perm": "rw"
        },
        {
            'cmd': "orchestrator rgw rm "
                   "name=svc_id,type=CephString ",
            "desc": "Remove an RGW service",
            "perm": "rw"
        },
        {
            'cmd': "orchestrator nfs add "
                   "name=svc_arg,type=CephString "
                   "name=pool,type=CephString "
                   "name=namespace,type=CephString,req=false ",
            "desc": "Create an NFS service",
            "perm": "rw"
        },
        {
            'cmd': "orchestrator nfs rm "
                   "name=svc_id,type=CephString ",
            "desc": "Remove an NFS service",
            "perm": "rw"
        },
        {
            'cmd': "orchestrator service "
                   "name=action,type=CephChoices,"
                   "strings=start|stop|reload "
                   "name=svc_type,type=CephString "
                   "name=svc_name,type=CephString",
            "desc": "Start, stop or reload an entire service (i.e. all daemons)",
            "perm": "rw"
        },
        {
            'cmd': "orchestrator service-instance "
                   "name=action,type=CephChoices,"
                   "strings=start|stop|reload "
                   "name=svc_type,type=CephString "
                   "name=svc_id,type=CephString",
            "desc": "Start, stop or reload a specific service instance",
            "perm": "rw"
        },
        {
            'cmd': "orchestrator set backend "
                   "name=module,type=CephString,req=true",
            "desc": "Select orchestrator module backend",
            "perm": "rw"
        },
        {
            "cmd": "orchestrator status",
            "desc": "Report configured backend and its status",
            "perm": "r"
        },
        {
            'cmd': "orchestrator host add "
                   "name=host,type=CephString,req=true "
                   "name=labels,type=CephString,n=N,req=false",
            "desc": "Add a host",
            "perm": "rw"
        },
        {
            'cmd': "orchestrator host rm "
                   "name=host,type=CephString,req=true",
            "desc": "Remove a host",
            "perm": "rw"
        },
        {
            'cmd': "orchestrator host ls",
            "desc": "List hosts",
            "perm": "r"
        },
        {
            'cmd': "orchestrator mgr update "
                   "name=num,type=CephInt,req=true "
                   "name=hosts,type=CephString,n=N,req=false",
            "desc": "Update the number of manager instances",
            "perm": "rw"
        },
        {
            'cmd': "orchestrator mon update "
                   "name=num,type=CephInt,req=true "
                   "name=hosts,type=CephString,n=N,req=false",
            "desc": "Update the number of monitor instances",
            "perm": "rw"
        },
    ]

    def _select_orchestrator(self):
        o = self.get_module_option("orchestrator")
        if o is None:
            raise NoOrchestrator()

        return o

    def _add_host(self, cmd):
        host = cmd["host"]
        labels = cmd.get("labels", [])
        completion = self.add_host(host, labels)
        self._orchestrator_wait([completion])
        result = "\n".join(map(lambda r: str(r), completion.result))
        return HandleCommandResult(stdout=result)

    def _remove_host(self, cmd):
        host = cmd["host"]
        completion = self.remove_host(host)
        self._orchestrator_wait([completion])
        result = "\n".join(map(lambda r: str(r), completion.result))
        return HandleCommandResult(stdout=result)

    def _get_hosts(self):
        completion = self.get_hosts()
        self._orchestrator_wait([completion])
        result = "\n".join(map(lambda node: node.name, completion.result))
        return HandleCommandResult(stdout=result)

    def _list_devices(self, cmd):
        """
        This (all lines starting with ">") is how it is supposed to work. As of
        now, it's not yet implemented:
        > :returns: Either JSON:
        >     [
        >       {
        >         "name": "sda",
        >         "host": "foo",
        >         ... lots of stuff from ceph-volume ...
        >         "stamp": when this state was refreshed
        >       },
        >     ]
        >
        > or human readable:
        >
        >     HOST  DEV  SIZE  DEVID(vendor\\_model\\_serial)   IN-USE  TIMESTAMP
        >
        > Note: needs ceph-volume on the host.

        Note: this does not have to be completely synchronous. Slightly out of
        date hardware inventory is fine as long as hardware ultimately appears
        in the output of this command.
        """
        host = cmd.get('host', None)

        if host:
            nf = orchestrator.InventoryFilter()
            nf.nodes = [host]
        else:
            nf = None

        completion = self.get_inventory(node_filter=nf)

        self._orchestrator_wait([completion])

        if cmd.get('format', 'plain') == 'json':
            data = [n.to_json() for n in completion.result]
            return HandleCommandResult(stdout=json.dumps(data))
        else:
            # Return a human readable version
            result = ""
            for inventory_node in completion.result:
                result += "{0}:\n".format(inventory_node.name)
                for d in inventory_node.devices:
                    result += "  {0} ({1}, {2}b)\n".format(
                        d.id, d.type, d.size)
                result += "\n"

            return HandleCommandResult(stdout=result)

    def _list_services(self, cmd):
        hostname = cmd.get('host', None)
        svc_id = cmd.get('svc_id', None)
        svc_type = cmd.get('svc_type', None)
        # XXX this is kind of confusing for people because in the orchestrator
        # context the service ID for MDS is the filesystem ID, not the daemon ID

        completion = self.describe_service(svc_type, svc_id, hostname)
        self._orchestrator_wait([completion])
        services = completion.result

        # Sort the list for display
        services.sort(key=lambda s: (s.service_type, s.nodename, s.service_instance))

        if len(services) == 0:
            return HandleCommandResult(stdout="No services reported")
        elif cmd.get('format', 'plain') == 'json':
            data = [s.to_json() for s in services]
            return HandleCommandResult(stdout=json.dumps(data))
        else:
            lines = []
            for s in services:
                if s.service == None:
                    service_id = s.service_instance
                else:
                    service_id = "{0}.{1}".format(s.service, s.service_instance)

                lines.append("{0} {1} {2} {3} {4} {5}".format(
                    s.service_type,
                    service_id,
                    s.nodename,
                    s.container_id,
                    s.version,
                    s.rados_config_location))

            return HandleCommandResult(stdout="\n".join(lines))

    def _create_osd(self, inbuf, cmd):
        # type: (str, Dict[str, str]) -> HandleCommandResult
        """Create one or more OSDs"""

        usage = """
Usage:
  ceph orchestrator osd create -i <json_file>
  ceph orchestrator osd create host:device1,device2,...
"""

        if inbuf:
            try:
                drive_group = orchestrator.DriveGroupSpec.from_json(json.loads(inbuf))
            except ValueError as e:
                msg = 'Failed to read JSON input: {}'.format(str(e)) + usage
                return HandleCommandResult(-errno.EINVAL, stderr=msg)

        else:
            try:
                node_name, block_device = cmd['svc_arg'].split(":")
                block_devices = block_device.split(',')
            except (TypeError, KeyError, ValueError):
                msg = "Invalid host:device spec: '{}'".format(cmd['svc_arg']) + usage
                return HandleCommandResult(-errno.EINVAL, stderr=msg)

            devs = orchestrator.DeviceSelection(paths=block_devices)
            drive_group = orchestrator.DriveGroupSpec(node_name, data_devices=devs)

        # TODO: Remove this and make the orchestrator composable
        #   Like a future or so.
        host_completion = self.get_hosts()
        self._orchestrator_wait([host_completion])
        all_hosts = [h.name for h in host_completion.result]

        try:
            drive_group.validate(all_hosts)
        except orchestrator.DriveGroupValidationError as e:
            return HandleCommandResult(-errno.EINVAL, stderr=str(e))

        completion = self.create_osds(drive_group, all_hosts)
        self._orchestrator_wait([completion])
        self.log.warning(str(completion.result))
        return HandleCommandResult(stdout=str(completion.result))

    def _osd_rm(self, cmd):
        """
        Remove OSD's
        :cmd : Arguments for remove the osd
        """
        completion = self.remove_osds(cmd["svc_id"])
        self._orchestrator_wait([completion])
        return HandleCommandResult(stdout=str(completion.result))

    def _add_stateless_svc(self, svc_type, spec):
        completion = self.add_stateless_service(svc_type, spec)
        self._orchestrator_wait([completion])
        return HandleCommandResult()

    def _mds_add(self, cmd):
        spec = orchestrator.StatelessServiceSpec()
        spec.name = cmd['svc_arg']
        return self._add_stateless_svc("mds", spec)

    def _rgw_add(self, cmd):
        spec = orchestrator.StatelessServiceSpec()
        spec.name = cmd['svc_arg']
        return self._add_stateless_svc("rgw", spec)

    def _nfs_add(self, cmd):
        cluster_name = cmd['svc_arg']
        pool = cmd['pool']
        ns = cmd.get('namespace', None)

        spec = orchestrator.StatelessServiceSpec()
        spec.name = cluster_name
        spec.extended = { "pool":pool }
        if ns != None:
            spec.extended["namespace"] = ns
        return self._add_stateless_svc("nfs", spec)

    def _rm_stateless_svc(self, svc_type, svc_id):
        completion = self.remove_stateless_service(svc_type, svc_id)
        self._orchestrator_wait([completion])
        return HandleCommandResult()

    def _mds_rm(self, cmd):
        return self._rm_stateless_svc("mds", cmd['svc_id'])

    def _rgw_rm(self, cmd):
        return self._rm_stateless_svc("rgw", cmd['svc_id'])

    def _nfs_rm(self, cmd):
        return self._rm_stateless_svc("nfs", cmd['svc_id'])

    def _service_action(self, cmd):
        action = cmd['action']
        svc_type = cmd['svc_type']
        svc_name = cmd['svc_name']

        completion = self.service_action(action, svc_type, service_name=svc_name)
        self._orchestrator_wait([completion])

        return HandleCommandResult()

    def _service_instance_action(self, cmd):
        action = cmd['action']
        svc_type = cmd['svc_type']
        svc_id = cmd['svc_id']

        completion = self.service_action(action, svc_type, service_id=svc_id)
        self._orchestrator_wait([completion])

        return HandleCommandResult()

    def _update_mgrs(self, cmd):
        num = cmd["num"]
        hosts = cmd.get("hosts", [])

        if num <= 0:
            return HandleCommandResult(-errno.EINVAL,
                    stderr="Invalid number of mgrs: require {} > 0".format(num))

        completion = self.update_mgrs(num, hosts)
        self._orchestrator_wait([completion])
        result = "\n".join(map(lambda r: str(r), completion.result))
        return HandleCommandResult(stdout=result)

    def _update_mons(self, cmd):
        num = cmd["num"]
        hosts = cmd.get("hosts", [])

        if num <= 0:
            return HandleCommandResult(-errno.EINVAL,
                    stderr="Invalid number of mons: require {} > 0".format(num))

        def split_host(host):
            """Split host into host and network parts"""
            # TODO: stricter validation
            parts = host.split(":")
            if len(parts) == 1:
                return (parts[0], None)
            elif len(parts) == 2:
                return (parts[0], parts[1])
            else:
                raise RuntimeError("Invalid host specification: "
                        "'{}'".format(host))

        if hosts:
            try:
                hosts = list(map(split_host, hosts))
            except Exception as e:
                msg = "Failed to parse host list: '{}': {}".format(hosts, e)
                return HandleCommandResult(-errno.EINVAL, stderr=msg)

        completion = self.update_mons(num, hosts)
        self._orchestrator_wait([completion])
        result = "\n".join(map(lambda r: str(r), completion.result))
        return HandleCommandResult(stdout=result)

    def _set_backend(self, cmd):
        """
        We implement a setter command instead of just having the user
        modify the setting directly, so that we can validate they're setting
        it to a module that really exists and is enabled.

        There isn't a mechanism for ensuring they don't *disable* the module
        later, but this is better than nothing.
        """

        mgr_map = self.get("mgr_map")
        module_name = cmd['module']

        if module_name == "":
            self.set_module_option("orchestrator", None)
            return HandleCommandResult()

        for module in mgr_map['available_modules']:
            if module['name'] != module_name:
                continue

            if not module['can_run']:
                continue

            enabled = module['name'] in mgr_map['modules']
            if not enabled:
                return HandleCommandResult(-errno.EINVAL,
                                           stderr="Module '{module_name}' is not enabled. \n Run "
                                                  "`ceph mgr module enable {module_name}` "
                                                  "to enable.".format(module_name=module_name))

            try:
                is_orchestrator = self.remote(module_name,
                                              "is_orchestrator_module")
            except NameError:
                is_orchestrator = False

            if not is_orchestrator:
                return HandleCommandResult(-errno.EINVAL,
                                           stderr="'{0}' is not an orchestrator module".format(module_name))

            self.set_module_option("orchestrator", module_name)

            return HandleCommandResult()

        return HandleCommandResult(-errno.EINVAL, stderr="Module '{0}' not found".format(module_name))

    def _status(self):
        try:
            avail, why = self.available()
        except NoOrchestrator:
            return HandleCommandResult(-errno.ENODEV,
                                       stderr="No orchestrator configured (try "
                                       "`ceph orchestrator set backend`)")

        if avail is None:
            # The module does not report its availability
            return HandleCommandResult(stdout="Backend: {0}".format(self._select_orchestrator()))
        else:
            return HandleCommandResult(stdout="Backend: {0}\nAvailable: {1}{2}".format(
                                           self._select_orchestrator(),
                                           avail,
                                           " ({0})".format(why) if not avail else ""
                                       ))

    def handle_command(self, inbuf, cmd):
        try:
            return self._handle_command(inbuf, cmd)
        except NoOrchestrator:
            return HandleCommandResult(-errno.ENODEV, stderr="No orchestrator configured")
        except ImportError as e:
            return HandleCommandResult(-errno.ENOENT, stderr=str(e))
        except NotImplementedError:
            return HandleCommandResult(-errno.EINVAL, stderr="Command not found")

    def _handle_command(self, inbuf, cmd):
        if cmd['prefix'] == "orchestrator device ls":
            return self._list_devices(cmd)
        elif cmd['prefix'] == "orchestrator service ls":
            return self._list_services(cmd)
        elif cmd['prefix'] == "orchestrator service status":
            return self._list_services(cmd)  # TODO: create more detailed output
        elif cmd['prefix'] == "orchestrator osd rm":
            return self._osd_rm(cmd)
        elif cmd['prefix'] == "orchestrator mds add":
            return self._mds_add(cmd)
        elif cmd['prefix'] == "orchestrator mds rm":
            return self._mds_rm(cmd)
        elif cmd['prefix'] == "orchestrator rgw add":
            return self._rgw_add(cmd)
        elif cmd['prefix'] == "orchestrator rgw rm":
            return self._rgw_rm(cmd)
        elif cmd['prefix'] == "orchestrator nfs add":
            return self._nfs_add(cmd)
        elif cmd['prefix'] == "orchestrator nfs rm":
            return self._nfs_rm(cmd)
        elif cmd['prefix'] == "orchestrator service":
            return self._service_action(cmd)
        elif cmd['prefix'] == "orchestrator service-instance":
            return self._service_instance_action(cmd)
        elif cmd['prefix'] == "orchestrator set backend":
            return self._set_backend(cmd)
        elif cmd['prefix'] == "orchestrator status":
            return self._status()
        elif cmd['prefix'] == "orchestrator osd create":
            return self._create_osd(inbuf, cmd)
        elif cmd['prefix'] == "orchestrator osd remove":
            return self._remove_osd(cmd)
        elif cmd['prefix'] == "orchestrator host add":
            return self._add_host(cmd)
        elif cmd['prefix'] == "orchestrator host rm":
            return self._remove_host(cmd)
        elif cmd['prefix'] == "orchestrator host ls":
            return self._get_hosts()
        elif cmd['prefix'] == "orchestrator mgr update":
            return self._update_mgrs(cmd)
        elif cmd['prefix'] == "orchestrator mon update":
            return self._update_mons(cmd)
        else:
            raise NotImplementedError()
