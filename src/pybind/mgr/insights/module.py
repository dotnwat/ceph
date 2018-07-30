import datetime
import json
import re
import threading
from collections import defaultdict
import six
from mgr_module import MgrModule

# freq to write cached state to disk
PERSIST_PERIOD = datetime.timedelta(seconds = 10)
# hours of crash history to report
CRASH_HISTORY_HOURS = 24
# hours of health history to report
HEALTH_HISTORY_HOURS = 24
# health history retention (pruned every hour)
HEALTH_RETENTION_HOURS = 30
# health check name for inter-module errors (crash module)
HEALTH_CRASHES_MISSING = "MGR_INSIGHTS_MISSING_CRASH_INFO"
# version tag for persistent data format
ON_DISK_VERSION = 1
# on disk key prefix
HEALTH_HISTORY_KEY_PREFIX = "health_history/"

class HealthEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

class HealthCheckAccumulator(object):
    """
    Deuplicated storage of health checks.
    """
    def __init__(self, init_checks = None):
        # check : severity : { summary, detail }
        # summary and detail are deduplicated
        self._checks = defaultdict(lambda:
            defaultdict(lambda: {
                "summary": set(),
                "detail": set()
            }))

        if init_checks:
            self._update(init_checks)

    def __str__(self):
        return "check count {}".format(len(self._checks))

    def add(self, checks):
        """
        Add health checks to the current state

        Returns:
            bool: True if the state changed, False otherwise.
        """
        changed = False

        for check, info in checks.iteritems():

            # only keep the icky stuff
            severity = info["severity"]
            if severity == "HEALTH_OK":
                continue

            summary = info["summary"]["message"]
            details = map(lambda d: d["message"], info["detail"])

            if self._add_check(check, severity, [summary], details):
                changed = True

        return changed

    def checks(self):
        return self._checks

    def merge(self, other):
        assert isinstance(other, HealthCheckAccumulator)
        self._update(other._checks)

    def _update(self, checks):
        """Merge checks with same structure. Does not set dirty bit"""
        for check in checks:
            for severity in checks[check]:
                summaries = set(checks[check][severity]["summary"])
                details = set(checks[check][severity]["detail"])
                self._add_check(check, severity, summaries, details)

    def _add_check(self, check, severity, summaries, details):
        changed = False

        for summary in summaries:
            if summary not in self._checks[check][severity]["summary"]:
                changed = True
                self._checks[check][severity]["summary"].add(summary)

        for detail in details:
            if detail not in self._checks[check][severity]["detail"]:
                changed = True
                self._checks[check][severity]["detail"].add(detail)

        return changed

class HealthHistorySlot(object):
    """
    Manage the life cycle of a health history time slot.

    A time slot is a fixed slice of wall clock time (e.g. every hours, from :00
    to :59), and all health updates that occur during this time are deduplicated
    together. A slot is initially in a clean state, and becomes dirty when a new
    health check is observed. The state of a slot should be persisted when
    need_flush returns true. Once the state has been flushed, reset the dirty
    bit by calling mark_flushed.
    """
    def __init__(self, init_health = dict()):
        self._checks = HealthCheckAccumulator(init_health.get("checks"))
        self._slot = self._curr_slot()
        self._next_flush = None

    def __str__(self):
        return "key {} next flush {} checks {}".format(
            self.key(), self._next_flush, self._checks)

    def health(self):
        return dict(checks = self._checks.checks())

    def key(self):
        """Identifer in the persist store"""
        return self._key(self._slot)

    def expired(self):
        """True if this slot is the current slot, False otherwise"""
        return self._slot != self._curr_slot()

    def need_flush(self):
        """True if this slot needs to be flushed, False otherwise"""
        now = datetime.datetime.utcnow()
        if self._next_flush is not None:
            if self._next_flush <= now or self.expired():
                return True
        return False

    def mark_flushed(self):
        """Reset the dirty bit. Caller persists state"""
        assert self._next_flush
        self._next_flush = None

    def add(self, health):
        """
        Add health to the underlying health accumulator. When the slot
        transitions from clean to dirty a target flush time is computed.
        """
        changed = self._checks.add(health["checks"])
        if changed and not self._next_flush:
            self._next_flush = datetime.datetime.utcnow() + PERSIST_PERIOD

    def merge(self, other):
        assert isinstance(other, HealthHistorySlot)
        self._checks.merge(other._checks)

    @staticmethod
    def key_range(hours):
        """Return the time slot keys for the past N hours"""
        def inner(curr, hours):
            slot = curr - datetime.timedelta(hours = hours)
            return HealthHistorySlot._key(slot)
        curr = HealthHistorySlot._curr_slot()
        return map(lambda i: inner(curr, i), range(hours))

    @staticmethod
    def curr_key():
        """Key for the current UTC time slot"""
        return HealthHistorySlot._key(HealthHistorySlot._curr_slot())

    @staticmethod
    def key_to_time(key):
        """Return key converted into datetime"""
        timestr = key[len(HEALTH_HISTORY_KEY_PREFIX):]
        return datetime.datetime.strptime(timestr, "%Y-%m-%d_%H")

    @staticmethod
    def _key(dt):
        """Key format. Example: health_2018_11_05_00"""
        return HEALTH_HISTORY_KEY_PREFIX + dt.strftime("%Y-%m-%d_%H")

    @staticmethod
    def _curr_slot():
        """Slot for the current UTC time"""
        dt = datetime.datetime.utcnow()
        return datetime.datetime(
            year  = dt.year,
            month = dt.month,
            day   = dt.day,
            hour  = dt.hour)

class Module(MgrModule):
    """

    Health History:
        Health checks are reported for the last N hours. Time is divided into
        hour slots, and when a health check update is observed it is deduped
        against the health checks observed in the current hour and kept in the
        persistent store (modulo the flush period). When a report is generated
        the last N hours of health checks are deduplicated. Old reports may be
        pruned on demand.
    """

    COMMANDS = [
        {
            "cmd": "insights",
            "desc": "get insights stuff",
            "perm": "r",
            "poll": "false",
        },
        {
            "cmd": "insights self-test",
            "desc": "get insights stuff",
            "perm": "rw",
            "poll": "false",
        },
        {
            "cmd": "insights clear name=id,type=CephString",
            "desc": "get insights stuff",
            "perm": "rw",
            "poll": "false",
        },
        {
            "cmd": "insights health name=id,type=CephString",
            "desc": "get insights stuff",
            "perm": "rw",
            "poll": "false",
        },
    ]


    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.health = {}

        self._shutdown = False
        self._evt = threading.Event()

        # health history tracking
        self._pending_health = []
        self._health_slot = None

    def do_self_test(self, inbuf, command):
        h = HealthCheckAccumulator()
        assert h.checks() == {}

        return 0, "", ""

    def do_health(self, inbuf, cmd):
        name = cmd['id']
        check = {
            "XXX_TEST_{}".format(name): {
                "severity": "warning",
                "summary": "summary_{}".format(name),
                "detail": [name, name]
            }
        }
        self.health.update(check)
        self.set_health_checks(self.health)
        return 0, "", ""

    def do_clear(self, inbuf, cmd):
        name = cmd['id']
        key = "XXX_TEST_{}".format(name)
        if name == "all":
            self.health = {}
        if key in self.health.keys():
            del self.health[key]
        self.set_health_checks(self.health)
        return 0, "", ""

    def notify(self, ttype, ident):
        """Queue updates for processing"""
        if ttype == "health":
            self.log.info("Received health check update {} pending".format(
                len(self._pending_health)))
            health = json.loads(self.get("health")["json"])
            self._pending_health.append(health)
            self._evt.set()

    def serve(self):
        self._health_reset()
        while True:
            self._evt.wait(PERSIST_PERIOD.total_seconds())
            self._evt.clear()
            if self._shutdown:
                break

            # when the current health slot expires, finalize it by flushing it to
            # the store, and initializing a new empty slot.
            if self._health_slot.expired():
                self.log.info("Health history slot expired {}".format(
                    self._health_slot))
                self._health_maybe_flush()
                self._health_reset()

            self._health_prune_history()

            # fold in pending health snapshots and flush
            self.log.info("Applying {} health updates to slot {}".format(
                len(self._pending_health), self._health_slot))
            for health in self._pending_health:
                self._health_slot.add(health)
            self._pending_health = []
            self._health_maybe_flush()

    def shutdown(self):
        self._shutdown = True
        self._evt.set()

    def _health_reset(self):
        """Initialize the current health slot

        The slot will be initialized with any state found to have already been
        persisted, otherwise the slot will start empty.
        """
        key = HealthHistorySlot.curr_key()
        data = self.get_store(key)
        if data:
            init_health = json.loads(data)
            self._health_slot = HealthHistorySlot(init_health)
        else:
            self._health_slot = HealthHistorySlot()
        self.log.info("Reset curr health slot {}".format(self._health_slot))

    def _health_maybe_flush(self):
        """Store the health for the current time slot if needed"""

        self.log.info("Maybe flushing slot {} needed {}".format(
            self._health_slot, self._health_slot.need_flush()))

        if self._health_slot.need_flush():
            key = self._health_slot.key()

            # build store data entry
            slot = self._health_slot.health()
            assert "version" not in slot
            slot.update(dict(version = ON_DISK_VERSION))
            data = json.dumps(slot, cls=HealthEncoder)

            self.log.debug("Storing health key {} data {}".format(
                key, json.dumps(slot, indent=2, cls=HealthEncoder)))

            self.set_store(key, data)
            self._health_slot.mark_flushed()

    def _health_filter(self, f):
        """Filter hourly health reports timestamp"""
        matches = filter(
            lambda (key, _): f(HealthHistorySlot.key_to_time(key)),
            six.iteritems(self.get_store_prefix(HEALTH_HISTORY_KEY_PREFIX)))
        return map(lambda (k, _): k, matches)

    def _health_prune_history(self):
        """Prune old health entries"""
        cutoff = datetime.datetime.utcnow() - \
                datetime.timedelta(hours = HEALTH_RETENTION_HOURS)
        for key, _ in self._health_filter(lambda ts: ts <= cutoff):
            self.log.info("Removing old health slot key {}".format(key))
            self.set_store(key, None)

    def _health_report(self, hours):
        """
        Report a consolidated health report for the past N hours.
        """
        # roll up the past N hours of health info
        collector = HealthHistorySlot()
        keys = HealthHistorySlot.key_range(hours)
        for key in keys:
            data = self.get_store(key)
            self.log.info("Reporting health key {} found {}".format(
                key, bool(data)))
            health = json.loads(data) if data else {}
            slot = HealthHistorySlot(health)
            collector.merge(slot)

        # fold in the current health
        curr_health = json.loads(self.get("health")["json"])
        curr_health_slot = HealthHistorySlot()
        curr_health_slot.add(curr_health)
        collector.merge(curr_health_slot)

        return dict(
           current = curr_health,
           history = collector.health()
        )

    def _version_parse(self, version):
        """
        Return the components of a Ceph version string.

        This returns nothing when the verison string cannot be parsed into its
        constituent components, such as when Ceph has been built with
        ENABLE_GIT_VERSION=OFF.
        """
        r = "ceph version (?P<release>\d+)\.(?P<major>\d+)\.(?P<minor>\d+)"
        m = re.match(r, version)
        ver = {} if not m else {
            "release": m.group("release"),
            "major": m.group("major"),
            "minor": m.group("minor")
        }
        return { k:int(v) for k,v in ver.iteritems() }

    def _crash_history(self, hours):
        """
        Load crash history for the past N hours from the crash module.
        
        Loads crash data from the crash module which is an always-on module, so
        we don't need to worry about it not being enabled: normal health checks
        will report any issue with loading and enabling the crash module which
        are echoed by the insights report generated by this module. Other errors
        occuring in the crash module are reported as a health check error, and
        report["crash_history"]["ok"] will be set to false because health check
        errors are not immediately visible.
        """
        params = dict(
            prefix = "crash json_report",
            hours = hours
        )

        result = dict(
            summary = None,
            hours = params["hours"]
        )

        try:
            _, _, crashes = self.remote("crash", "handle_command", "", params)
            result["summary"] = json.loads(crashes)
            health_checks = dict()
        except Exception as e:
            self.log.warning("Failed to invoke crash module {}".format(e))
            health_checks = {
                HEALTH_CRASHES_MISSING: {
                    "severity": "warning",
                    "summary": "failed to invoke ceph-mgr crash module",
                    "detail": [str(e)]
                }
            }

        return result, health_checks

    def do_report(self, inbuf, command):
        health = {}
        report = {}

        report.update({
            "version": dict(full = self.version,
                **self._version_parse(self.version))
        })

        # crash history
        history, health_update = self._crash_history(CRASH_HISTORY_HOURS)
        report["crashes"] = history
        health.update(health_update)

        # health history
        report["health"] = self._health_report(HEALTH_HISTORY_HOURS)

        #report["osd_dump"] = self.get("osd_map")
        #report["df"] = self.get("df")
        #report["osd_tree"] = self.get("osd_map_tree")
        #report["fs_map"] = self.get("fs_map")
        #report["crush_map"] = self.get("osd_map_crush")
        #report["mon_map"] = self.get("mon_map")
        #report["service_map"] = self.get("service_map")
        #report["manager_map"] = self.get("mgr_map")
        #report["mon_status"] = json.loads(self.get("mon_status")["json"])
        #report["pg_summary"] = self.get("pg_summary")
        #report["osd_metadata"] = self.get("osd_metadata")

        #self.set_health_checks(health)

        return 0, "", json.dumps(report, indent=2, cls=HealthEncoder)

    def handle_command(self, inbuf, command):
        if command["prefix"] == "insights":
            return self.do_report(inbuf, command)
        elif command["prefix"] == "insights self-test":
            return self.do_self_test(inbuf, command)
        elif command["prefix"] == "insights health":
            return self.do_health(inbuf, command)
        elif command["prefix"] == "insights clear":
            return self.do_clear(inbuf, command)
        else:
            raise NotImplementedError(cmd["prefix"])
