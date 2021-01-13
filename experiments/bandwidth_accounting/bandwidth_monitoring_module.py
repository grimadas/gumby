import os
import signal
import subprocess

from gumby.experiment import experiment_callback
from gumby.modules.experiment_module import ExperimentModule, static_module


@static_module
class TrafficMonitor(ExperimentModule):

    def __init__(self, experiment):
        super().__init__(experiment)
        self.run_process = None

        self.num_validators = int(os.environ["NUM_VALIDATORS"])
        self.num_clients = int(os.environ["NUM_CLIENTS"])

    def is_client(self):
        return self.my_id > self.num_validators

    def is_responsible_validator(self):
        """
        Return whether this validator is the responsible validator to setup/init databases on this machine.
        This can only be conducted by a single process.
        """
        if self.is_client():
            return False

        my_host, _ = self.experiment.get_peer_ip_port_by_id(self.my_id)

        is_responsible = True
        for peer_id in self.experiment.all_vars.keys():
            if self.experiment.all_vars[peer_id]['host'] == my_host and int(peer_id) < self.my_id:
                is_responsible = False
                break

        return is_responsible

    @experiment_callback
    def start_traffic_monitor(self):
        if self.is_responsible_validator():
            self._logger.info("Starting bandwidth monitoring")
            path = os.path.join(os.environ.get('PROJECT_DIR'),
                                'experiments',
                                'bandwidth_accounting',
                                'nethogs_monitor.py')
            cmd = 'sudo python3 -u %s > monitor.log 2>&1' % str(path)

            print(cmd)
            self.run_process = subprocess.Popen([cmd], shell=True, preexec_fn=os.setpgrp)

    @experiment_callback
    def stop_traffic_monitor(self):
        if self.run_process:
            self._logger.info("Stopping bandwidth monitoring")
            # Get the process id
            pgid = os.getpgid(self.run_process.pid)
            print(pgid)
            print(self.run_process.pid)
            subprocess.check_call(['sudo', 'pkill', '-f', 'nethogs_monitor.py'])
