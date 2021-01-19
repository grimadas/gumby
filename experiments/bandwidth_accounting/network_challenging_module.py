import os

from gumby.experiment import experiment_callback
from gumby.modules.experiment_module import ExperimentModule, static_module


@static_module
class LatencyChallenger(ExperimentModule):

    def __init__(self, experiment):
        super().__init__(experiment)
        self.run_process = None

        self.num_validators = int(os.environ["NUM_VALIDATORS"])
        self.num_clients = int(os.environ["NUM_CLIENTS"])

        self.counter = None

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
    def add_latency(self):
        vars = os.getenv('LATENCY')
        if not vars:
            k = int(200)
        else:
            k = int(vars)
        cmd = "sudo tc qdisc add dev eno5 root netem delay {}ms".format(k)
        if self.is_responsible_validator():
            os.system(cmd)

    @experiment_callback
    def remove_latency(self):
        if self.is_responsible_validator():
            cmd = "sudo tc qdisc del dev eno5 root netem delay 0ms"
            os.system(cmd)
