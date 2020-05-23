#!/usr/bin/env bash
gumby/experiments/wikipedia/post_process_wiki.py .

# Invoke the IPv8 experiment process which will also plot our Noodle statistics
post_process_ipv8_experiment.sh
