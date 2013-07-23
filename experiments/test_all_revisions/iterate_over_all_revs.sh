#!/bin/bash
# parallel_runner.sh ---
#
# Filename: parallel_runner.sh
# Description:
# Author: Elric Milon
# Maintainer:
# Created: Thu Jul 11 14:51:05 2013 (+0200)
# Version:

# Commentary:
#
#
#
#

# Change Log:
#
#
#
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 3, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; see the file COPYING.  If not, write to
# the Free Software Foundation, Inc., 51 Franklin Street, Fifth
# Floor, Boston, MA 02110-1301, USA.
#
#

# Code:

set -ex

rm -f /tmp/results.log

if [ ! -d dispersy ]; then
    git clone https://github.com/Tribler/dispersy.git
fi

cd dispersy
git clean -fd
git checkout devel
cd ..

export PYTHONPATH=.
export TESTNAME="Whatever"
mkdir -p output
export OUTPUTDIR=$(readlink -f output)
CONFFILE=$(readlink -f "test.conf")

# Do only one iteration by default
if [ -z "$STAP_RUN_ITERATIONS" ]; then
    STAP_RUN_ITERATIONS=1
fi

ITERATION=0
COUNT=0

for REV in $(git log --quiet d1dbf7e..HEAD | grep ^"commit " | cut -f2 -d" "); do
    let COUNT=1+$COUNT

    git checkout $REV
    git submodule sync
    git submodule update
    export REVISION=$REV
    while [ $ITERATION -lt $STAP_RUN_ITERATIONS ]; do
        let ITERATION=1+$ITERATION

        rm -fR sqlite

        run_stap_probe.sh "nosetests dispersy/tests/test_sync.py" $OUTPUTDIR/${TESTNAME}_${COUNT}_${REVISION}_${ITERATION}.csv

        echo $? $REV >> /tmp/results.log
    done
    git clean -fd
done

#
# parallel_runner.sh ends here