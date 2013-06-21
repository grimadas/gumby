#/bin/bash
# setup_env.sh ---
#
# Filename: setup_env.sh
# Description:
# Author: Elric Milon
# Maintainer:
# Created: Wed May 22 19:18:49 2013 (+0200)

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

VENV=$HOME/venv

if [ -e $VENV/.completed ]; then
    echo "The virtualenv has been successfully built in a previous run of the script."
    echo "If you want to rebuild it or the script has been updated, either delete $VENV/.completed"
    echo "or the full $VENV dir and re-run the script."
    exit 0
fi

if [ -d $VENV ]; then
    virtualenv --no-site-packages --clear $VENV
fi

mkdir -p $VENV/src

source $VENV/bin/activate

#hack for m2crypto to build properly in RH/fedora
if [ ! -e $VENV/lib/libcrypto.so ]; then
    pushd $VENV/src
    wget https://www.openssl.org/source/openssl-1.0.1e.tar.gz
    tar xvzpf openssl*tar.gz
    pushd openssl-*/

    ./config --prefix=$VENV threads zlib shared  --openssldir=$VENV/share/openssl
    #make -j$(grep processor /proc/cpuinfo | wc -l) #Fails when building in multithreaded mode
    make
    make install
    echo "Done"
    popd
    popd
fi

pip install m2crypto || (
    pushd $VENV/build/m2crypto
    python setup.py build_py
    python setup.py build_ext --openssl=$VENV
    #python setup.py build # Do not run this, it will break the proper stuff made by build_ext
    python setup.py install
    popd
)


# Install apsw manually as it is not available trough pip.
if [ ! -e $VENV/lib64/python2.6/site-packages/apsw.so ]; then
    pushd $VENV/src
    if [ ! -e apsw-*zip ]; then
        wget https://apsw.googlecode.com/files/apsw-3.7.16.2-r1.zip
    fi
    if [ ! -d apsw*/src ]; then
        unzip apsw*.zip
    fi
    cd apsw*/
    python setup.py fetch --missing-checksum-ok --all build --enable-all-extensions install # test # running the tests makes it segfault...
fi

# TODO: Fix this mess properly
export LD_LIBRARY_PATH=$VENV/lib:$LD_LIBRARY_PATH
export LD_RUN_PATH=$VENV/lib:$LD_RUN_PATH
export LD_PRELOAD=$VENV/lib/libcrypto.so
echo "Testing if the EC stuff is working..."
python -c "from M2Crypto import EC; print dir(EC)"
popd

#Not sure if we need this:
#pushd build-tmp
#wget http://download.zeromq.org/zeromq-3.2.3.tar.gz
#tar xvzpf zeromq*tar.gz
#cd zeromq*/
#./configure --prefix=$VIRTUAL_ENV
#make -j$(grep processor /proc/cpuinfo | wc -l)
#popd


# Build libboost
pushd $VENV/src
if [ ! -e $VENV/lib/libboost_wserialization.so ]; then
    wget http://netcologne.dl.sourceforge.net/project/boost/boost/1.53.0/boost_1_53_0.tar.bz2
    tar xavf boost_*.tar.bz2
    cd boost*/
    ./bootstrap.sh
    ./b2 -j$(grep process /proc/cpuinfo | wc -l) --prefix=$VENV install
fi
popd


# Before continuing fix a stupid symlink bug
#cd $VENV
#rm lib64
#ln -s lib lib64
#cd

# Build Libtorrent and its python bindings
pushd $VENV/src
if [ ! -e $VENV/lib/pkgconfig/libtorrent-rasterbar.pc ]; then
    wget --no-check-certificate https://libtorrent.googlecode.com/files/libtorrent-rasterbar-0.16.10.tar.gz
    tar xavf libtorrent-rasterbar-*.tar.gz
    cd libtorrent-rasterbar*/
    ./configure --with-boost-python --with-boost=$VENV/include/boost --with-boost-libdir=$VENV/lib --with-boost-system=boost_system --prefix=$VENV --enable-python-binding
    make -j$(grep process /proc/cpuinfo | wc -l) || make
    make install
    cd $VENV/lib
    ln -s libboost_python.so libboost_python-py27.so.1.53.0
    cd ../..
fi

echo "
ipython
ntplib
gmpy==1.16
pyzmq
twisted
pysqlite
" > ~/requeriments.txt
pip install -r ~/requeriments.txt
rm ~/requeriments.txt

deactivate

virtualenv --relocatable $VENV

unset LD_PRELOAD
#rm -fR venv
#mv $VENV $VENV/../venv
rm -fR build-tmp
touch $VENV/.completed

echo "Done, you can use this virtualenv with:
	source venv/bin/activate
And exit from it with:
	activate
Enjoy."


#
# setup_env.sh ends here
