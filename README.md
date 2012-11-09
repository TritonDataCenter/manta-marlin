# Marlin: Manta Compute Engine

Repository: <git@git.joyent.com:marlin.git>
Browsing: <https://mo.joyent.com/marlin>
Who: Dave Pacheco
Docs: <https://mo.joyent.com/docs/marlin>
Tickets/bugs: <https://devhub.joyent.com/jira/browse/MANTA>


# Overview

For an overview and details on Marlin, see docs/index.restdown (or
https://mo.joyent.com/docs/marlin).


# Development

First, set up a working Manta install.  You'll need the nameserver IP addresses
as well as the names of the Moray, Mahi, and Muppet zones (usually something
like {1.moray,auth,manta}.coal.joyent.us).  The rest of these steps assume
you've already set up the node-manta.git clients and have tools like "mls" and
"msign" in your path.

The suggested Marlin development environment is a compute zone as set up by
ca-headnode-setup, available in the global zone at
/opt/smartdc/agents/lib/node\_modules/cabase/tools/ca-headnode-setup.  This is
basically a smartos zone with a few particular packages, including
gcc-compiler, cscope, gmake, scmgit, and python26.

In your new zone, add the Manta nameservers to /etc/resolv.conf.  Check this
using "dig manta.coal.joyent.us" (or whatever the manta front door is in your
setup).

Now you're ready to check out the source:

    $ git clone git@git.joyent.com:marlin.git
    $ cd marlin

Source the environment file to get useful tools on your path (including the
marlin and moray tools):

    $ . env.sh

Then build the source:

    $ make

For good measure, run "make check":

    $ make check
    ...
    check ok


# Tools

The main development tool is "mrjob", which allows you to create, update, and
fetch detailed status about Marlin jobs by talking to Moray directly.

To create a job, you must first create an auth token against Manta.  The
easiest way to do this is to sign the 'POST /$user/tokens' URL:

    $ msign -m POST /dap/tokens
    https://manta.coal.joyent.us/dap/tokens?...

Then use the URL you were given with curl to create a new token:

    $ curl -k -XPOST 'https://manta.coal.joyent.us/dap/tokens?...'
    {"token":"..."}

Copy this token, and pass that to "mrjob submit" using the "-t" option.  You'll
also want to set USER to the uuid of your user.  (Sorry, this part sucks, but
mrjob doesn't know about mahi yet.)

    $ jobid=$(USER=50c7f736-9d2c-40cd-80b9-8138c003b35d mrjob submit -t '...' -m wc); echo $jobid
    8abafb0d-9e9c-4023-b82b-73c4b52559f2

For details on using "mrjob" to submit more complex jobs, add keys to jobs,
fetch detailed jobs, and so on, run "mrjob --help".


# Test suite

The Marlin test suite is not nearly comprehensive, but it exercises a good
portion of the stack.  **The test suite should always be totally clean.
Tickets should be fileld on any failures not due to local changes or
misconfiguration.**

The test environment assumes several environment variables are set.  Here are
example settings for COAL:

    $ export MAHI_URL=tcp://auth.coal.joyent.us:6379/
    $ export MORAY_URL=tcp://1.moray.coal.joyent.us:2020/
    $ export MANTA_URL=https://manta.coal.joyent.us
    $ export MANTA_USER=poseidon
    $ export MANTA_KEY_ID=ff:4a:ad:ac:92:86:ec:d2:5a:9d:88:c8:e8:12:8d:15

Change "coal.joyent.us" as appropriate for your setup.  MANTA\_KEY\_ID is also
specific to each installation.  To find it, run this in headnode global zone:

    # sdc-ldap search -b "$(sdc-ldap search login=poseidon | \
          head -1 | cut -c5-)" objectclass=sdckey | \
          awk '/^fingerprint:/{ print $2 }'
    ff:4a:ad:ac:92:86:ec:d2:5a:9d:88:c8:e8:12:8d:15

You must also copy the ssh key of the "poseidon" user to
"$HOME/.ssh/id\_rsa{,.pub}".  You can get this key from the "ops" Manta zone.
If your dev zone is on the same box as the ops zone, you can do this with:

    # cp /zones/$(vmadm lookup alias=~ops)/root/root/.ssh/id_rsa* \
         /zones/$YOUR_DEV_ZONE_UUID/root/$YOUR_LOGIN_NAME/.ssh

Finally, to actually run the tests:

    $ tools/catest -a


# Before pushing changes

- All code should be "make check" clean.
- Test suite should be clean (see above).
- Code review is suggested.
