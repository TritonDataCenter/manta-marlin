# Hyprlofs Bindings

Hyprlofs is a SmartOS in-memory filesystem that allows consumers to map files
from many disparate locations under one namespace (the hyprlofs mount).  See
hyprlofs(7fs) for details.

This module allows consumers to create and tear down hyprlofs mounts and modify
the mappings associated with a hyprlofs mount.  This interface closely
resembles the underlying kernel interface without attempting to provide a
higher-level abstraction.  The main difference between this interface and the
kernel interface is that this module's operations are asynchronous, but only
one may be outstanding for a given object at any given time.


## Synopsis

    var mod_hyprlofs = require('hyprlofs');
    
    var fs = new mod_hyprlofs.Filesystem('/export/mymount');
    
    fs.mount(function (err) {
            fs.addMappings([
                [ '/etc/ssh/sshd_config', 'ssh_config' ],
                [ '/etc/release', 'release' ],
                /* ... */
            ], function (err) {
                    if (err)
                            throw (err);
    
                    console.log('setup complete.');
            });
    });
    
    /* ... */
    
    fs.unmount(function (err) {
            if (err)
                    throw (err);

            console.log('unmount complete');
    });


## Interface

This module exports a `Filesystem` object on which all management operations
are performed, including:

* mount (always initially empty)
* unmount
* add files
* remove specific files
* remove all files

All operations are asynchronous.  They take a "callback" argument that will be
invoked upon completion with an optional "error" argument indicating whether
the operation failed.

`Filesystem` objects are stateless: they're essentially just a handle to work
with a given hyprlofs mount, which is identified by the mountpoint.  These
objects do not keep track of the underlying mount state at all.  You can even
have more than one object managing a single hyprlofs mount.  This is not
recommended, since concurrently-dispatched operations will be processed in an
undefined order.

All operations other than "mount" require that the underlying hyprlofs
filesystem be mounted, though not necessarily via this interface.  For the most
part, this is enforced by the kernel interface, which will fail operations that
are attempted on non-hyprlofs mounts.  But see the special note on "unmount"
below.

### `new Filesystem(mountpoint)`: work with a hyprlofs filesystem

The constructor creates a handle for issuing subsequent hyprlofs requests.  This
does *not* mount the filesystem or create any mappings, but this call will fail
if the given mountpoint does not already exist.

### `fs.mount(callback)`: mount a hyprlofs filesystem

Mounts a new **read-only** hyprlofs filesystem at this object's mountpoint.  The
filesystem initially contains no mappings.

### `fs.unmount(callback)`: unmount a hyprlofs filesystem

Unmounts the filesystem at this object's mountpoint.  **This will unmount any
filesystem mounted here, regardless of whether it's a hyprlofs mount.**  It is
the caller's responsibility to ensure that this is the right thing.

### `fs.addMappings(mappings, callback)`: add a set of file mappings

Adds the specified mappings to the underlying hyprlofs filesystem.  `mappings`
is an array of mappings.  Each mapping is itself an array with two entries: the
full path to the file to be added, and the alias (the relative path where it
should appear) under the hyprlofs mount.  See the example above for details.

If the underlying mountpoint is not a mounted hyprlofs filesystem, this call
will fail.

### `fs.removeMappings(filenames, callback)`: remove a set of file mappings

Removes the specified mappings from the underlying hyprlofs filesystem.
`filenames` is an array of aliases (relative paths) under the hyprlofs mount.

If the underlying mountpoint is not a mounted hyprlofs filesystem, this call
will fail.

### `fs.removeAll(callback)`: removes all mappings

Removes all mappings from the underlying hyprlofs filesystem.  This is useful
for resetting the underlying filesystem without having to iterate the existing
files (and without the associated race condition).

If the underlying mountpoint is not a mounted hyprlofs filesystem, this call
will fail.

### `fs.listMappings(callback)`: lists all mappings

Returns (via callback) the list of all mappings on the given mount, in the same
format as would be passed to addMappings.

If the underlying mountpoint is not a mounted hyprlofs filesystem, this call
will fail.
