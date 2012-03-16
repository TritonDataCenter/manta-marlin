/*
 * hyprlofs.cc: Node.js bindings for SmartOS's hyprlofs filesystem.
 */

#include <v8.h>
#include <node.h>
#include <node_object_wrap.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mount.h>
#include <sys/fs/hyprlofs.h>

using namespace v8;
using namespace node;

/*
 * This flag controls whether to emit debug output to stderr whenever we make a
 * hyprlofs ioctl call.
 */
static bool hyfs_debug = false;

class HyprlofsFilesystem;

static const char *hyprlofs_cmdname(int);
static hyprlofs_entries_t *hyfs_entries_populate_add(const Local<Array>&);
static hyprlofs_entries_t *hyfs_entries_populate_remove(const Local<Array>&);
static void hyfs_entries_free(hyprlofs_entries_t *);

/*
 * The HyprlofsFilesystem is the nexus of administration.  Users construct an
 * instance of this object to operate on any hyprlofs mount, and then invoke
 * methods to add, remove, or clear mappings.  See README.md for details.
 */
class HyprlofsFilesystem : node::ObjectWrap {
public:
	static void Initialize(Handle<Object> target);

protected:
	HyprlofsFilesystem(const char *);
	~HyprlofsFilesystem();

	void asyncIoctl(int, hyprlofs_entries_t *, Local<Value>);
	void async(void (*)(eio_req *), Local<Value>);

	static int eioAsyncFini(eio_req *);
	static void eioIoctlRun(eio_req *);
	static void eioMountRun(eio_req *);
	static void eioUmountRun(eio_req *);

	static Handle<Value> New(const Arguments&);
	static Handle<Value> Mount(const Arguments&);
	static Handle<Value> Unmount(const Arguments&);
	static Handle<Value> AddMappings(const Arguments&);
	static Handle<Value> RemoveMappings(const Arguments& );
	static Handle<Value> RemoveAll(const Arguments&);

	int argsCheck(const char *, const Arguments&, int);

private:
	static Persistent<FunctionTemplate> hfs_templ;

	/* immutable state */
	int			hfs_fd;			/* mountpoint fd */
	char			hfs_label[PATH_MAX];	/* mountpoint path */

	/* common async operation state */
	bool 			hfs_pending;	/* operation outstanding */
	Persistent<Function>	hfs_callback;	/* user callback */
	int			hfs_rv;		/* current async rv */
	int			hfs_errno;	/* current async errno */

	/* ioctl-specific operation state */
	int			hfs_ioctl_cmd;	/* current ioctl cmd */
	hyprlofs_entries_t	*hfs_ioctl_arg;	/* current ioctl arg */
};

/*
 * The initializer for this Node module defines a Filesystem class backed by the
 * HyprlofsFilesystem class.  See README.md for details.
 */
Persistent<FunctionTemplate> HyprlofsFilesystem::hfs_templ;

NODE_MODULE(hyprlofs, HyprlofsFilesystem::Initialize)

void
HyprlofsFilesystem::Initialize(Handle<Object> target)
{
	HandleScope scope;
	Local<FunctionTemplate> hfs =
	    FunctionTemplate::New(HyprlofsFilesystem::New);

	hfs_templ = Persistent<FunctionTemplate>::New(hfs);
	hfs_templ->InstanceTemplate()->SetInternalFieldCount(1);
	hfs_templ->SetClassName(String::NewSymbol("Filesystem"));

	NODE_SET_PROTOTYPE_METHOD(hfs_templ, "mount",
	    HyprlofsFilesystem::Mount);
	NODE_SET_PROTOTYPE_METHOD(hfs_templ, "unmount",
	    HyprlofsFilesystem::Unmount);
	NODE_SET_PROTOTYPE_METHOD(hfs_templ, "addMappings",
	    HyprlofsFilesystem::AddMappings);
	NODE_SET_PROTOTYPE_METHOD(hfs_templ, "removeMappings",
	    HyprlofsFilesystem::RemoveMappings);
	NODE_SET_PROTOTYPE_METHOD(hfs_templ, "removeAll",
	    HyprlofsFilesystem::RemoveAll);

	target->Set(String::NewSymbol("Filesystem"),
	    hfs_templ->GetFunction());
}

/*
 * This object wraps a mountpoint, caching both the file descriptor and
 * mountpoint path.  Ownership of the file descriptor is passed to this object
 * when the object is constructed, and the fd will be closed when the object is
 * destroyed.
 */
Handle<Value>
HyprlofsFilesystem::New(const Arguments& args)
{
	HandleScope scope;
	HyprlofsFilesystem *hfs;

	if (args.Length() < 1 || !args[0]->IsString())
		return (ThrowException(Exception::Error(String::New(
		    "first argument must be a mountpoint"))));

	String::Utf8Value mountpt(args[0]->ToString());

	hfs = new HyprlofsFilesystem(*mountpt);
	hfs->Wrap(args.Holder());
	return (args.This());
}

HyprlofsFilesystem::HyprlofsFilesystem(const char *label) :
    node::ObjectWrap(),
    hfs_fd(-1),
    hfs_pending(false),
    hfs_ioctl_arg(NULL)
{
	(void) strlcpy(hfs_label, label, sizeof (hfs_label));
}

HyprlofsFilesystem::~HyprlofsFilesystem()
{
	if (this->hfs_fd != -1)
		(void) close(this->hfs_fd);
}

/*
 * See README.md.
 */
Handle<Value>
HyprlofsFilesystem::Mount(const Arguments& args)
{
	HandleScope scope;
	HyprlofsFilesystem *hfs;
	
	hfs = ObjectWrap::Unwrap<HyprlofsFilesystem>(args.Holder());

	if (hfs->argsCheck("mount", args, 0) == 0)
		hfs->async(eioMountRun, args[0]);

	return (Undefined());
}

/*
 * See README.md.
 */
Handle<Value>
HyprlofsFilesystem::Unmount(const Arguments& args)
{
	HandleScope scope;
	HyprlofsFilesystem *hfs;
	
	hfs = ObjectWrap::Unwrap<HyprlofsFilesystem>(args.Holder());

	if (hfs->argsCheck("unmount", args, 0) == 0)
		hfs->async(eioUmountRun, args[0]);

	return (Undefined());
}

/*
 * See README.md.
 */
Handle<Value>
HyprlofsFilesystem::AddMappings(const Arguments& args)
{
	HandleScope scope;
	HyprlofsFilesystem *hfs;
	hyprlofs_entries_t *entrylstp;

	hfs = ObjectWrap::Unwrap<HyprlofsFilesystem>(args.Holder());

	if (args.Length() < 1 || !args[0]->IsArray())
		return (ThrowException(Exception::Error(String::New(
		    "addMappings: expected array"))));

	if (hfs->argsCheck("addMappings", args, 1) != 0)
		return (Undefined());

	if ((entrylstp = hyfs_entries_populate_add(
	    Array::Cast(*args[0]))) == NULL)
		return (ThrowException(Exception::Error(String::New(
		    "addMappings: invalid mappings"))));

	hfs->asyncIoctl(HYPRLOFS_ADD_ENTRIES, entrylstp, args[1]);
	return (Undefined());
}

/*
 * See README.md.
 */
Handle<Value>
HyprlofsFilesystem::RemoveMappings(const Arguments& args)
{
	HandleScope scope;
	HyprlofsFilesystem *hfs;
	hyprlofs_entries_t *entrylstp;

	hfs = ObjectWrap::Unwrap<HyprlofsFilesystem>(args.Holder());

	if (args.Length() < 1 || !args[0]->IsArray())
		return (ThrowException(Exception::Error(String::New(
		    "removeMappings: expected array"))));

	if (hfs->argsCheck("removeMappings", args, 1) != 0)
		return (Undefined());

	if ((entrylstp = hyfs_entries_populate_remove(
	    Array::Cast(*args[0]))) == NULL)
		return (ThrowException(Exception::Error(String::New(
		    "removeMappings: invalid mappings"))));

	hfs->asyncIoctl(HYPRLOFS_RM_ENTRIES, entrylstp, args[1]);
	return (Undefined());
}

/*
 * See README.md.
 */
Handle<Value>
HyprlofsFilesystem::RemoveAll(const Arguments& args)
{
	HandleScope scope;
	HyprlofsFilesystem *hfs;

	hfs = ObjectWrap::Unwrap<HyprlofsFilesystem>(args.Holder());

	if (hfs->argsCheck("removeAll", args, 0) == 0)
		hfs->asyncIoctl(HYPRLOFS_RM_ALL, NULL, args[0]);

	return (Undefined());
}

/*
 * Validates arguments common to asynchronous functions.  If this function
 * returns -1, the caller must return back to V8 without invoking more
 * JavaScript code, since a JavaScript exception has already been scheduled to
 * be thrown.
 */
int
HyprlofsFilesystem::argsCheck(const char *label, const Arguments& args,
    int idx)
{
	char errbuf[128];

	if (idx >= args.Length() || !args[idx]->IsFunction()) {
		(void) snprintf(errbuf, sizeof (errbuf),
		    "%s: expected callback argument", label);
		ThrowException(Exception::Error(String::New(errbuf)));
		return (-1);
	}

	if (this->hfs_pending) {
		(void) snprintf(errbuf, sizeof (errbuf),
		    "%s: operation already in progress", label);
		ThrowException(Exception::Error(String::New(errbuf)));
		return (-1);
	}

	return (0);
}

/*
 * Invoked from Unmount and the hyprlofs ioctl entry points, running in the
 * event loop context, to invoke operations asynchronously.  This is pretty much
 * boilerplate for Node add-ons implementing asynchronous operations.
 */
void
HyprlofsFilesystem::async(void (*eiofunc)(eio_req *), Local<Value> callback)
{
	assert(!this->hfs_pending);
	this->hfs_pending = true;
	this->hfs_callback = Persistent<Function>::New(
	    Local<Function>::Cast(callback));
	this->Ref();

	eio_custom(eiofunc, EIO_PRI_DEFAULT, eioAsyncFini, this);
	ev_ref(EV_DEFAULT_UC);
}

/*
 * Invoked outside the event loop (via eio_custom) to actually run umount(2).
 */
void
HyprlofsFilesystem::eioUmountRun(eio_req *req)
{
	HyprlofsFilesystem *hfs = static_cast<HyprlofsFilesystem *>(req->data);
	if (hyfs_debug)
		(void) fprintf(stderr, "hyfs umount (%s)\n", hfs->hfs_label);

	/*
	 * We have to close our fd first in order to unmount the filesystem.  It
	 * will be reopened as-needed if the user goes to do another ioctl.
	 */
	if (hfs->hfs_fd != -1) {
		(void) close(hfs->hfs_fd);
		hfs->hfs_fd = -1;
	}

	hfs->hfs_errno = 0;
	hfs->hfs_rv = umount(hfs->hfs_label);
	hfs->hfs_errno = hfs->hfs_rv != 0 ? errno : 0;

	if (hyfs_debug)
		(void) fprintf(stderr, "    hyfs umount (%s) returned %d "
		    "(error = %s)\n", hfs->hfs_label, hfs->hfs_rv,
		    strerror(errno));
}

/*
 * Invoked outside the event loop (via eio_custom) to actually run mount(2).
 */
void
HyprlofsFilesystem::eioMountRun(eio_req *req)
{
	char optstr[256];

	HyprlofsFilesystem *hfs = static_cast<HyprlofsFilesystem *>(req->data);
	if (hyfs_debug)
		(void) fprintf(stderr, "hyfs mount (%s)\n", hfs->hfs_label);

	(void) strlcpy(optstr, "ro", sizeof (optstr));
	hfs->hfs_errno = 0;
	hfs->hfs_rv = mount("swap", hfs->hfs_label, MS_OPTIONSTR,
	    "hyprlofs", NULL, 0, optstr, sizeof (optstr));
	hfs->hfs_errno = hfs->hfs_rv != 0 ? errno : 0;

	if (hyfs_debug)
		(void) fprintf(stderr, "    hyfs mount (%s) returned %d "
		    "(error = %s, optstr=\"%s\")\n", hfs->hfs_label,
		    hfs->hfs_rv, strerror(errno), optstr);
}

/*
 * Invoked from C++ functions running in the event loop (e.g., AddMappings,
 * RemoveMappings, and so on) to invoke a hyprlofs ioctl asynchronously.
 */
void
HyprlofsFilesystem::asyncIoctl(int cmd, hyprlofs_entries_t *ioctl_arg,
    Local<Value> callback)
{
	this->hfs_ioctl_cmd = cmd;
	this->hfs_ioctl_arg = ioctl_arg;
	this->async(eioIoctlRun, callback);
}

/*
 * Invoked outside the event loop (via eio_custom) to actually perform a
 * hyprlofs ioctl.
 */
void
HyprlofsFilesystem::eioIoctlRun(eio_req *req)
{
	HyprlofsFilesystem *hfs = (HyprlofsFilesystem *)(req->data);

	if (hfs->hfs_fd == -1) {
		if (hyfs_debug)
			(void) fprintf(stderr, "    hyfs open (%s)\n",
			    hfs->hfs_label);

		if ((hfs->hfs_fd = open(hfs->hfs_label, O_RDONLY)) < 0) {
			hfs->hfs_rv = -1;
			hfs->hfs_errno = errno;
			if (hyfs_debug)
				(void) fprintf(stderr, "    hyfs open (%s) "
				    "failed: %s\n", hfs->hfs_label,
				    strerror(errno));
			return;
		}
	}

	if (hyfs_debug) {
		(void) fprintf(stderr, "    hyfs ioctl (%s): %s\n", hfs->hfs_label,
		    hyprlofs_cmdname(hfs->hfs_ioctl_cmd));

		if (hfs->hfs_ioctl_arg != NULL) {
			hyprlofs_entries_t *entrylstp =
			    (hyprlofs_entries_t *)hfs->hfs_ioctl_arg;

			for (int i = 0; i < entrylstp->hle_len; i++) {
				hyprlofs_entry_t *entryp =
				    &entrylstp->hle_entries[i];
				(void) fprintf(stderr, "    %3d: %s -> %s\n", i,
				    entryp->hle_path, entryp->hle_name);
			}
		}
	}

	hfs->hfs_errno = 0;
	hfs->hfs_rv = ioctl(hfs->hfs_fd, hfs->hfs_ioctl_cmd, hfs->hfs_ioctl_arg);
	hfs->hfs_errno = hfs->hfs_rv != 0 ? errno : 0;

	if (hyfs_debug)
		(void) fprintf(stderr, "    hyfs ioctl (%s) returned %d "
		    "(error = %s)\n", hfs->hfs_label, hfs->hfs_rv,
		    strerror(errno));
}

/*
 * Invoked back in the context of the event loop after an asynchronous ioctl has
 * completed.  Here we invoke the user's callback to indicate that the operation
 * has completed.
 */
int
HyprlofsFilesystem::eioAsyncFini(eio_req *req)
{
	HandleScope scope;
	char errbuf[128];
	Local<Function> callback;

	HyprlofsFilesystem *hfs = (HyprlofsFilesystem *)req->data;

	ev_unref(EV_DEFAULT_UC);
	hfs->Unref();

	/*
	 * We must clear out internal per-operation state before invoking the
	 * callback because the caller may call right back into us to begin
	 * another asynchronous operation.
	 */
	hfs->hfs_pending = false;

	hyfs_entries_free(hfs->hfs_ioctl_arg);
	hfs->hfs_ioctl_arg = NULL;

	callback = Local<Function>::New(hfs->hfs_callback);
	hfs->hfs_callback.Dispose();

	TryCatch try_catch;

	if (hfs->hfs_rv == 0) {
		callback->Call(Context::GetCurrent()->Global(), 0, NULL);
	} else {
		/* XXX include errno field and cmd/function name? (and add errno to
		 * open(2) error too) */
		(void) snprintf(errbuf, sizeof (errbuf), "hyprlofs: %d/%s",
		    hfs->hfs_errno, strerror(hfs->hfs_errno));
	
		Local<Value> argv[1];
		argv[0] = Exception::Error(String::New(errbuf));
	
		callback->Call(Context::GetCurrent()->Global(), 1, argv);
	}

	if (try_catch.HasCaught())
		FatalException(try_catch);
	return (0);
}

/*
 * hyprlofs interface functions.
 */

static const char *
hyprlofs_cmdname(int cmd)
{
	switch (cmd) {
	case HYPRLOFS_ADD_ENTRIES:	return ("ADD");
	case HYPRLOFS_RM_ENTRIES:	return ("REMOVE");
	case HYPRLOFS_RM_ALL:		return ("CLEAR");
	default:			break;
	}

	return ("UNKNOWN");
}

static hyprlofs_entries_t *
hyfs_entries_populate_add(const Local<Array>& arg)
{
	hyprlofs_entries_t *entrylstp;
	hyprlofs_entry_t *entries;

	if ((entrylstp = (hyprlofs_entries_t *)calloc(1,
	    sizeof (hyprlofs_entries_t))) == NULL ||
	    ((entries = (hyprlofs_entry_t *)calloc(arg->Length(),
	    sizeof (hyprlofs_entry_t))) == NULL)) {
		free(entrylstp);
		return (NULL);
	}

	entrylstp->hle_entries = entries;
	entrylstp->hle_len = arg->Length();

	for (int i = 0; i < arg->Length(); i++) {
		Local<Array> entry = Array::Cast(*(arg->Get(i)));
		if (*entry == NULL || entry->Length() != 2) {
			hyfs_entries_free(entrylstp);
			return (NULL);
		}

		String::Utf8Value file(entry->Get(0)->ToString());
		String::Utf8Value alias(entry->Get(1)->ToString());

		entries[i].hle_path = strdup(*file);
		entries[i].hle_name = strdup(*alias);

		if (entries[i].hle_path == NULL || entries[i].hle_name == NULL) {
			hyfs_entries_free(entrylstp);
			return (NULL);
		}

		entries[i].hle_plen = strlen(entries[i].hle_path);
		entries[i].hle_nlen = strlen(entries[i].hle_name);
	}

	return (entrylstp);
}

static hyprlofs_entries_t *
hyfs_entries_populate_remove(const Local<Array>& arg)
{
	hyprlofs_entries_t *entrylstp;
	hyprlofs_entry_t *entries;

	if ((entrylstp = (hyprlofs_entries_t *)calloc(1,
	    sizeof (hyprlofs_entries_t))) == NULL ||
	    ((entries = (hyprlofs_entry_t *)calloc(arg->Length(),
	    sizeof (hyprlofs_entry_t))) == NULL)) {
		free(entrylstp);
		return (NULL);
	}

	entrylstp->hle_entries = entries;
	entrylstp->hle_len = arg->Length();

	for (int i = 0; i < arg->Length(); i++) {
		String::Utf8Value alias(arg->Get(i)->ToString());

		entries[i].hle_name = strdup(*alias);

		if (entries[i].hle_name == NULL) {
			hyfs_entries_free(entrylstp);
			return (NULL);
		}

		entries[i].hle_nlen = strlen(entries[i].hle_name);
	}

	return (entrylstp);
}

static void
hyfs_entries_free(hyprlofs_entries_t *entrylstp)
{
	if (entrylstp == NULL)
		return;

	for (int i = 0; i < entrylstp->hle_len; i++) {
		/* XXX jerry */
		free((void *)entrylstp->hle_entries[i].hle_name);
		free((void *)entrylstp->hle_entries[i].hle_path);
	}

	free(entrylstp);
}
