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
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mount.h>
#include <sys/fs/hyprlofs.h>

using namespace v8;
using namespace node;

/*
 * This flag controls whether to emit debug output to stderr whenever we make a
 * hyprlofs ioctl call.  It can be overridden on a per-object basis.
 */
static bool hyprlofs_debug = false;

class HyprlofsFilesystem;

static const char *hyprlofs_cmdname(int);
static hyprlofs_entries_t *hyprlofs_entries_populate_add(const Local<Array>&);
static hyprlofs_entries_t *hyprlofs_entries_populate_remove(
    const Local<Array>&);
static void hyprlofs_entries_free(hyprlofs_entries_t *);

/*
 * The HyprlofsFilesystem is the nexus of administration.  Users construct an
 * instance of this object to operate on any hyprlofs mount, and then invoke
 * methods to add, remove, or clear mappings.  See README.md for details.
 */
class HyprlofsFilesystem : node::ObjectWrap {
public:
	static void Initialize(Handle<Object> target);

protected:
	HyprlofsFilesystem(const char *, bool);
	~HyprlofsFilesystem();

	void async(void (*)(uv_work_t *), Local<Value>);
	void doIoctl(int, void *);

	static void eioAsyncFini(uv_work_t *);
	static void eioIoctlRun(uv_work_t *);
	static void eioIoctlGetRun(uv_work_t *);
	static void eioMountRun(uv_work_t *);
	static void eioUmountRun(uv_work_t *);

	static Handle<Value> New(const Arguments&);
	static Handle<Value> Mount(const Arguments&);
	static Handle<Value> Unmount(const Arguments&);
	static Handle<Value> AddMappings(const Arguments&);
	static Handle<Value> ListMappings(const Arguments&);
	static Handle<Value> RemoveAll(const Arguments&);
	static Handle<Value> RemoveMappings(const Arguments& );

	int argsCheck(const char *, const Arguments&, int);

private:
	static Persistent<FunctionTemplate> hfs_templ;

	/* immutable state */
	bool			hfs_debug;		/* debug output */
	int			hfs_fd;			/* mountpoint fd */
	char			hfs_label[PATH_MAX];	/* mountpoint path */

	/*
	 * Several invariants are maintained for these fields:
	 *
	 * While an asynchronous task (triggered by a caller invoking one of
	 * several methods on this object) is pending:
	 *
	 *     hfs_pending is true. We will not allow another asynchronous task
	 *     to be issued while hfs_pending is true.
	 *
	 *     hfs_callback refers to the JS callback function to be invoked
	 *     upon completion of the task.
	 *
	 *     hfs_ioctl_cmd refers to the ioctl command if this is an ioctl
	 *     task or -1 otherwise.
	 *
	 * After an asynchronous task completes, hfs_rv and hfs_errno are valid
	 * until the next asynchronous task is dispatched.
	 *
	 * For ioctl tasks, hfs_ioctl_arg is valid from when the task is
	 * dispatched until it completes.
	 *
	 * For the "get" ioctl specifically (which is the only one that returns
	 * data from the ioctl), hfs_curr_ents and hfs_entv are valid only
	 * when processing the ioctl completion.
	 *
	 * These invariants are mostly maintained by async() (which dispatches
	 * new tasks) and eioAsyncFini (which handles completion).  Each
	 * function dispatching an aysnc ioctl takes care of setting ioctl_cmd
	 * and ioctl_arg first.
	 */

	/* common async operation state */
	char			hfs_opname[32];	/* operation name */
	bool 			hfs_pending;	/* operation outstanding */
	Persistent<Function>	hfs_callback;	/* user callback */
	int			hfs_rv;		/* current async rv */
	int			hfs_errno;	/* current async errno */

	/* ioctl-specific operation state */
	int			hfs_ioctl_cmd;	/* current ioctl cmd */
	void			*hfs_ioctl_arg;	/* current ioctl arg */

	/* get-specific state */
	hyprlofs_curr_entries_t	hfs_curr_ents;	/* current mappings */
	hyprlofs_curr_entry_t	*hfs_entv;
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
	NODE_SET_PROTOTYPE_METHOD(hfs_templ, "listMappings",
	    HyprlofsFilesystem::ListMappings);
	NODE_SET_PROTOTYPE_METHOD(hfs_templ, "removeMappings",
	    HyprlofsFilesystem::RemoveMappings);
	NODE_SET_PROTOTYPE_METHOD(hfs_templ, "removeAll",
	    HyprlofsFilesystem::RemoveAll);

	target->Set(String::NewSymbol("Filesystem"),
	    hfs_templ->GetFunction());
}

/*
 * This object wraps a mountpoint, caching the mountpoint path.  The mountpoint
 * path is not checked or used until the first time it's needed.
 */
Handle<Value>
HyprlofsFilesystem::New(const Arguments& args)
{
	HandleScope scope;
	HyprlofsFilesystem *hfs;
	bool debug = false;

	if (args.Length() < 1 || !args[0]->IsString())
		return (ThrowException(Exception::Error(String::New(
		    "first argument must be a mountpoint"))));

	String::Utf8Value mountpt(args[0]->ToString());

	if (args.Length() > 1)
		debug = args[1]->BooleanValue();

	hfs = new HyprlofsFilesystem(*mountpt, debug);
	hfs->Wrap(args.Holder());
	return (args.This());
}

HyprlofsFilesystem::HyprlofsFilesystem(const char *label, bool debug) :
    node::ObjectWrap(),
    hfs_fd(-1),
    hfs_debug(debug),
    hfs_pending(false),
    hfs_ioctl_cmd(-1),
    hfs_ioctl_arg(NULL),
    hfs_entv(NULL)
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

	if ((entrylstp = hyprlofs_entries_populate_add(
	    Array::Cast(*args[0]))) == NULL)
		return (ThrowException(Exception::Error(String::New(
		    "addMappings: invalid mappings"))));

	hfs->hfs_ioctl_cmd = HYPRLOFS_ADD_ENTRIES;
	hfs->hfs_ioctl_arg = entrylstp;
	hfs->async(eioIoctlRun, args[1]);
	return (Undefined());
}

/*
 * See README.md.
 */
Handle<Value>
HyprlofsFilesystem::ListMappings(const Arguments& args)
{
	HandleScope scope;
	HyprlofsFilesystem *hfs;

	hfs = ObjectWrap::Unwrap<HyprlofsFilesystem>(args.Holder());

	if (hfs->argsCheck("listMappings", args, 0) == 0) {
		hfs->hfs_ioctl_cmd = HYPRLOFS_GET_ENTRIES;
		hfs->async(eioIoctlGetRun, args[0]);
	}

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

	if ((entrylstp = hyprlofs_entries_populate_remove(
	    Array::Cast(*args[0]))) == NULL)
		return (ThrowException(Exception::Error(String::New(
		    "removeMappings: invalid mappings"))));

	hfs->hfs_ioctl_cmd = HYPRLOFS_RM_ENTRIES;
	hfs->hfs_ioctl_arg = entrylstp;
	hfs->async(eioIoctlRun, args[1]);
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

	if (hfs->argsCheck("removeAll", args, 0) == 0) {
		hfs->hfs_ioctl_cmd = HYPRLOFS_RM_ALL;
		hfs->async(eioIoctlRun, args[0]);
	}

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
HyprlofsFilesystem::async(void (*eiofunc)(uv_work_t *), Local<Value> callback)
{
	assert(!this->hfs_pending);
	this->hfs_pending = true;
	this->hfs_callback = Persistent<Function>::New(
	    Local<Function>::Cast(callback));
	this->Ref();

	uv_work_t *req = new uv_work_t;
	req->data = this;
	uv_queue_work(uv_default_loop(), req, eiofunc, eioAsyncFini);
}

/*
 * Invoked outside the event loop (via uv_queue_work) to actually run umount(2).
 */
void
HyprlofsFilesystem::eioUmountRun(uv_work_t *req)
{
	HyprlofsFilesystem *hfs = static_cast<HyprlofsFilesystem *>(req->data);
	if (hyprlofs_debug || hfs->hfs_debug)
		(void) fprintf(stderr, "hyprlofs umount %s\n", hfs->hfs_label);

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
	(void) strlcpy(hfs->hfs_opname, "hyprlofs umount",
	    sizeof (hfs->hfs_opname));

	if (hyprlofs_debug || hfs->hfs_debug)
		(void) fprintf(stderr, "    hyprlofs umount (%s) returned %d "
		    "(error = %s)\n", hfs->hfs_label, hfs->hfs_rv,
		    strerror(errno));
}

/*
 * Invoked outside the event loop (via uv_queue_work) to actually run mount(2).
 */
void
HyprlofsFilesystem::eioMountRun(uv_work_t *req)
{
	char optstr[256];

	HyprlofsFilesystem *hfs = static_cast<HyprlofsFilesystem *>(req->data);
	if (hyprlofs_debug || hfs->hfs_debug)
		(void) fprintf(stderr, "hyprlofs mount %s\n", hfs->hfs_label);

	(void) strlcpy(optstr, "ro", sizeof (optstr));
	hfs->hfs_errno = 0;
	hfs->hfs_rv = mount("swap", hfs->hfs_label, MS_OPTIONSTR,
	    "hyprlofs", NULL, 0, optstr, sizeof (optstr));
	hfs->hfs_errno = hfs->hfs_rv != 0 ? errno : 0;
	(void) strlcpy(hfs->hfs_opname, "hyprlofs mount",
	    sizeof (hfs->hfs_opname));

	if (hyprlofs_debug || hfs->hfs_debug)
		(void) fprintf(stderr, "    hyprlofs mount (%s) returned %d "
		    "(error = %s, optstr=\"%s\")\n", hfs->hfs_label,
		    hfs->hfs_rv, strerror(errno), optstr);
}

void
HyprlofsFilesystem::doIoctl(int cmd, void *arg)
{
	if (this->hfs_fd == -1) {
		if (hyprlofs_debug || this->hfs_debug)
			(void) fprintf(stderr, "    hyprlofs open (%s)\n",
			    this->hfs_label);

		if ((this->hfs_fd = open(this->hfs_label, O_RDONLY)) < 0) {
			this->hfs_rv = -1;
			this->hfs_errno = errno;
			(void) strlcpy(this->hfs_opname, "hyprlofs open",
			    sizeof (this->hfs_opname));
			if (hyprlofs_debug || this->hfs_debug)
				(void) fprintf(stderr, "    hyprlofs open (%s) "
				    "failed: %s\n", this->hfs_label,
				    strerror(errno));
			return;
		}
	}

	if (hyprlofs_debug || this->hfs_debug) {
		(void) fprintf(stderr, "    hyprlofs ioctl (%s): %s\n",
		    this->hfs_label, hyprlofs_cmdname(cmd));

		if (arg != NULL) {
			hyprlofs_entries_t *entrylstp =
			    (hyprlofs_entries_t *)arg;

			for (int i = 0; i < entrylstp->hle_len; i++) {
				hyprlofs_entry_t *entryp =
				    &entrylstp->hle_entries[i];
				(void) fprintf(stderr, "    %3d: %s -> %s\n", i,
				    entryp->hle_path, entryp->hle_name);
			}
		}
	}

	this->hfs_errno = 0;
	this->hfs_rv = ioctl(this->hfs_fd, cmd, arg);
	this->hfs_errno = this->hfs_rv != 0 ? errno : 0;
	(void) snprintf(this->hfs_opname, sizeof (this->hfs_opname),
	    "hyprlofs ioctl %s", hyprlofs_cmdname(cmd));

	if (hyprlofs_debug || this->hfs_debug)
		(void) fprintf(stderr, "    hyprlofs ioctl (%s) returned %d "
		    "(error = %s)\n", this->hfs_label, this->hfs_rv,
		    strerror(errno));

	if (this->hfs_rv == -1 && this->hfs_errno == ENOTTY) {
		(void) close(this->hfs_fd);
		this->hfs_fd = -1;
	}
}

/*
 * Invoked outside the event loop (via uv_queue_work) to actually perform a
 * hyprlofs ioctl.
 */
void
HyprlofsFilesystem::eioIoctlRun(uv_work_t *req)
{
	HyprlofsFilesystem *hfs = (HyprlofsFilesystem *)(req->data);
	hfs->doIoctl(hfs->hfs_ioctl_cmd, hfs->hfs_ioctl_arg);
}

void
HyprlofsFilesystem::eioIoctlGetRun(uv_work_t *req)
{
	HyprlofsFilesystem *hfs = (HyprlofsFilesystem *)(req->data);
	hyprlofs_curr_entry_t *entv;

	/*
	 * The "GET" ioctl is a bit more complex than the others because we're
	 * retrieving a variable-size amount of data.  We invoke the ioctl
	 * twice: once to see how many mappings we got, and once to actually
	 * retrieve them.
	 */
	assert(hfs->hfs_ioctl_arg == NULL);
	assert(hfs->hfs_entv == NULL);

	bzero(&hfs->hfs_curr_ents, sizeof (hfs->hfs_curr_ents));
	hfs->doIoctl(HYPRLOFS_GET_ENTRIES, &hfs->hfs_curr_ents);

	if (hfs->hfs_rv == 0 || hfs->hfs_errno != E2BIG) {
		/*
		 * We either got zero entries, or we got an unexpected error.
		 * Either way, bail out and let the fini function figure out
		 * how to express it back to the user.
		 */
		return;
	}

top:
	if ((hfs->hfs_entv = (hyprlofs_curr_entry_t *)calloc(
	    sizeof (hyprlofs_curr_entry_t),
	    hfs->hfs_curr_ents.hce_cnt)) == NULL) {
		hfs->hfs_errno = ENOMEM;
		return;
	}

	hfs->hfs_curr_ents.hce_entries = hfs->hfs_entv;
	hfs->doIoctl(HYPRLOFS_GET_ENTRIES, &hfs->hfs_curr_ents);

	if (hfs->hfs_rv == 0) {
		/*
		 * We're done.  The fini eio callback will convert hfs_entv into
		 * a JavaScript object and invoke the callback.
		 */
		return;
	}

	free(hfs->hfs_entv);
	hfs->hfs_curr_ents.hce_entries = hfs->hfs_entv = NULL;

	if (hfs->hfs_errno == E2BIG)
		goto top;
}

/*
 * Invoked back in the context of the event loop after an asynchronous ioctl has
 * completed.  Here we invoke the user's callback to indicate that the operation
 * has completed.
 */
void
HyprlofsFilesystem::eioAsyncFini(uv_work_t *req)
{
	HandleScope scope;
	Local<Function> callback;
	int cmd;

	HyprlofsFilesystem *hfs = (HyprlofsFilesystem *)req->data;
	delete req;
	hfs->Unref();

	/*
	 * We must clear out internal per-operation state before invoking the
	 * callback because the caller may call right back into us to begin
	 * another asynchronous operation.
	 */
	hfs->hfs_pending = false;

	hyprlofs_entries_free((hyprlofs_entries_t *)hfs->hfs_ioctl_arg);
	hfs->hfs_ioctl_arg = NULL;

	cmd = hfs->hfs_ioctl_cmd;
	hfs->hfs_ioctl_cmd = -1;

	callback = Local<Function>::New(hfs->hfs_callback);
	hfs->hfs_callback.Dispose();

	Handle<Value> argv[2];
	int argc = 0;

	if (hfs->hfs_rv != 0) {
		assert(hfs->hfs_entv == NULL);
		argv[argc++] = ErrnoException(hfs->hfs_errno, hfs->hfs_opname,
		    "", hfs->hfs_label);
	} else if (cmd == HYPRLOFS_GET_ENTRIES) {
		argv[argc++] = Null();

		Local<Array> rv = Array::New(hfs->hfs_curr_ents.hce_cnt);
		argv[argc++] = rv;

		/*
		 * hfs_entv (which is hfs_curr_ents.hce_entries) may be NULL,
		 * but only if hfs_curr_ents.hce_cnt == 0.
		 */
		for (int i = 0; i < hfs->hfs_curr_ents.hce_cnt; i++) {
			Local<Array> entry = Array::New(2);
			hyprlofs_curr_entry_t *ent = &hfs->hfs_curr_ents.
			    hce_entries[i];
			entry->Set(0, String::New(ent->hce_path));
			entry->Set(1, String::New(ent->hce_name));
			rv->Set(i, entry);
		}

		free(hfs->hfs_entv);
		hfs->hfs_curr_ents.hce_entries = hfs->hfs_entv = NULL;
	}

	TryCatch try_catch;
	callback->Call(Context::GetCurrent()->Global(), argc, argv);
	if (try_catch.HasCaught())
		FatalException(try_catch);
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
	case HYPRLOFS_GET_ENTRIES:	return ("GET");
	default:			break;
	}

	return ("UNKNOWN");
}

static hyprlofs_entries_t *
hyprlofs_entries_populate_add(const Local<Array>& arg)
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
		if (!arg->Get(i)->IsArray()) {
			hyprlofs_entries_free(entrylstp);
			return (NULL);
		}

		Local<Array> entry = Array::Cast(*(arg->Get(i)));
		if (*entry == NULL || entry->Length() != 2) {
			hyprlofs_entries_free(entrylstp);
			return (NULL);
		}

		String::Utf8Value file(entry->Get(0)->ToString());
		String::Utf8Value alias(entry->Get(1)->ToString());

		entries[i].hle_path = strdup(*file);
		entries[i].hle_name = strdup(*alias);

		if (entries[i].hle_path == NULL ||
		    entries[i].hle_name == NULL) {
			hyprlofs_entries_free(entrylstp);
			return (NULL);
		}

		entries[i].hle_plen = strlen(entries[i].hle_path);
		entries[i].hle_nlen = strlen(entries[i].hle_name);
	}

	return (entrylstp);
}

static hyprlofs_entries_t *
hyprlofs_entries_populate_remove(const Local<Array>& arg)
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
			hyprlofs_entries_free(entrylstp);
			return (NULL);
		}

		entries[i].hle_nlen = strlen(entries[i].hle_name);
	}

	return (entrylstp);
}

static void
hyprlofs_entries_free(hyprlofs_entries_t *entrylstp)
{
	if (entrylstp == NULL)
		return;

	for (int i = 0; i < entrylstp->hle_len; i++) {
		free(entrylstp->hle_entries[i].hle_name);
		free(entrylstp->hle_entries[i].hle_path);
	}

	free(entrylstp->hle_entries);
	free(entrylstp);
}
