/*
 * lib/agent/executor.js: executes shell commands
 */

var mod_events = require('events');
var mod_child = require('child_process');
var mod_fs = require('fs');
var mod_util = require('util');

var mod_assertplus = require('assert-plus');
var mod_contract = require('illumos_contract');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var EventEmitter = mod_events.EventEmitter;
var VError = mod_verror.VError;

/* Public interface */
module.exports = Executor;


/*
 * Manages the reliable execution of an arbitrary shell command.  Arguments:
 *
 *     exec		shell command to invoke (invoked with "bash -c")
 *
 *     env		environment
 *
 *     log		Bunyan-style logger
 *
 *     stdout,		WritableStreams for stdout and stderr.  The caller is
 *     stderr		responsible for setting up these streams so that they
 *     			can be passed to child_process.spawn, which means that
 *     			they must be Node library streams implemented with a
 *     			file descriptor.  The underlying file descriptor will be
 *     			passed to the child process, the stream will not be
 *     			written or modified by this process, and the caller is
 *     			responsible for destroying the streams after calling
 *     			start() to avoid leakage.
 *
 *     stdin		File descriptor (from fs.open) or any readable stream to
 *     			use as stdin.  If a file descriptor is used, it will be
 *     			passed directly to the child process.  The caller
 *     			maintains responsibility for it and it should be closed
 *     			after calling start().  If a stream is used, the
 *     			contents will be piped to to the child process.  (This
 *     			is different than what happens for stdout and stderr.
 *     			You can use a custom ReadableStream implementation here,
 *     			and you're not responsible for closing it.)
 *
 * After constructing this object, invoke start() to execute the given command.
 * When execution completes, "done" is emitted with a result object with:
 *
 *     error		Error object denoting fork/exec error (very unlikely)
 *
 *     code		Exit status (see child_process.spawn)
 *
 *     signal		Fatal signal (see child_process.spawn)
 *
 *     core		If true, indicates that the child process or its
 *     			progeny dumped core
 */
function Executor(args)
{
	mod_assertplus.object(args, 'args');
	mod_assertplus.string(args['exec'], 'args.exec');
	mod_assertplus.object(args['env'], 'args.env');
	mod_assertplus.object(args['log'], 'args.log');
	mod_assertplus.object(args['stdout'], 'args.stdout');
	mod_assertplus.object(args['stderr'], 'args.stderr');

	/* initialization args */
	this.ex_exec = args['exec'];
	this.ex_env = args['env'];
	this.ex_log = args['log'];
	this.ex_stdin = args['stdin'];
	this.ex_stdout = args['stdout'];
	this.ex_stderr = args['stderr'];

	/* dynamic state */
	this.ex_barrier = undefined;	/* drains when execution is done */
	this.ex_child = undefined;	/* child process */
	this.ex_contract = undefined;	/* OS contract object */
	this.ex_ctevent = undefined;
	this.ex_result = {
	    'core': false,		/* process dumped core */
	    'code': undefined,		/* exit code */
	    'signal': undefined,	/* fatal signal number */
	    'error': undefined		/* fork/exec error */
	};

	EventEmitter.call(this);
}

mod_util.inherits(Executor, EventEmitter);

/*
 * Begin execution.  See above.
 */
Executor.prototype.start = function ()
{
	mod_assertplus.ok(this.ex_barrier === undefined,
	    'execution has already started');

	var executor = this;

	this.ex_log.info('launching "%s"', this.ex_exec);

	try {
		this.ex_child = this.spawn();
	} catch (exn) {
		/*
		 * The only errors we can get here (ENOMEM, EAGAIN, and the
		 * like) are likely unrecoverable -- for this whole process,
		 * since it indicates that the system (or zone) is out of
		 * resources to perform even basic tasks.  Nevertheless, we do
		 * our best to report this like any other error.
		 */
		this.ex_log.fatal(exn, 'failed to launch "%s"', this.ex_exec);
		this.ex_result['error'] = new VError(
		    exn, 'failed to launch "%s"', this.ex_exec);
		process.nextTick(
		    function () { executor.emit('done', executor.ex_result); });
		return;
	}

	this.ex_contract = mod_contract.latest();

	this.ex_barrier = mod_vasync.barrier();
	this.ex_barrier.on('drain', this.onExecDone.bind(this));
	this.ex_barrier.start('contract');
	this.ex_barrier.start('child process');

	if (typeof (this.ex_stdin) != 'number') {
		this.ex_stdin.pipe(this.ex_child.stdin);
		this.ex_stdin.resume();

		this.ex_child.stdin.on('error', function (err) {
			if (err['code'] == 'EPIPE')
				return;

			executor.ex_log.warn(err, 'error writing to child');
		});
	}

	this.ex_child.on('exit', this.onChildExit.bind(this));
	this.ex_contract.on('pr_core', this.onContractEvent.bind(this));
	this.ex_contract.on('pr_empty', this.onContractEvent.bind(this));
	this.ex_contract.on('pr_hwerr', this.onContractEvent.bind(this));
};

Executor.prototype.spawn = function ()
{
	/*
	 * We use --norc to force bash not to read ~/.bashrc.  Actually, we
	 * don't really care whether it does or not, as long as it's consistent.
	 * But without specifying this, bash attempts to determine whether its
	 * stdin refers to a network socket or not, and its check fails if the
	 * socket is no longer writable.  (This is arguably a bug, but it's our
	 * own fault for expecting more rigor from an ancient heuristic intended
	 * to determine if bash is running under rsh.)   Node only shuts down
	 * the socket when we call end(), so it's a race between end() and bash
	 * startup.  We avoid this mess by just specifying --norc.
	 */
	return (mod_child.spawn('bash', [ '--norc', '-c', this.ex_exec ], {
	    'env': this.ex_env,
	    'stdio': [
	        typeof (this.ex_stdin) == 'number' ? this.ex_stdin : 'pipe',
		this.ex_stdout,
		this.ex_stderr ]
	}));
};

Executor.prototype.onChildExit = function (code, signal)
{
	if (code === 0 || code)
		this.ex_log.info('user command exited with status ' + code);
	else
		this.ex_log.info('user command killed by signal ' + signal);

	this.ex_result['code'] = code;
	this.ex_result['signal'] = signal;
	this.ex_barrier.done('child process');
};

Executor.prototype.onContractEvent = function (ev)
{
	this.ex_log.info('contract event', ev);

	if (ev['type'] == 'pr_core')
		this.ex_result['core'] = true;

	if (this.ex_ctevent === undefined) {
		this.ex_ctevent = ev;
		this.ex_barrier.done('contract');
	}
};

Executor.prototype.onExecDone = function ()
{
	mod_assertplus.ok(this.ex_barrier !== undefined);
	this.ex_barrier = undefined;

	/*
	 * Abandon the contract, dispose it (because it maintains its own
	 * reference), and remove listeners so that this function doesn't get
	 * invoked for a different event on task "i" (which we took for dead)
	 * after we've moved on to "i+1".
	 */
	this.ex_contract.abandon();
	this.ex_contract.dispose();
	this.ex_contract.removeAllListeners();
	this.ex_contract = undefined;

	mod_assertplus.ok(this.ex_result['code'] !== undefined ||
	    this.ex_result['signal'] !== undefined);
	this.emit('done', this.ex_result);
};
