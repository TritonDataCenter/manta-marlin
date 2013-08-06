/*
 * lib/provider.js: create a DTrace provider from a declarative definition
 */

var mod_assert = require('assert');

var mod_dtrace_provider = require('dtrace-provider');
var mod_verror = require('verror');

var VError = mod_verror.VError;

/* public interface */
exports.createProvider = createProvider;
exports.validateProvider = validateProvider;

function createProvider(def)
{
	var err, dtp;

	err = validateProvider(def);
	if (err)
		throw (err);

	dtp = mod_dtrace_provider.createDTraceProvider(def['name']);
	def['probes'].forEach(function (probe) {
		dtp.addProbe.apply(dtp, probe);
	});
	return (dtp);
}

function validateProvider(def)
{
	var i, j, probe;

	if (!def.hasOwnProperty('name') || typeof (def['name']) != 'string')
		return (new Error('provider: expected string "name"'));

	if (!def.hasOwnProperty('probes') || !Array.isArray(def['probes']))
		return (new VError('provider "%s": expected array "probes"',
		    def['name']));

	if (def['probes'].length === 0)
		return (new VError('provider "%s": expected at least one probe',
		    def['name']));

	for (i = 0; i < def['probes'].length; i++) {
		probe = def['probes'][i];
		if (!Array.isArray(probe) || probe.length === 0)
			return (new VError('provider "%s": probe %d: ' +
			    'expected non-empty array', def['name'], i));
		if (typeof (probe[0]) != 'string')
			return (new VError('provider "%s": probe %d: ' +
			    'probe name must be a string', def['name'], i));

		if (typeof (probe[0]) != 'string')
			return (new VError('provider "%s": probe %d: ' +
			    'probe name must be a string', def['name'], i));

		for (j = 1; j < probe.length; j++) {
			switch (probe[j]) {
			case 'json':
			case 'char *':
			case 'char*':
			case 'int':
				break;
			default:
				return (new VError('provider "%s": probe %d: ' +
				    'argument %d: unsupported probe type',
				    def['name'], i, j - 1));
			}
		}
	}

	return (null);
}
