'use strict';

const request     = require('request');
const url         = require('url');
const qs          = require('querystring');
const http        = require('http');
const https       = require('https');
const _           = require('lodash');
const uuid        = require('uuid');
const JSONStream  = require('JSONStream');
const stream      = require('stream');
const tsv         = require('tsv');
const StreamIter  = require('stream2asynciter').WrapStream;
const zlib        = require('zlib');

const defaultFormat = 'JSON';

const httpAgent = new http.Agent({
    keepAlive: true,
    keepAliveMsecs: 5000,
    maxSockets: Infinity
});

const httpsAgent = new https.Agent({
    keepAlive: true,
    keepAliveMsecs: 5000,
    maxSockets: Infinity,
});

// Helper functions
function isPostQuery(query, usePostFlag) {
    const queryLength = Buffer.byteLength(query || '', 'utf8');
    return usePostFlag || queryLength > 8192 || /INSERT|ALTER|CREATE|DROP|RENAME|OPTIMIZE/i.test(query);
}
function getFullFormatName(shortFormat) {
    const fmt = String(shortFormat || defaultFormat);
    switch (fmt.toLowerCase()) {
        case 'json': return 'JSON';
        case 'csv': return 'CSV';
        case 'tsv': return 'TabSeparated';
        case 'tabseparated': return 'TabSeparated';
        default: return fmt;
    }
}
function getShortFormatName(fullFormat) {
    const fmt = String(fullFormat || defaultFormat);
     switch (fmt) {
         case 'JSON': return 'json';
         case 'CSV': return 'csv';
         case 'TabSeparated': return 'tsv';
         default: return null;
     }
}


class ClickHouse {
    constructor (options) {
        // Default options initialization
        this.options = {
            debug: false, basicAuth: null, isUseGzip: false, format: defaultFormat,
            raw: false, host: 'localhost', port: 8123, protocol: 'http:',
            pathname: '/', query: {}, config: {
                output_format_json_quote_64bit_integers: 0,
                enable_http_compression: 0, // Default to 0 here
            }, reqParams: {}, isSessionPerQuery: false, usePost: undefined,
            connect_timeout: 10000, requestTimeout: 30000,
        };
        // Option merging logic
        if (typeof options === 'string') {
            const parsedUrl = url.parse(options, true);
            _.assign(this.options, _.pick(parsedUrl, ['host', 'port', 'protocol', 'pathname']));
            if (parsedUrl.host) {
                const hostParts = parsedUrl.host.split(':');
                this.options.host = hostParts[0];
                this.options.port = hostParts[1] ? parseInt(hostParts[1], 10) : (parsedUrl.protocol === 'https:' ? 443 : 8123);
            }
            _.assign(this.options.query, parsedUrl.query);
            if (parsedUrl.auth) {
                this.options.basicAuth = { username: parsedUrl.auth.split(':')[0], password: parsedUrl.auth.split(':')[1] };
            }
        } else if (typeof options === 'object') {
             if (options.url) {
                 try {
                     const urlOpts = url.parse(String(options.url));
                     if (urlOpts.protocol) this.options.protocol = urlOpts.protocol;
                     if (urlOpts.host) {
                         const hostParts = urlOpts.host.split(':');
                         this.options.host = hostParts[0];
                         if (hostParts[1]) this.options.port = parseInt(hostParts[1], 10);
                     }
                     if (urlOpts.pathname) this.options.pathname = urlOpts.pathname;
                     if (urlOpts.auth && options.basicAuth === undefined) {
                          options.basicAuth = { username: urlOpts.auth.split(':')[0], password: urlOpts.auth.split(':')[1] };
                     }
                 } catch (e) { console.error("Error parsing ClickHouse URL option:", e); }
             }
             _.merge(this.options, options);
        }
        // Gzip finalization - SET THE CONFIG PARAMETER here based on option
        if (this.options.isUseGzip) {
             this.options.config.enable_http_compression = 1;
        } else {
             this.options.config.enable_http_compression = 0;
        }
        // Format validation
        this.options.format = getFullFormatName(this.options.format);
        // Base query options
        this.queryOptions = { host: this.options.host, port: this.options.port, protocol: this.options.protocol, pathname: this.options.pathname, agent: this.options.protocol === 'https:' ? httpsAgent : httpAgent };

        if (this.options.debug) {
            console.log('--- ClickHouse Driver Initialized Options ---');
            console.log(JSON.stringify(this.options, null, 2));
            console.log('---------------------------------------------');
        }
    } // End constructor

    query (query, options, cb) {
        if (this.options.debug) console.log(`--- CH DEBUG [${new Date().toISOString()}] Driver query() method called for Query Type: ${query?.substring(0, 30)}...`);

        if (typeof options === 'function') { cb = options; options = {}; }
        options = options || {};
        if (options.data) {
             if (this.options.debug) console.log(`--- CH DEBUG [${new Date().toISOString()}] Driver query() routing to INSERT for Query Type: ${query?.substring(0, 30)}...`);
             return this.insert(query, options, cb);
        }

        let cursor;
        try {
            if (this.options.debug) console.log(`--- CH DEBUG [${new Date().toISOString()}] Driver query() creating QueryCursor for Query Type: ${query?.substring(0, 30)}...`);
            cursor = new QueryCursor(this, query, options);
        } catch (e) {
             console.error(`ClickHouse Driver: ERROR during QueryCursor creation for Query Type: ${query?.substring(0, 30)}...`, e);
             if (cb) { process.nextTick(() => cb(e)); return; }
             else { throw e; }
        }

        if (typeof cb === 'function') {
             if (this.options.debug) console.log(`--- CH DEBUG [${new Date().toISOString()}] Driver query() executing immediately (callback) for Query Type: ${query?.substring(0, 30)}...`);
             cursor.exec(cb);
        }
        else {
            if (this.options.debug) console.log(`--- CH DEBUG [${new Date().toISOString()}] Driver query() returning QueryCursor for Query Type: ${query?.substring(0, 30)}...`);
            return cursor;
        }
    }

    insert(query, options, cb) {
        // Simplified insert - okay for now
        if (typeof options === 'function') { cb = options; options = {}; }
        options = options || {};
        const insertFormat = getFullFormatName(options.format || 'JSONEachRow');
        // Pass isUseGzip from instance options to queryParams for _getHttpRequestOptions
        const queryParams = { ...options, format: insertFormat, query_sql: query, config: { ...this.options.config, ...(options.config || {}) }, isUseGzip: this.options.isUseGzip };
        const reqOptions = this._getHttpRequestOptions(query, queryParams, true); // Force POST

        return new Promise((resolve, reject) => {
            const dataToInsert = options.data || [];
            let jsonData = "";
            if (insertFormat === 'JSONEachRow') { jsonData = dataToInsert.map(row => JSON.stringify(row)).join('\n'); }
            else if (insertFormat === 'TabSeparated') { jsonData = dataToInsert.map(row => Object.values(row).join('\t')).join('\n'); }
            else if (insertFormat === 'CSV') { jsonData = dataToInsert.map(row => Object.values(row).map(v => `"${String(v).replace(/"/g, '""')}"`).join(',')).join('\n'); }
            else { return reject(new Error(`Unsupported format for simple insert: ${insertFormat}`)); }

            reqOptions.body = jsonData;
            if (insertFormat === 'JSONEachRow') reqOptions.headers['Content-Type'] = 'application/json';
            else reqOptions.headers['Content-Type'] = 'text/plain; charset=utf-8';

            if (this.options.debug) console.log(`--- CH DEBUG [${new Date().toISOString()}] ---> Calling request() for INSERT Query Type: ${queryParams?.query_sql?.substring(0, 30)}... URL: ${reqOptions?.url}`);
            request(reqOptions, (error, response, body) => {
                 if (this.options.debug) {
                     console.log(`--- CH DEBUG [${new Date().toISOString()}] <--- Response Received for INSERT Query Type: ${queryParams?.query_sql?.substring(0, 30)}... Status: ${response?.statusCode}`);
                     if (response?.statusCode !== 200) {
                          console.error(`--- CH DEBUG [${new Date().toISOString()}] !!! INSERT ERROR Body: ${body?.substring(0, 500)}`);
                     }
                 }
                 if (error) {
                      console.error(`ClickHouse Driver: INSERT REQUEST ERROR for URL: ${reqOptions?.url}`, error);
                      return reject(error);
                 }
                 if (response.statusCode !== 200) {
                      console.error(`ClickHouse Driver: INSERT failed with status ${response.statusCode}: ${body?.substring(0, 1000)}`);
                      return reject(new Error(`ClickHouse INSERT failed with status ${response.statusCode}: ${body}`));
                 }
                 resolve({ success: true, body: body });
            });
        }).then(result => {
            if (cb) cb(null, result); return result;
        }).catch(err => {
            if (cb) cb(err); throw err;
        });
    }

    ping (cb) {
       // Ensure ping uses instance gzip setting
       const pingOptions = { query_sql: 'SELECT 1', format: 'JSON', isUseGzip: this.options.isUseGzip };
        const req = this._getHttpRequest(pingOptions, (err, res) => {
            if (err) return cb(err);
            let body = '';
            res.on('data', chunk => body += chunk);
            res.on('error', readErr => cb(readErr));
            res.on('end', () => {
                 if (res.statusCode !== 200) { return cb(new Error(`Ping failed with status ${res.statusCode}: ${body}`)); }
                 return cb(null, true);
            });
        });
        if (req) {
            req.on('error', initErr => { if (cb) cb(initErr); else console.error("ClickHouse Ping Error (no callback):", initErr); });
            req.end();
        }
    }

    _getHttpRequestOptions (query, queryParams, forcePost = false) {
        // --- This function prepares options ---
        const queryOpts = _.cloneDeep(this.options);
        _.merge(queryOpts, _.omit(queryParams, ['query_sql']));

        if (queryOpts.isSessionPerQuery || !queryOpts.config.session_id) {
             queryOpts.config.session_id = uuid.v4();
        }

        const format = getFullFormatName(queryOpts.format);
        // Ensure enable_http_compression reflects the CURRENT query's isUseGzip setting
        queryOpts.config.enable_http_compression = queryOpts.isUseGzip ? 1 : 0;
        const finalQueryObj = { ...queryOpts.query, ...queryOpts.config };

        if (queryOpts.params) {
            for (const key in queryOpts.params) { finalQueryObj[`param_${key}`] = queryOpts.params[key]; }
        }

        const reqMethod = forcePost || isPostQuery(query, queryOpts.usePost) ? 'POST' : 'GET';
        const reqUrl = { protocol: queryOpts.protocol, hostname: queryOpts.host, port: queryOpts.port, pathname: queryOpts.pathname, query: finalQueryObj };
        const reqOptions = {
            url: url.format(reqUrl), method: reqMethod, encoding: null, // <<< IMPORTANT: Set encoding to null for binary Gzip data
            // gzip: queryOpts.isUseGzip, // <<< REMOVE: Let request handle request compression, we handle response decompression
            agent: queryOpts.protocol === 'https:' ? httpsAgent : httpAgent,
            timeout: queryOpts.requestTimeout,
            ..._.omit(queryOpts.reqParams, ['headers', 'qs', 'body', 'auth', 'gzip']), // Omit gzip here too
            headers: {
                 'Accept': 'application/json', // Still accept JSON preferably
                 'Accept-Encoding': 'gzip, deflate', // <<< ADD: Explicitly accept gzip/deflate
                 ...(queryOpts.reqParams?.headers || {}),
             },
        };

        if (reqMethod === 'POST') {
            reqOptions.body = query;
             if (!reqOptions.headers['Content-Type']) { reqOptions.headers['Content-Type'] = 'text/plain; charset=utf-8'; }
        } else {
            reqUrl.query.query = query; reqOptions.url = url.format(reqUrl);
        }
        if (queryOpts.basicAuth && queryOpts.basicAuth.username) {
            reqOptions.auth = { user: queryOpts.basicAuth.username, pass: queryOpts.basicAuth.password, sendImmediately: true };
        }

        // --- CORRECTED FORMAT LOGIC ---
        if (!queryOpts.raw) {
            reqUrl.query.default_format = format;
            reqOptions.url = url.format(reqUrl);
        }
        // --- END CORRECTED FORMAT LOGIC ---

        return reqOptions;
    }

    _getHttpRequest (queryParams, cb) {
        if (this.options.debug) console.log(`--- CH DEBUG [${new Date().toISOString()}] _getHttpRequest called for Query Type: ${queryParams?.query_sql?.substring(0, 30)}...`);

        let reqOptions;
        try {
             const sqlQuery = queryParams?.query_sql;
             if (!sqlQuery) throw new Error("_getHttpRequest requires 'query_sql' in queryParams");
             // Pass isUseGzip setting down from queryParams
             reqOptions = this._getHttpRequestOptions(sqlQuery, queryParams);
        } catch (err) {
             console.error(`ClickHouse Driver: Error in _getHttpRequestOptions for Query Type: ${queryParams?.query_sql?.substring(0, 30)}...`, err);
             if (cb) process.nextTick(() => cb(err));
             else throw err;
             return null;
        }

        try {
             if (this.options.debug) console.log(`--- CH DEBUG [${new Date().toISOString()}] ---> Calling request() for Query Type: ${queryParams?.query_sql?.substring(0, 30)}... URL: ${reqOptions?.url}`);
             const req = request(reqOptions); // request library instance

             req.on('error', (err) => {
                 console.error(`ClickHouse Driver: REQUEST ERROR for URL: ${reqOptions?.url}`, err);
                 if (cb) cb(err);
             });

             req.on('response', (response) => {
                 if (this.options.debug) console.log(`--- CH DEBUG [${new Date().toISOString()}] <--- Response Received for Query Type: ${queryParams?.query_sql?.substring(0, 30)}... Status: ${response.statusCode}, Content-Type: ${response.headers['content-type']}, Content-Encoding: ${response.headers['content-encoding']}`); // Log encoding

                 if (cb) {
                     // Callback mode (less common for query, used by ping/insert)
                     let responseStream = response; // Start with the raw response stream

                     // --- GZIP HANDLING FOR CALLBACK ---
                     const encoding = response.headers['content-encoding'];
                     if (encoding === 'gzip' || encoding === 'deflate') {
                         if (this.options.debug) console.log(`--- CH DEBUG [${new Date().toISOString()}] Decompressing (Callback)...`);
                         const decompressor = encoding === 'gzip' ? zlib.createGunzip() : zlib.createInflate();
                         response.pipe(decompressor);
                         responseStream = decompressor; // Use the decompressed stream
                         // Handle decompression errors
                         decompressor.on('error', (zerr) => {
                             console.error(`ClickHouse Driver: Decompression Error (Callback) for Query Type: ${queryParams?.query_sql?.substring(0, 30)}...`, zerr);
                             if(cb) cb(zerr);
                             cb = null; // Prevent further calls
                         });
                     }
                     // --- END GZIP HANDLING FOR CALLBACK ---

                     let bodyChunks = [];
                     responseStream.on('data', (chunk) => bodyChunks.push(chunk)); // Read from potentially decompressed stream
                     responseStream.on('end', () => {
                         if (!cb) return; // Callback already called due to error?
                         const rawBody = Buffer.concat(bodyChunks).toString('utf8');
                         if (this.options.debug) console.log(`--- CH DEBUG [${new Date().toISOString()}] --- Final Body (Callback, Status ${response.statusCode}): ${rawBody.substring(0, 500)}${rawBody.length > 500 ? '...' : ''}`);
                         response.body = rawBody;
                         cb(null, response);
                     });
                     responseStream.on('error', (readErr) => {
                          console.error(`ClickHouse Driver: Response Stream Error (Callback) for Query Type: ${queryParams?.query_sql?.substring(0, 30)}...`, readErr);
                         if(cb) cb(readErr);
                         cb = null;
                     });
                 }
                 // If no callback, QueryCursor handles piping
             });

             return req; // Return the request object

        } catch (syncError) {
             console.error(`ClickHouse Driver: Synchronous request() Error for Query Type: ${queryParams?.query_sql?.substring(0, 30)}...`, syncError);
             if (cb) process.nextTick(() => cb(syncError));
             else throw syncError;
             return null;
        }
    }

} // End Class ClickHouse

// QueryCursor class (with Gzip handling in _setupParser)
class QueryCursor extends stream.Readable {
    constructor (ch, query, options) {
        // <<< CH DEBUG LOG >>> Log entry into QueryCursor constructor
        if (ch.options.debug) console.log(`--- CH DEBUG [${new Date().toISOString()}] QueryCursor CONSTRUCTOR called for Query Type: ${query?.substring(0, 30)}...`);

        super({ objectMode: true, highWaterMark: 16 });
        this.ch = ch; this.query = query; this.options = options || {};
        this.format = getFullFormatName(options.format || ch.options.format);
        this.raw = options.raw !== undefined ? options.raw : ch.options.raw;
        // <<< Get isUseGzip setting for this specific query cursor >>>
        this.isUseGzip = options.isUseGzip !== undefined ? options.isUseGzip : ch.options.isUseGzip;

        this._rs = null; this._parser = null; this._reading = false;
        this._destroyed = false; this._responseHandled = false;
    }

    _read(size) {
        if (this._reading || this._destroyed) return;
        this._reading = true;
        if (this._parser && typeof this._parser.resume === 'function' && this._parser.isPaused && this._parser.isPaused()) { this._parser.resume(); }
        else if (this._rs && typeof this._rs.resume === 'function' && this._rs.isPaused && this._rs.isPaused()) { this._rs.resume(); }
        else if (!this._rs) { try { this._initStream(); } catch(err) { process.nextTick(() => this._handleError(err)); this._reading = false; return; } }
        this._reading = false;
    }

    _initStream () {
        if (this._rs || this._destroyed) return;

        const queryParams = {
            ...this.options, format: this.format, raw: this.raw, query_sql: this.query,
            config: { ...this.ch.options.config, ...(this.options.config || {}) },
            params: this.options.params,
            isUseGzip: this.isUseGzip // <<< Pass Gzip setting for this query
        };
        this._rs = this.ch._getHttpRequest(queryParams);
        if (!this._rs) { throw new Error("Failed to create HTTP request stream."); }

        this._rs.on('error', (err) => this._handleError(err));
        this._rs.on('response', (response) => {
             if (this._responseHandled || this._destroyed) return;
             this._responseHandled = true;
             this._handleResponse(response);
        });
    }

    _handleResponse(response) {
        if (this._destroyed) { if (response?.destroy) response.destroy(); return; }

        if (response.statusCode !== 200) {
            let body = '';
            response.on('data', chunk => body += chunk);
            response.on('error', readErr => this._handleError(readErr));
            response.on('end', () => {
                const error = new Error(`ClickHouse query failed with status ${response.statusCode}: ${body}`);
                error.code = response.statusCode;
                console.error(`ClickHouse Driver: QueryCursor non-200 response body: ${body?.substring(0, 1000)}`);
                this._handleError(error);
            });
            return;
        }
        // Status is 200, proceed to setup parser, passing the response
        this._setupParser(response);
    }

    // <<< MODIFIED _setupParser >>>
    _setupParser(responseStream) {
        if (this._destroyed) { if (responseStream?.destroy) responseStream.destroy(); return; }

        let inputStream = responseStream; // Start with the original response stream

        // --- GZIP DECOMPRESSION LOGIC ---
        const encoding = responseStream.headers['content-encoding'];
        if (!this.raw && (encoding === 'gzip' || encoding === 'deflate')) {
             if (this.ch.options.debug) console.log(`--- CH DEBUG [${new Date().toISOString()}] QueryCursor decompressing stream (${encoding})...`);
             const decompressor = encoding === 'gzip' ? zlib.createGunzip() : zlib.createInflate();

             // Handle errors on the decompressor itself
             decompressor.on('error', (zerr) => {
                  console.error(`ClickHouse Driver: QueryCursor Decompression Error (${encoding})`, zerr);
                  this._handleError(new Error(`Decompression Error: ${zerr.message}`));
             });

             // Pipe the original response into the decompressor
             responseStream.pipe(decompressor);
             // The stream we will read/parse from is now the decompressor
             inputStream = decompressor;

             // Handle errors on the *original* response stream as well (e.g., if connection drops)
             responseStream.on('error', (resErr) => {
                 if (this.ch.options.debug) console.error(`--- CH DEBUG [${new Date().toISOString()}] QueryCursor Original Response Stream Error (during decompression):`, resErr);
                 // If the decompressor hasn't already errored, forward this error
                 if (!decompressor.destroyed) {
                     this._handleError(resErr);
                 }
             });

        } else if (this.raw && (encoding === 'gzip' || encoding === 'deflate')) {
            if (this.ch.options.debug) console.log(`--- CH DEBUG [${new Date().toISOString()}] QueryCursor RAW mode, received compressed stream (${encoding}) - passing through.`);
            // In raw mode, we pass the compressed stream directly
        } else {
             if (this.ch.options.debug) console.log(`--- CH DEBUG [${new Date().toISOString()}] QueryCursor received uncompressed stream or in RAW mode.`);
            // If not compressed or in raw mode, handle errors directly on the response stream
             responseStream.on('error', (err) => this._handleError(err));
        }
        // --- END GZIP DECOMPRESSION LOGIC ---


        // --- PARSING LOGIC (uses inputStream) ---
         if (this.raw) {
             // if (this.ch.options.debug) console.log("--- CH DEBUG [...] Cursor piping RAW stream ---");
             inputStream.on('data', (chunk) => {
                  if (!this.push(chunk)) { if (inputStream.pause) inputStream.pause(); }
             });
             inputStream.on('end', () => { this.push(null); });
             // Error handled above based on whether it's compressed or not
         } else if (this.format === 'JSON') {
              // if (this.ch.options.debug) console.log("--- CH DEBUG [...] Cursor using JSONStream path ['data', true] ---");
              this._parser = JSONStream.parse(['data', true]);
              this._parser.on('data', (row) => {
                   if (this.ch.options.debug) console.log(`--- CH DEBUG [${new Date().toISOString()}] --- JSON Emitted: ${JSON.stringify(row).substring(0, 500)}${JSON.stringify(row).length > 500 ? '...' : ''}`);
                   if (!this.push(row)) { if (inputStream.pause) inputStream.pause(); } // Pause the input stream
              });
              this._parser.on('error', (err) => {
                  console.error(`ClickHouse Driver: JSON PARSER ERROR`, err);
                  this._handleError(new Error(`JSON Parsing Error: ${err.message}`));
              });
              this._parser.on('end', () => { this.push(null); });
              inputStream.pipe(this._parser); // Pipe the (potentially decompressed) stream to parser

         } else if (this.format === 'TabSeparated') {
              // if (this.ch.options.debug) console.log(`--- CH DEBUG [...] Cursor using TSV parser ---`);
              this._parser = tsv.parseStream({});
              this._parser.on('data', (row) => {
                   if (!this.push(row)) { if (inputStream.pause) inputStream.pause(); } // Pause the input stream
              });
              this._parser.on('error', (err) => {
                   console.error(`ClickHouse Driver: TSV PARSER ERROR`, err);
                   this._handleError(new Error(`TSV Parsing Error: ${err.message}`));
              });
              this._parser.on('end', () => { this.push(null); });
              inputStream.pipe(this._parser); // Pipe the (potentially decompressed) stream to parser

         } else {
              console.warn(`ClickHouse Driver: Unsupported format '${this.format}' for parsing, falling back to raw stream.`);
              this.raw = true;
              // Re-call setupParser - it will now enter the raw=true block
              // Need to ensure the original response stream is used here
              this._setupParser(responseStream);
         }
    }

    _handleError (err) {
        if (this._destroyed) return;
        this._destroyed = true;
        // Log critical errors handled by cursor
        console.error(`ClickHouse Driver: QueryCursor Error`, err);
        if (this.ch.options.debug) {
            console.error("--- CH DEBUG --- Error Stack:", err?.stack);
            console.error("--- CH DEBUG --- Query:", this.query?.substring(0, 500));
            console.error("-------------------------------------------------------");
        }
        this._cleanupStreams();
        process.nextTick(() => { if (!this.destroyed) this.emit('error', err); });
    }

    _cleanupStreams() {
        // if (this.ch.options.debug) console.log(`--- CH DEBUG [${new Date().toISOString()}] QueryCursor _cleanupStreams ---`);
        // Cleanup parser and request stream
        if (this._parser) { try { if (this._parser.unpipe) this._parser.unpipe(); if (this._parser.destroy) this._parser.destroy(); this._parser.removeAllListeners(); } catch(e){} this._parser = null; }
        if (this._rs) { try { if (this._rs.unpipe) this._rs.unpipe(); if (this._rs.abort) this._rs.abort(); else if (this._rs.destroy) this._rs.destroy(); this._rs.removeAllListeners(); } catch(e){} this._rs = null; }
    }

    destroy(err) {
        if (this._destroyed) return;
        this._destroyed = true;
        // if (this.ch.options.debug) console.log(`--- CH DEBUG [${new Date().toISOString()}] QueryCursor explicit destroy(${err ? err.message : ''}) ---`);
        this._cleanupStreams();
        super.destroy(err);
    }

    // Public Methods (exec, toPromise, stream, asyncIterator - kept as before)
    exec (cb) { const result = []; let errorOccurred = null; let callback = cb; const onData = row => result.push(row); const onError = err => { errorOccurred = err; if (callback) { const cb = callback; callback = null; cb(err); } }; const onEnd = () => { if (callback) { const cb = callback; callback = null; cb(null, result); } }; this.on('data', onData); this.once('error', onError); this.once('end', onEnd); this.once('close', () => { if (callback && !errorOccurred) { callback(new Error("Stream closed prematurely")); } callback = null; }); }
    toPromise () { return new Promise((resolve, reject) => { const result = []; const onError = (err) => { this.removeListener('data', onData); this.removeListener('end', onEnd); reject(err); }; const onData = (row) => result.push(row); const onEnd = () => { this.removeListener('data', onData); this.removeListener('error', onError); resolve(result); }; this.on('data', onData); this.once('error', onError); this.once('end', onEnd); if (!this._rs && !this._destroyed) { try { this._initStream(); } catch(err) { return reject(err); } } else if (this.isPaused()) { this.resume(); } }); }
    stream () { if (!this._rs && !this._destroyed) { try { this._initStream(); } catch (e) { process.nextTick(()=>this.emit('error',e)); } } return this; }
    [Symbol.asyncIterator]() { if (!this._rs && !this._destroyed) { try { this._initStream(); } catch (e) { process.nextTick(()=>this.emit('error',e)); } } return StreamIter(this); }

} // End Class QueryCursor

// Modified Export
module.exports = {
    ClickHouse: ClickHouse,
    default: ClickHouse
};
