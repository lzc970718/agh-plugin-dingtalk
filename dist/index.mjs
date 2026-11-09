import { createRequire as __createRequire } from 'node:module'; const require = __createRequire(import.meta.url);
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __require = /* @__PURE__ */ ((x) => typeof require !== "undefined" ? require : typeof Proxy !== "undefined" ? new Proxy(x, {
  get: (a, b) => (typeof require !== "undefined" ? require : a)[b]
}) : x)(function(x) {
  if (typeof require !== "undefined") return require.apply(this, arguments);
  throw Error('Dynamic require of "' + x + '" is not supported');
});
var __esm = (fn, res, err) => function __init() {
  if (err) throw err[0];
  try {
    return fn && (res = (0, fn[__getOwnPropNames(fn)[0]])(fn = 0)), res;
  } catch (e) {
    throw err = [e], e;
  }
};
var __commonJS = (cb, mod) => function __require2() {
  try {
    return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
  } catch (e) {
    throw mod = 0, e;
  }
};
var __export = (target, all3) => {
  for (var name in all3)
    __defProp(target, name, { get: all3[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __reExport = (target, mod, secondTarget) => (__copyProps(target, mod, "default"), secondTarget && __copyProps(secondTarget, mod, "default"));
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));

// src/errors.ts
var CHANNEL_ERRORS, ChannelError;
var init_errors = __esm({
  "src/errors.ts"() {
    "use strict";
    CHANNEL_ERRORS = Object.freeze([
      "E_MANIFEST_INVALID",
      "E_CONFIG_INVALID",
      "E_SECRETS_UNREADABLE",
      "E_CONNECT_FAILED",
      "E_DAEMON_UNAVAILABLE",
      "E_CAPABILITY_MISSING",
      "E_NOT_IMPLEMENTED",
      "E_STATE_STALE"
    ]);
    ChannelError = class extends Error {
      code;
      detail;
      constructor(code, message, detail) {
        super(`${code}: ${message}`);
        this.name = "ChannelError";
        this.code = code;
        this.detail = detail;
      }
    };
  }
});

// node_modules/dingtalk-stream/dist/constants.mjs
var GATEWAY_URL, GET_TOKEN_URL, TOPIC_ROBOT, TOPIC_CARD, TOPIC_AI_GRAPH_API;
var init_constants = __esm({
  "node_modules/dingtalk-stream/dist/constants.mjs"() {
    GATEWAY_URL = "https://api.dingtalk.com/v1.0/gateway/connections/open";
    GET_TOKEN_URL = "https://oapi.dingtalk.com/gettoken";
    TOPIC_ROBOT = "/v1.0/im/bot/messages/get";
    TOPIC_CARD = "/v1.0/card/instances/callback";
    TOPIC_AI_GRAPH_API = "/v1.0/graph/api/invoke";
  }
});

// node_modules/ws/lib/constants.js
var require_constants = __commonJS({
  "node_modules/ws/lib/constants.js"(exports, module) {
    "use strict";
    var BINARY_TYPES = ["nodebuffer", "arraybuffer", "fragments"];
    var hasBlob = typeof Blob !== "undefined";
    if (hasBlob) BINARY_TYPES.push("blob");
    module.exports = {
      BINARY_TYPES,
      CLOSE_TIMEOUT: 3e4,
      EMPTY_BUFFER: Buffer.alloc(0),
      GUID: "258EAFA5-E914-47DA-95CA-C5AB0DC85B11",
      hasBlob,
      kForOnEventAttribute: /* @__PURE__ */ Symbol("kIsForOnEventAttribute"),
      kListener: /* @__PURE__ */ Symbol("kListener"),
      kStatusCode: /* @__PURE__ */ Symbol("status-code"),
      kWebSocket: /* @__PURE__ */ Symbol("websocket"),
      NOOP: () => {
      }
    };
  }
});

// node_modules/ws/lib/buffer-util.js
var require_buffer_util = __commonJS({
  "node_modules/ws/lib/buffer-util.js"(exports, module) {
    "use strict";
    var { websocketMask, websocketUnmask } = __require("http");
    var { EMPTY_BUFFER } = require_constants();
    var FastBuffer = Buffer[Symbol.species];
    function concat(list, totalLength) {
      if (list.length === 0) return EMPTY_BUFFER;
      if (list.length === 1) return list[0];
      const target = Buffer.allocUnsafe(totalLength);
      let offset = 0;
      for (let i = 0; i < list.length; i++) {
        const buf = list[i];
        target.set(buf, offset);
        offset += buf.length;
      }
      if (offset < totalLength) {
        return new FastBuffer(target.buffer, target.byteOffset, offset);
      }
      return target;
    }
    function _mask(source, mask, output, offset, length) {
      for (let i = 0; i < length; i++) {
        output[offset + i] = source[i] ^ mask[i & 3];
      }
    }
    function _unmask(buffer, mask) {
      for (let i = 0; i < buffer.length; i++) {
        buffer[i] ^= mask[i & 3];
      }
    }
    function toArrayBuffer(buf) {
      if (buf.length === buf.buffer.byteLength) {
        return buf.buffer;
      }
      return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.length);
    }
    function toBuffer(data) {
      toBuffer.readOnly = true;
      if (Buffer.isBuffer(data)) return data;
      let buf;
      if (data instanceof ArrayBuffer) {
        buf = new FastBuffer(data);
      } else if (ArrayBuffer.isView(data)) {
        buf = new FastBuffer(data.buffer, data.byteOffset, data.byteLength);
      } else {
        buf = Buffer.from(data);
        toBuffer.readOnly = false;
      }
      return buf;
    }
    module.exports = {
      concat,
      mask: _mask,
      toArrayBuffer,
      toBuffer,
      unmask: _unmask
    };
    if (websocketMask) {
      module.exports.mask = function(source, mask, output, offset, length) {
        if (length < 24) _mask(source, mask, output, offset, length);
        else websocketMask(source, mask, output, offset, length);
      };
      module.exports.unmask = function(buffer, mask) {
        if (buffer.length < 24) _unmask(buffer, mask);
        else websocketUnmask(buffer, mask);
      };
    } else if (!process.env.WS_NO_BUFFER_UTIL) {
      try {
        const bufferUtil = __require("bufferutil");
        module.exports.mask = function(source, mask, output, offset, length) {
          if (length < 48) _mask(source, mask, output, offset, length);
          else bufferUtil.mask(source, mask, output, offset, length);
        };
        module.exports.unmask = function(buffer, mask) {
          if (buffer.length < 32) _unmask(buffer, mask);
          else bufferUtil.unmask(buffer, mask);
        };
      } catch (e) {
      }
    }
  }
});

// node_modules/ws/lib/limiter.js
var require_limiter = __commonJS({
  "node_modules/ws/lib/limiter.js"(exports, module) {
    "use strict";
    var kDone = /* @__PURE__ */ Symbol("kDone");
    var kRun = /* @__PURE__ */ Symbol("kRun");
    var Limiter = class {
      /**
       * Creates a new `Limiter`.
       *
       * @param {Number} [concurrency=Infinity] The maximum number of jobs allowed
       *     to run concurrently
       */
      constructor(concurrency) {
        this[kDone] = () => {
          this.pending--;
          this[kRun]();
        };
        this.concurrency = concurrency || Infinity;
        this.jobs = [];
        this.pending = 0;
      }
      /**
       * Adds a job to the queue.
       *
       * @param {Function} job The job to run
       * @public
       */
      add(job) {
        this.jobs.push(job);
        this[kRun]();
      }
      /**
       * Removes a job from the queue and runs it if possible.
       *
       * @private
       */
      [kRun]() {
        if (this.pending === this.concurrency) return;
        if (this.jobs.length) {
          const job = this.jobs.shift();
          this.pending++;
          job(this[kDone]);
        }
      }
    };
    module.exports = Limiter;
  }
});

// node_modules/ws/lib/permessage-deflate.js
var require_permessage_deflate = __commonJS({
  "node_modules/ws/lib/permessage-deflate.js"(exports, module) {
    "use strict";
    var zlib2 = __require("zlib");
    var bufferUtil = require_buffer_util();
    var Limiter = require_limiter();
    var { kStatusCode } = require_constants();
    var FastBuffer = Buffer[Symbol.species];
    var TRAILER = Buffer.from([0, 0, 255, 255]);
    var kPerMessageDeflate = /* @__PURE__ */ Symbol("permessage-deflate");
    var kTotalLength = /* @__PURE__ */ Symbol("total-length");
    var kCallback = /* @__PURE__ */ Symbol("callback");
    var kBuffers = /* @__PURE__ */ Symbol("buffers");
    var kError = /* @__PURE__ */ Symbol("error");
    var zlibLimiter;
    var PerMessageDeflate2 = class {
      /**
       * Creates a PerMessageDeflate instance.
       *
       * @param {Object} [options] Configuration options
       * @param {(Boolean|Number)} [options.clientMaxWindowBits] Advertise support
       *     for, or request, a custom client window size
       * @param {Boolean} [options.clientNoContextTakeover=false] Advertise/
       *     acknowledge disabling of client context takeover
       * @param {Number} [options.concurrencyLimit=10] The number of concurrent
       *     calls to zlib
       * @param {Boolean} [options.isServer=false] Create the instance in either
       *     server or client mode
       * @param {Number} [options.maxPayload=0] The maximum allowed message length
       * @param {(Boolean|Number)} [options.serverMaxWindowBits] Request/confirm the
       *     use of a custom server window size
       * @param {Boolean} [options.serverNoContextTakeover=false] Request/accept
       *     disabling of server context takeover
       * @param {Number} [options.threshold=1024] Size (in bytes) below which
       *     messages should not be compressed if context takeover is disabled
       * @param {Object} [options.zlibDeflateOptions] Options to pass to zlib on
       *     deflate
       * @param {Object} [options.zlibInflateOptions] Options to pass to zlib on
       *     inflate
       */
      constructor(options) {
        this._options = options || {};
        this._threshold = this._options.threshold !== void 0 ? this._options.threshold : 1024;
        this._maxPayload = this._options.maxPayload | 0;
        this._isServer = !!this._options.isServer;
        this._deflate = null;
        this._inflate = null;
        this.params = null;
        if (!zlibLimiter) {
          const concurrency = this._options.concurrencyLimit !== void 0 ? this._options.concurrencyLimit : 10;
          zlibLimiter = new Limiter(concurrency);
        }
      }
      /**
       * @type {String}
       */
      static get extensionName() {
        return "permessage-deflate";
      }
      /**
       * Create an extension negotiation offer.
       *
       * @return {Object} Extension parameters
       * @public
       */
      offer() {
        const params = {};
        if (this._options.serverNoContextTakeover) {
          params.server_no_context_takeover = true;
        }
        if (this._options.clientNoContextTakeover) {
          params.client_no_context_takeover = true;
        }
        if (this._options.serverMaxWindowBits) {
          params.server_max_window_bits = this._options.serverMaxWindowBits;
        }
        if (this._options.clientMaxWindowBits) {
          params.client_max_window_bits = this._options.clientMaxWindowBits;
        } else if (this._options.clientMaxWindowBits == null) {
          params.client_max_window_bits = true;
        }
        return params;
      }
      /**
       * Accept an extension negotiation offer/response.
       *
       * @param {Array} configurations The extension negotiation offers/reponse
       * @return {Object} Accepted configuration
       * @public
       */
      accept(configurations) {
        configurations = this.normalizeParams(configurations);
        this.params = this._isServer ? this.acceptAsServer(configurations) : this.acceptAsClient(configurations);
        return this.params;
      }
      /**
       * Releases all resources used by the extension.
       *
       * @public
       */
      cleanup() {
        if (this._inflate) {
          this._inflate.close();
          this._inflate = null;
        }
        if (this._deflate) {
          const callback = this._deflate[kCallback];
          this._deflate.close();
          this._deflate = null;
          if (callback) {
            callback(
              new Error(
                "The deflate stream was closed while data was being processed"
              )
            );
          }
        }
      }
      /**
       *  Accept an extension negotiation offer.
       *
       * @param {Array} offers The extension negotiation offers
       * @return {Object} Accepted configuration
       * @private
       */
      acceptAsServer(offers) {
        const opts = this._options;
        const accepted = offers.find((params) => {
          if (opts.serverNoContextTakeover === false && params.server_no_context_takeover || params.server_max_window_bits && (opts.serverMaxWindowBits === false || typeof opts.serverMaxWindowBits === "number" && opts.serverMaxWindowBits > params.server_max_window_bits) || typeof opts.clientMaxWindowBits === "number" && (typeof params.client_max_window_bits === "number" ? opts.clientMaxWindowBits > params.client_max_window_bits : !params.client_max_window_bits)) {
            return false;
          }
          return true;
        });
        if (!accepted) {
          throw new Error("None of the extension offers can be accepted");
        }
        if (opts.serverNoContextTakeover) {
          accepted.server_no_context_takeover = true;
        }
        if (opts.clientNoContextTakeover) {
          accepted.client_no_context_takeover = true;
        }
        if (typeof opts.serverMaxWindowBits === "number") {
          accepted.server_max_window_bits = opts.serverMaxWindowBits;
        }
        if (typeof opts.clientMaxWindowBits === "number") {
          accepted.client_max_window_bits = opts.clientMaxWindowBits;
        } else if (accepted.client_max_window_bits === true || opts.clientMaxWindowBits === false) {
          delete accepted.client_max_window_bits;
        }
        return accepted;
      }
      /**
       * Accept the extension negotiation response.
       *
       * @param {Array} response The extension negotiation response
       * @return {Object} Accepted configuration
       * @private
       */
      acceptAsClient(response) {
        const params = response[0];
        if (this._options.clientNoContextTakeover === false && params.client_no_context_takeover) {
          throw new Error('Unexpected parameter "client_no_context_takeover"');
        }
        if (!params.client_max_window_bits) {
          if (typeof this._options.clientMaxWindowBits === "number") {
            params.client_max_window_bits = this._options.clientMaxWindowBits;
          }
        } else if (this._options.clientMaxWindowBits === false || typeof this._options.clientMaxWindowBits === "number" && params.client_max_window_bits > this._options.clientMaxWindowBits) {
          throw new Error(
            'Unexpected or invalid parameter "client_max_window_bits"'
          );
        }
        return params;
      }
      /**
       * Normalize parameters.
       *
       * @param {Array} configurations The extension negotiation offers/reponse
       * @return {Array} The offers/response with normalized parameters
       * @private
       */
      normalizeParams(configurations) {
        configurations.forEach((params) => {
          Object.keys(params).forEach((key) => {
            let value = params[key];
            if (value.length > 1) {
              throw new Error(`Parameter "${key}" must have only a single value`);
            }
            value = value[0];
            if (key === "client_max_window_bits") {
              if (value !== true) {
                const num = +value;
                if (!Number.isInteger(num) || num < 8 || num > 15) {
                  throw new TypeError(
                    `Invalid value for parameter "${key}": ${value}`
                  );
                }
                value = num;
              } else if (!this._isServer) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
            } else if (key === "server_max_window_bits") {
              const num = +value;
              if (!Number.isInteger(num) || num < 8 || num > 15) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
              value = num;
            } else if (key === "client_no_context_takeover" || key === "server_no_context_takeover") {
              if (value !== true) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
            } else {
              throw new Error(`Unknown parameter "${key}"`);
            }
            params[key] = value;
          });
        });
        return configurations;
      }
      /**
       * Decompress data. Concurrency limited.
       *
       * @param {Buffer} data Compressed data
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @public
       */
      decompress(data, fin, callback) {
        zlibLimiter.add((done) => {
          this._decompress(data, fin, (err, result) => {
            done();
            callback(err, result);
          });
        });
      }
      /**
       * Compress data. Concurrency limited.
       *
       * @param {(Buffer|String)} data Data to compress
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @public
       */
      compress(data, fin, callback) {
        zlibLimiter.add((done) => {
          this._compress(data, fin, (err, result) => {
            done();
            callback(err, result);
          });
        });
      }
      /**
       * Decompress data.
       *
       * @param {Buffer} data Compressed data
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @private
       */
      _decompress(data, fin, callback) {
        const endpoint = this._isServer ? "client" : "server";
        if (!this._inflate) {
          const key = `${endpoint}_max_window_bits`;
          const windowBits = typeof this.params[key] !== "number" ? zlib2.Z_DEFAULT_WINDOWBITS : this.params[key];
          this._inflate = zlib2.createInflateRaw({
            ...this._options.zlibInflateOptions,
            windowBits
          });
          this._inflate[kPerMessageDeflate] = this;
          this._inflate[kTotalLength] = 0;
          this._inflate[kBuffers] = [];
          this._inflate.on("error", inflateOnError);
          this._inflate.on("data", inflateOnData);
        }
        this._inflate[kCallback] = callback;
        this._inflate.write(data);
        if (fin) this._inflate.write(TRAILER);
        this._inflate.flush(() => {
          const err = this._inflate[kError];
          if (err) {
            this._inflate.close();
            this._inflate = null;
            callback(err);
            return;
          }
          const data2 = bufferUtil.concat(
            this._inflate[kBuffers],
            this._inflate[kTotalLength]
          );
          if (this._inflate._readableState.endEmitted) {
            this._inflate.close();
            this._inflate = null;
          } else {
            this._inflate[kTotalLength] = 0;
            this._inflate[kBuffers] = [];
            if (fin && this.params[`${endpoint}_no_context_takeover`]) {
              this._inflate.reset();
            }
          }
          callback(null, data2);
        });
      }
      /**
       * Compress data.
       *
       * @param {(Buffer|String)} data Data to compress
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @private
       */
      _compress(data, fin, callback) {
        const endpoint = this._isServer ? "server" : "client";
        if (!this._deflate) {
          const key = `${endpoint}_max_window_bits`;
          const windowBits = typeof this.params[key] !== "number" ? zlib2.Z_DEFAULT_WINDOWBITS : this.params[key];
          this._deflate = zlib2.createDeflateRaw({
            ...this._options.zlibDeflateOptions,
            windowBits
          });
          this._deflate[kTotalLength] = 0;
          this._deflate[kBuffers] = [];
          this._deflate.on("data", deflateOnData);
        }
        this._deflate[kCallback] = callback;
        this._deflate.write(data);
        this._deflate.flush(zlib2.Z_SYNC_FLUSH, () => {
          if (!this._deflate) {
            return;
          }
          let data2 = bufferUtil.concat(
            this._deflate[kBuffers],
            this._deflate[kTotalLength]
          );
          if (fin) {
            data2 = new FastBuffer(data2.buffer, data2.byteOffset, data2.length - 4);
          }
          this._deflate[kCallback] = null;
          this._deflate[kTotalLength] = 0;
          this._deflate[kBuffers] = [];
          if (fin && this.params[`${endpoint}_no_context_takeover`]) {
            this._deflate.reset();
          }
          callback(null, data2);
        });
      }
    };
    module.exports = PerMessageDeflate2;
    function deflateOnData(chunk) {
      this[kBuffers].push(chunk);
      this[kTotalLength] += chunk.length;
    }
    function inflateOnData(chunk) {
      this[kTotalLength] += chunk.length;
      if (this[kPerMessageDeflate]._maxPayload < 1 || this[kTotalLength] <= this[kPerMessageDeflate]._maxPayload) {
        this[kBuffers].push(chunk);
        return;
      }
      this[kError] = new RangeError("Max payload size exceeded");
      this[kError].code = "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH";
      this[kError][kStatusCode] = 1009;
      this.removeListener("data", inflateOnData);
      this.reset();
    }
    function inflateOnError(err) {
      this[kPerMessageDeflate]._inflate = null;
      if (this[kError]) {
        this[kCallback](this[kError]);
        return;
      }
      err[kStatusCode] = 1007;
      this[kCallback](err);
    }
  }
});

// node_modules/ws/lib/validation.js
var require_validation = __commonJS({
  "node_modules/ws/lib/validation.js"(exports, module) {
    "use strict";
    var { isUtf8 } = __require("buffer");
    var { hasBlob } = require_constants();
    var tokenChars = [
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // 0 - 15
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // 16 - 31
      0,
      1,
      0,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      1,
      1,
      0,
      1,
      1,
      0,
      // 32 - 47
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      0,
      0,
      0,
      0,
      // 48 - 63
      0,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      // 64 - 79
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      0,
      1,
      1,
      // 80 - 95
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      // 96 - 111
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      1,
      0,
      1,
      0
      // 112 - 127
    ];
    function isValidStatusCode(code) {
      return code >= 1e3 && code <= 1014 && code !== 1004 && code !== 1005 && code !== 1006 || code >= 3e3 && code <= 4999;
    }
    function _isValidUTF8(buf) {
      const len = buf.length;
      let i = 0;
      while (i < len) {
        if ((buf[i] & 128) === 0) {
          i++;
        } else if ((buf[i] & 224) === 192) {
          if (i + 1 === len || (buf[i + 1] & 192) !== 128 || (buf[i] & 254) === 192) {
            return false;
          }
          i += 2;
        } else if ((buf[i] & 240) === 224) {
          if (i + 2 >= len || (buf[i + 1] & 192) !== 128 || (buf[i + 2] & 192) !== 128 || buf[i] === 224 && (buf[i + 1] & 224) === 128 || // Overlong
          buf[i] === 237 && (buf[i + 1] & 224) === 160) {
            return false;
          }
          i += 3;
        } else if ((buf[i] & 248) === 240) {
          if (i + 3 >= len || (buf[i + 1] & 192) !== 128 || (buf[i + 2] & 192) !== 128 || (buf[i + 3] & 192) !== 128 || buf[i] === 240 && (buf[i + 1] & 240) === 128 || // Overlong
          buf[i] === 244 && buf[i + 1] > 143 || buf[i] > 244) {
            return false;
          }
          i += 4;
        } else {
          return false;
        }
      }
      return true;
    }
    function isBlob2(value) {
      return hasBlob && typeof value === "object" && typeof value.arrayBuffer === "function" && typeof value.type === "string" && typeof value.stream === "function" && (value[Symbol.toStringTag] === "Blob" || value[Symbol.toStringTag] === "File");
    }
    module.exports = {
      isBlob: isBlob2,
      isValidStatusCode,
      isValidUTF8: _isValidUTF8,
      tokenChars
    };
    if (isUtf8) {
      module.exports.isValidUTF8 = function(buf) {
        return buf.length < 24 ? _isValidUTF8(buf) : isUtf8(buf);
      };
    } else if (!process.env.WS_NO_UTF_8_VALIDATE) {
      try {
        const isValidUTF8 = __require("utf-8-validate");
        module.exports.isValidUTF8 = function(buf) {
          return buf.length < 32 ? _isValidUTF8(buf) : isValidUTF8(buf);
        };
      } catch (e) {
      }
    }
  }
});

// node_modules/ws/lib/receiver.js
var require_receiver = __commonJS({
  "node_modules/ws/lib/receiver.js"(exports, module) {
    "use strict";
    var { Writable } = __require("stream");
    var PerMessageDeflate2 = require_permessage_deflate();
    var {
      BINARY_TYPES,
      EMPTY_BUFFER,
      kStatusCode,
      kWebSocket
    } = require_constants();
    var { concat, toArrayBuffer, unmask } = require_buffer_util();
    var { isValidStatusCode, isValidUTF8 } = require_validation();
    var FastBuffer = Buffer[Symbol.species];
    var GET_INFO = 0;
    var GET_PAYLOAD_LENGTH_16 = 1;
    var GET_PAYLOAD_LENGTH_64 = 2;
    var GET_MASK = 3;
    var GET_DATA = 4;
    var INFLATING = 5;
    var DEFER_EVENT = 6;
    var Receiver2 = class extends Writable {
      /**
       * Creates a Receiver instance.
       *
       * @param {Object} [options] Options object
       * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {String} [options.binaryType=nodebuffer] The type for binary data
       * @param {Object} [options.extensions] An object containing the negotiated
       *     extensions
       * @param {Boolean} [options.isServer=false] Specifies whether to operate in
       *     client or server mode
       * @param {Number} [options.maxBufferedChunks=0] The maximum number of
       *     buffered data chunks
       * @param {Number} [options.maxFragments=0] The maximum number of message
       *     fragments
       * @param {Number} [options.maxPayload=0] The maximum allowed message length
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       */
      constructor(options = {}) {
        super();
        this._allowSynchronousEvents = options.allowSynchronousEvents !== void 0 ? options.allowSynchronousEvents : true;
        this._binaryType = options.binaryType || BINARY_TYPES[0];
        this._extensions = options.extensions || {};
        this._isServer = !!options.isServer;
        this._maxBufferedChunks = options.maxBufferedChunks | 0;
        this._maxFragments = options.maxFragments | 0;
        this._maxPayload = options.maxPayload | 0;
        this._skipUTF8Validation = !!options.skipUTF8Validation;
        this[kWebSocket] = void 0;
        this._bufferedBytes = 0;
        this._buffers = [];
        this._compressed = false;
        this._payloadLength = 0;
        this._mask = void 0;
        this._fragmented = 0;
        this._masked = false;
        this._fin = false;
        this._opcode = 0;
        this._totalPayloadLength = 0;
        this._messageLength = 0;
        this._numFragments = 0;
        this._fragments = [];
        this._errored = false;
        this._loop = false;
        this._state = GET_INFO;
      }
      /**
       * Implements `Writable.prototype._write()`.
       *
       * @param {Buffer} chunk The chunk of data to write
       * @param {String} encoding The character encoding of `chunk`
       * @param {Function} cb Callback
       * @private
       */
      _write(chunk, encoding, cb) {
        if (this._opcode === 8 && this._state == GET_INFO) return cb();
        if (this._maxBufferedChunks > 0 && this._buffers.length >= this._maxBufferedChunks) {
          cb(
            this.createError(
              RangeError,
              "Too many buffered chunks",
              false,
              1008,
              "WS_ERR_TOO_MANY_BUFFERED_PARTS"
            )
          );
          return;
        }
        this._bufferedBytes += chunk.length;
        this._buffers.push(chunk);
        this.startLoop(cb);
      }
      /**
       * Consumes `n` bytes from the buffered data.
       *
       * @param {Number} n The number of bytes to consume
       * @return {Buffer} The consumed bytes
       * @private
       */
      consume(n) {
        this._bufferedBytes -= n;
        if (n === this._buffers[0].length) return this._buffers.shift();
        if (n < this._buffers[0].length) {
          const buf = this._buffers[0];
          this._buffers[0] = new FastBuffer(
            buf.buffer,
            buf.byteOffset + n,
            buf.length - n
          );
          return new FastBuffer(buf.buffer, buf.byteOffset, n);
        }
        const dst = Buffer.allocUnsafe(n);
        do {
          const buf = this._buffers[0];
          const offset = dst.length - n;
          if (n >= buf.length) {
            dst.set(this._buffers.shift(), offset);
          } else {
            dst.set(new Uint8Array(buf.buffer, buf.byteOffset, n), offset);
            this._buffers[0] = new FastBuffer(
              buf.buffer,
              buf.byteOffset + n,
              buf.length - n
            );
          }
          n -= buf.length;
        } while (n > 0);
        return dst;
      }
      /**
       * Starts the parsing loop.
       *
       * @param {Function} cb Callback
       * @private
       */
      startLoop(cb) {
        this._loop = true;
        do {
          switch (this._state) {
            case GET_INFO:
              this.getInfo(cb);
              break;
            case GET_PAYLOAD_LENGTH_16:
              this.getPayloadLength16(cb);
              break;
            case GET_PAYLOAD_LENGTH_64:
              this.getPayloadLength64(cb);
              break;
            case GET_MASK:
              this.getMask();
              break;
            case GET_DATA:
              this.getData(cb);
              break;
            case INFLATING:
            case DEFER_EVENT:
              this._loop = false;
              return;
          }
        } while (this._loop);
        if (!this._errored) cb();
      }
      /**
       * Reads the first two bytes of a frame.
       *
       * @param {Function} cb Callback
       * @private
       */
      getInfo(cb) {
        if (this._bufferedBytes < 2) {
          this._loop = false;
          return;
        }
        const buf = this.consume(2);
        if ((buf[0] & 48) !== 0) {
          const error = this.createError(
            RangeError,
            "RSV2 and RSV3 must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_RSV_2_3"
          );
          cb(error);
          return;
        }
        const compressed = (buf[0] & 64) === 64;
        if (compressed && !this._extensions[PerMessageDeflate2.extensionName]) {
          const error = this.createError(
            RangeError,
            "RSV1 must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_RSV_1"
          );
          cb(error);
          return;
        }
        this._fin = (buf[0] & 128) === 128;
        this._opcode = buf[0] & 15;
        this._payloadLength = buf[1] & 127;
        if (this._opcode === 0) {
          if (compressed) {
            const error = this.createError(
              RangeError,
              "RSV1 must be clear",
              true,
              1002,
              "WS_ERR_UNEXPECTED_RSV_1"
            );
            cb(error);
            return;
          }
          if (!this._fragmented) {
            const error = this.createError(
              RangeError,
              "invalid opcode 0",
              true,
              1002,
              "WS_ERR_INVALID_OPCODE"
            );
            cb(error);
            return;
          }
          this._opcode = this._fragmented;
        } else if (this._opcode === 1 || this._opcode === 2) {
          if (this._fragmented) {
            const error = this.createError(
              RangeError,
              `invalid opcode ${this._opcode}`,
              true,
              1002,
              "WS_ERR_INVALID_OPCODE"
            );
            cb(error);
            return;
          }
          this._compressed = compressed;
        } else if (this._opcode > 7 && this._opcode < 11) {
          if (!this._fin) {
            const error = this.createError(
              RangeError,
              "FIN must be set",
              true,
              1002,
              "WS_ERR_EXPECTED_FIN"
            );
            cb(error);
            return;
          }
          if (compressed) {
            const error = this.createError(
              RangeError,
              "RSV1 must be clear",
              true,
              1002,
              "WS_ERR_UNEXPECTED_RSV_1"
            );
            cb(error);
            return;
          }
          if (this._payloadLength > 125 || this._opcode === 8 && this._payloadLength === 1) {
            const error = this.createError(
              RangeError,
              `invalid payload length ${this._payloadLength}`,
              true,
              1002,
              "WS_ERR_INVALID_CONTROL_PAYLOAD_LENGTH"
            );
            cb(error);
            return;
          }
        } else {
          const error = this.createError(
            RangeError,
            `invalid opcode ${this._opcode}`,
            true,
            1002,
            "WS_ERR_INVALID_OPCODE"
          );
          cb(error);
          return;
        }
        if (!this._fin && !this._fragmented) this._fragmented = this._opcode;
        this._masked = (buf[1] & 128) === 128;
        if (this._isServer) {
          if (!this._masked) {
            const error = this.createError(
              RangeError,
              "MASK must be set",
              true,
              1002,
              "WS_ERR_EXPECTED_MASK"
            );
            cb(error);
            return;
          }
        } else if (this._masked) {
          const error = this.createError(
            RangeError,
            "MASK must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_MASK"
          );
          cb(error);
          return;
        }
        if (this._payloadLength === 126) this._state = GET_PAYLOAD_LENGTH_16;
        else if (this._payloadLength === 127) this._state = GET_PAYLOAD_LENGTH_64;
        else this.haveLength(cb);
      }
      /**
       * Gets extended payload length (7+16).
       *
       * @param {Function} cb Callback
       * @private
       */
      getPayloadLength16(cb) {
        if (this._bufferedBytes < 2) {
          this._loop = false;
          return;
        }
        this._payloadLength = this.consume(2).readUInt16BE(0);
        this.haveLength(cb);
      }
      /**
       * Gets extended payload length (7+64).
       *
       * @param {Function} cb Callback
       * @private
       */
      getPayloadLength64(cb) {
        if (this._bufferedBytes < 8) {
          this._loop = false;
          return;
        }
        const buf = this.consume(8);
        const num = buf.readUInt32BE(0);
        if (num > Math.pow(2, 53 - 32) - 1) {
          const error = this.createError(
            RangeError,
            "Unsupported WebSocket frame: payload length > 2^53 - 1",
            false,
            1009,
            "WS_ERR_UNSUPPORTED_DATA_PAYLOAD_LENGTH"
          );
          cb(error);
          return;
        }
        this._payloadLength = num * Math.pow(2, 32) + buf.readUInt32BE(4);
        this.haveLength(cb);
      }
      /**
       * Payload length has been read.
       *
       * @param {Function} cb Callback
       * @private
       */
      haveLength(cb) {
        if (this._payloadLength && this._opcode < 8) {
          this._totalPayloadLength += this._payloadLength;
          if (this._totalPayloadLength > this._maxPayload && this._maxPayload > 0) {
            const error = this.createError(
              RangeError,
              "Max payload size exceeded",
              false,
              1009,
              "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH"
            );
            cb(error);
            return;
          }
        }
        if (this._masked) this._state = GET_MASK;
        else this._state = GET_DATA;
      }
      /**
       * Reads mask bytes.
       *
       * @private
       */
      getMask() {
        if (this._bufferedBytes < 4) {
          this._loop = false;
          return;
        }
        this._mask = this.consume(4);
        this._state = GET_DATA;
      }
      /**
       * Reads data bytes.
       *
       * @param {Function} cb Callback
       * @private
       */
      getData(cb) {
        let data = EMPTY_BUFFER;
        if (this._payloadLength) {
          if (this._bufferedBytes < this._payloadLength) {
            this._loop = false;
            return;
          }
          data = this.consume(this._payloadLength);
          if (this._masked && (this._mask[0] | this._mask[1] | this._mask[2] | this._mask[3]) !== 0) {
            unmask(data, this._mask);
          }
        }
        if (this._opcode > 7) {
          this.controlMessage(data, cb);
          return;
        }
        if (this._maxFragments > 0 && ++this._numFragments > this._maxFragments) {
          const error = this.createError(
            RangeError,
            "Too many message fragments",
            false,
            1008,
            "WS_ERR_TOO_MANY_BUFFERED_PARTS"
          );
          cb(error);
          return;
        }
        if (this._compressed) {
          this._state = INFLATING;
          this.decompress(data, cb);
          return;
        }
        if (data.length) {
          this._messageLength = this._totalPayloadLength;
          this._fragments.push(data);
        }
        this.dataMessage(cb);
      }
      /**
       * Decompresses data.
       *
       * @param {Buffer} data Compressed data
       * @param {Function} cb Callback
       * @private
       */
      decompress(data, cb) {
        const perMessageDeflate = this._extensions[PerMessageDeflate2.extensionName];
        perMessageDeflate.decompress(data, this._fin, (err, buf) => {
          if (err) return cb(err);
          if (buf.length) {
            this._messageLength += buf.length;
            if (this._messageLength > this._maxPayload && this._maxPayload > 0) {
              const error = this.createError(
                RangeError,
                "Max payload size exceeded",
                false,
                1009,
                "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH"
              );
              cb(error);
              return;
            }
            this._fragments.push(buf);
          }
          this.dataMessage(cb);
          if (this._state === GET_INFO) this.startLoop(cb);
        });
      }
      /**
       * Handles a data message.
       *
       * @param {Function} cb Callback
       * @private
       */
      dataMessage(cb) {
        if (!this._fin) {
          this._state = GET_INFO;
          return;
        }
        const messageLength = this._messageLength;
        const fragments = this._fragments;
        this._totalPayloadLength = 0;
        this._messageLength = 0;
        this._fragmented = 0;
        this._numFragments = 0;
        this._fragments = [];
        if (this._opcode === 2) {
          let data;
          if (this._binaryType === "nodebuffer") {
            data = concat(fragments, messageLength);
          } else if (this._binaryType === "arraybuffer") {
            data = toArrayBuffer(concat(fragments, messageLength));
          } else if (this._binaryType === "blob") {
            data = new Blob(fragments);
          } else {
            data = fragments;
          }
          if (this._allowSynchronousEvents) {
            this.emit("message", data, true);
            this._state = GET_INFO;
          } else {
            this._state = DEFER_EVENT;
            setImmediate(() => {
              this.emit("message", data, true);
              this._state = GET_INFO;
              this.startLoop(cb);
            });
          }
        } else {
          const buf = concat(fragments, messageLength);
          if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
            const error = this.createError(
              Error,
              "invalid UTF-8 sequence",
              true,
              1007,
              "WS_ERR_INVALID_UTF8"
            );
            cb(error);
            return;
          }
          if (this._state === INFLATING || this._allowSynchronousEvents) {
            this.emit("message", buf, false);
            this._state = GET_INFO;
          } else {
            this._state = DEFER_EVENT;
            setImmediate(() => {
              this.emit("message", buf, false);
              this._state = GET_INFO;
              this.startLoop(cb);
            });
          }
        }
      }
      /**
       * Handles a control message.
       *
       * @param {Buffer} data Data to handle
       * @return {(Error|RangeError|undefined)} A possible error
       * @private
       */
      controlMessage(data, cb) {
        if (this._opcode === 8) {
          if (data.length === 0) {
            this._loop = false;
            this.emit("conclude", 1005, EMPTY_BUFFER);
            this.end();
          } else {
            const code = data.readUInt16BE(0);
            if (!isValidStatusCode(code)) {
              const error = this.createError(
                RangeError,
                `invalid status code ${code}`,
                true,
                1002,
                "WS_ERR_INVALID_CLOSE_CODE"
              );
              cb(error);
              return;
            }
            const buf = new FastBuffer(
              data.buffer,
              data.byteOffset + 2,
              data.length - 2
            );
            if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
              const error = this.createError(
                Error,
                "invalid UTF-8 sequence",
                true,
                1007,
                "WS_ERR_INVALID_UTF8"
              );
              cb(error);
              return;
            }
            this._loop = false;
            this.emit("conclude", code, buf);
            this.end();
          }
          this._state = GET_INFO;
          return;
        }
        if (this._allowSynchronousEvents) {
          this.emit(this._opcode === 9 ? "ping" : "pong", data);
          this._state = GET_INFO;
        } else {
          this._state = DEFER_EVENT;
          setImmediate(() => {
            this.emit(this._opcode === 9 ? "ping" : "pong", data);
            this._state = GET_INFO;
            this.startLoop(cb);
          });
        }
      }
      /**
       * Builds an error object.
       *
       * @param {function(new:Error|RangeError)} ErrorCtor The error constructor
       * @param {String} message The error message
       * @param {Boolean} prefix Specifies whether or not to add a default prefix to
       *     `message`
       * @param {Number} statusCode The status code
       * @param {String} errorCode The exposed error code
       * @return {(Error|RangeError)} The error
       * @private
       */
      createError(ErrorCtor, message, prefix, statusCode, errorCode) {
        this._loop = false;
        this._errored = true;
        const err = new ErrorCtor(
          prefix ? `Invalid WebSocket frame: ${message}` : message
        );
        Error.captureStackTrace(err, this.createError);
        err.code = errorCode;
        err[kStatusCode] = statusCode;
        return err;
      }
    };
    module.exports = Receiver2;
  }
});

// node_modules/ws/lib/sender.js
var require_sender = __commonJS({
  "node_modules/ws/lib/sender.js"(exports, module) {
    "use strict";
    var { Duplex } = __require("stream");
    var { randomFillSync } = __require("crypto");
    var {
      types: { isUint8Array }
    } = __require("util");
    var PerMessageDeflate2 = require_permessage_deflate();
    var { EMPTY_BUFFER, kWebSocket, NOOP } = require_constants();
    var { isBlob: isBlob2, isValidStatusCode } = require_validation();
    var { mask: applyMask, toBuffer } = require_buffer_util();
    var kByteLength = /* @__PURE__ */ Symbol("kByteLength");
    var maskBuffer = Buffer.alloc(4);
    var RANDOM_POOL_SIZE = 8 * 1024;
    var randomPool;
    var randomPoolPointer = RANDOM_POOL_SIZE;
    var DEFAULT = 0;
    var DEFLATING = 1;
    var GET_BLOB_DATA = 2;
    var Sender2 = class _Sender {
      /**
       * Creates a Sender instance.
       *
       * @param {Duplex} socket The connection socket
       * @param {Object} [extensions] An object containing the negotiated extensions
       * @param {Function} [generateMask] The function used to generate the masking
       *     key
       */
      constructor(socket, extensions, generateMask) {
        this._extensions = extensions || {};
        if (generateMask) {
          this._generateMask = generateMask;
          this._maskBuffer = Buffer.alloc(4);
        }
        this._socket = socket;
        this._firstFragment = true;
        this._compress = false;
        this._bufferedBytes = 0;
        this._queue = [];
        this._state = DEFAULT;
        this.onerror = NOOP;
        this[kWebSocket] = void 0;
      }
      /**
       * Frames a piece of data according to the HyBi WebSocket protocol.
       *
       * @param {(Buffer|String)} data The data to frame
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @return {(Buffer|String)[]} The framed data
       * @public
       */
      static frame(data, options) {
        let mask;
        let merge2 = false;
        let offset = 2;
        let skipMasking = false;
        if (options.mask) {
          mask = options.maskBuffer || maskBuffer;
          if (options.generateMask) {
            options.generateMask(mask);
          } else {
            if (randomPoolPointer === RANDOM_POOL_SIZE) {
              if (randomPool === void 0) {
                randomPool = Buffer.alloc(RANDOM_POOL_SIZE);
              }
              randomFillSync(randomPool, 0, RANDOM_POOL_SIZE);
              randomPoolPointer = 0;
            }
            mask[0] = randomPool[randomPoolPointer++];
            mask[1] = randomPool[randomPoolPointer++];
            mask[2] = randomPool[randomPoolPointer++];
            mask[3] = randomPool[randomPoolPointer++];
          }
          skipMasking = (mask[0] | mask[1] | mask[2] | mask[3]) === 0;
          offset = 6;
        }
        let dataLength;
        if (typeof data === "string") {
          if ((!options.mask || skipMasking) && options[kByteLength] !== void 0) {
            dataLength = options[kByteLength];
          } else {
            data = Buffer.from(data);
            dataLength = data.length;
          }
        } else {
          dataLength = data.length;
          merge2 = options.mask && options.readOnly && !skipMasking;
        }
        let payloadLength = dataLength;
        if (dataLength >= 65536) {
          offset += 8;
          payloadLength = 127;
        } else if (dataLength > 125) {
          offset += 2;
          payloadLength = 126;
        }
        const target = Buffer.allocUnsafe(merge2 ? dataLength + offset : offset);
        target[0] = options.fin ? options.opcode | 128 : options.opcode;
        if (options.rsv1) target[0] |= 64;
        target[1] = payloadLength;
        if (payloadLength === 126) {
          target.writeUInt16BE(dataLength, 2);
        } else if (payloadLength === 127) {
          target[2] = target[3] = 0;
          target.writeUIntBE(dataLength, 4, 6);
        }
        if (!options.mask) {
          return dataLength ? [target, data] : [target];
        }
        target[1] |= 128;
        target[offset - 4] = mask[0];
        target[offset - 3] = mask[1];
        target[offset - 2] = mask[2];
        target[offset - 1] = mask[3];
        if (!dataLength) return [target];
        if (skipMasking) return [target, data];
        if (merge2) {
          applyMask(data, mask, target, offset, dataLength);
          return [target];
        }
        applyMask(data, mask, data, 0, dataLength);
        return [target, data];
      }
      /**
       * Sends a close message to the other peer.
       *
       * @param {Number} [code] The status code component of the body
       * @param {(String|Buffer)} [data] The message component of the body
       * @param {Boolean} [mask=false] Specifies whether or not to mask the message
       * @param {Function} [cb] Callback
       * @public
       */
      close(code, data, mask, cb) {
        let buf;
        if (code === void 0) {
          buf = EMPTY_BUFFER;
        } else if (typeof code !== "number" || !isValidStatusCode(code)) {
          throw new TypeError("First argument must be a valid error code number");
        } else if (data === void 0 || !data.length) {
          buf = Buffer.allocUnsafe(2);
          buf.writeUInt16BE(code, 0);
        } else {
          const length = Buffer.byteLength(data);
          if (length > 123) {
            throw new RangeError("The message must not be greater than 123 bytes");
          }
          buf = Buffer.allocUnsafe(2 + length);
          buf.writeUInt16BE(code, 0);
          if (typeof data === "string") {
            buf.write(data, 2);
          } else if (isUint8Array(data)) {
            buf.set(data, 2);
          } else {
            throw new TypeError("Second argument must be a string or a Uint8Array");
          }
        }
        const options = {
          [kByteLength]: buf.length,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 8,
          readOnly: false,
          rsv1: false
        };
        if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, buf, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(buf, options), cb);
        }
      }
      /**
       * Sends a ping message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Boolean} [mask=false] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback
       * @public
       */
      ping(data, mask, cb) {
        let byteLength2;
        let readOnly;
        if (typeof data === "string") {
          byteLength2 = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob2(data)) {
          byteLength2 = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength2 = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (byteLength2 > 125) {
          throw new RangeError("The data size must not be greater than 125 bytes");
        }
        const options = {
          [kByteLength]: byteLength2,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 9,
          readOnly,
          rsv1: false
        };
        if (isBlob2(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, false, options, cb]);
          } else {
            this.getBlobData(data, false, options, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(data, options), cb);
        }
      }
      /**
       * Sends a pong message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Boolean} [mask=false] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback
       * @public
       */
      pong(data, mask, cb) {
        let byteLength2;
        let readOnly;
        if (typeof data === "string") {
          byteLength2 = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob2(data)) {
          byteLength2 = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength2 = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (byteLength2 > 125) {
          throw new RangeError("The data size must not be greater than 125 bytes");
        }
        const options = {
          [kByteLength]: byteLength2,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 10,
          readOnly,
          rsv1: false
        };
        if (isBlob2(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, false, options, cb]);
          } else {
            this.getBlobData(data, false, options, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(data, options), cb);
        }
      }
      /**
       * Sends a data message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Object} options Options object
       * @param {Boolean} [options.binary=false] Specifies whether `data` is binary
       *     or text
       * @param {Boolean} [options.compress=false] Specifies whether or not to
       *     compress `data`
       * @param {Boolean} [options.fin=false] Specifies whether the fragment is the
       *     last one
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Function} [cb] Callback
       * @public
       */
      send(data, options, cb) {
        const perMessageDeflate = this._extensions[PerMessageDeflate2.extensionName];
        let opcode = options.binary ? 2 : 1;
        let rsv1 = options.compress;
        let byteLength2;
        let readOnly;
        if (typeof data === "string") {
          byteLength2 = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob2(data)) {
          byteLength2 = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength2 = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (this._firstFragment) {
          this._firstFragment = false;
          if (rsv1 && perMessageDeflate && perMessageDeflate.params[perMessageDeflate._isServer ? "server_no_context_takeover" : "client_no_context_takeover"]) {
            rsv1 = byteLength2 >= perMessageDeflate._threshold;
          }
          this._compress = rsv1;
        } else {
          rsv1 = false;
          opcode = 0;
        }
        if (options.fin) this._firstFragment = true;
        const opts = {
          [kByteLength]: byteLength2,
          fin: options.fin,
          generateMask: this._generateMask,
          mask: options.mask,
          maskBuffer: this._maskBuffer,
          opcode,
          readOnly,
          rsv1
        };
        if (isBlob2(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, this._compress, opts, cb]);
          } else {
            this.getBlobData(data, this._compress, opts, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, this._compress, opts, cb]);
        } else {
          this.dispatch(data, this._compress, opts, cb);
        }
      }
      /**
       * Gets the contents of a blob as binary data.
       *
       * @param {Blob} blob The blob
       * @param {Boolean} [compress=false] Specifies whether or not to compress
       *     the data
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @param {Function} [cb] Callback
       * @private
       */
      getBlobData(blob, compress, options, cb) {
        this._bufferedBytes += options[kByteLength];
        this._state = GET_BLOB_DATA;
        blob.arrayBuffer().then((arrayBuffer) => {
          if (this._socket.destroyed) {
            const err = new Error(
              "The socket was closed while the blob was being read"
            );
            process.nextTick(callCallbacks, this, err, cb);
            return;
          }
          this._bufferedBytes -= options[kByteLength];
          const data = toBuffer(arrayBuffer);
          if (!compress) {
            this._state = DEFAULT;
            this.sendFrame(_Sender.frame(data, options), cb);
            this.dequeue();
          } else {
            this.dispatch(data, compress, options, cb);
          }
        }).catch((err) => {
          process.nextTick(onError, this, err, cb);
        });
      }
      /**
       * Dispatches a message.
       *
       * @param {(Buffer|String)} data The message to send
       * @param {Boolean} [compress=false] Specifies whether or not to compress
       *     `data`
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @param {Function} [cb] Callback
       * @private
       */
      dispatch(data, compress, options, cb) {
        if (!compress) {
          this.sendFrame(_Sender.frame(data, options), cb);
          return;
        }
        const perMessageDeflate = this._extensions[PerMessageDeflate2.extensionName];
        this._bufferedBytes += options[kByteLength];
        this._state = DEFLATING;
        perMessageDeflate.compress(data, options.fin, (_, buf) => {
          if (this._socket.destroyed) {
            const err = new Error(
              "The socket was closed while data was being compressed"
            );
            callCallbacks(this, err, cb);
            return;
          }
          this._bufferedBytes -= options[kByteLength];
          this._state = DEFAULT;
          options.readOnly = false;
          this.sendFrame(_Sender.frame(buf, options), cb);
          this.dequeue();
        });
      }
      /**
       * Executes queued send operations.
       *
       * @private
       */
      dequeue() {
        while (this._state === DEFAULT && this._queue.length) {
          const params = this._queue.shift();
          this._bufferedBytes -= params[3][kByteLength];
          Reflect.apply(params[0], this, params.slice(1));
        }
      }
      /**
       * Enqueues a send operation.
       *
       * @param {Array} params Send operation parameters.
       * @private
       */
      enqueue(params) {
        this._bufferedBytes += params[3][kByteLength];
        this._queue.push(params);
      }
      /**
       * Sends a frame.
       *
       * @param {(Buffer | String)[]} list The frame to send
       * @param {Function} [cb] Callback
       * @private
       */
      sendFrame(list, cb) {
        if (list.length === 2) {
          this._socket.cork();
          this._socket.write(list[0]);
          this._socket.write(list[1], cb);
          this._socket.uncork();
        } else {
          this._socket.write(list[0], cb);
        }
      }
    };
    module.exports = Sender2;
    function callCallbacks(sender, err, cb) {
      if (typeof cb === "function") cb(err);
      for (let i = 0; i < sender._queue.length; i++) {
        const params = sender._queue[i];
        const callback = params[params.length - 1];
        if (typeof callback === "function") callback(err);
      }
    }
    function onError(sender, err, cb) {
      callCallbacks(sender, err, cb);
      sender.onerror(err);
    }
  }
});

// node_modules/ws/lib/event-target.js
var require_event_target = __commonJS({
  "node_modules/ws/lib/event-target.js"(exports, module) {
    "use strict";
    var { kForOnEventAttribute, kListener } = require_constants();
    var kCode = /* @__PURE__ */ Symbol("kCode");
    var kData = /* @__PURE__ */ Symbol("kData");
    var kError = /* @__PURE__ */ Symbol("kError");
    var kMessage = /* @__PURE__ */ Symbol("kMessage");
    var kReason = /* @__PURE__ */ Symbol("kReason");
    var kTarget = /* @__PURE__ */ Symbol("kTarget");
    var kType = /* @__PURE__ */ Symbol("kType");
    var kWasClean = /* @__PURE__ */ Symbol("kWasClean");
    var Event = class {
      /**
       * Create a new `Event`.
       *
       * @param {String} type The name of the event
       * @throws {TypeError} If the `type` argument is not specified
       */
      constructor(type) {
        this[kTarget] = null;
        this[kType] = type;
      }
      /**
       * @type {*}
       */
      get target() {
        return this[kTarget];
      }
      /**
       * @type {String}
       */
      get type() {
        return this[kType];
      }
    };
    Object.defineProperty(Event.prototype, "target", { enumerable: true });
    Object.defineProperty(Event.prototype, "type", { enumerable: true });
    var CloseEvent = class extends Event {
      /**
       * Create a new `CloseEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {Number} [options.code=0] The status code explaining why the
       *     connection was closed
       * @param {String} [options.reason=''] A human-readable string explaining why
       *     the connection was closed
       * @param {Boolean} [options.wasClean=false] Indicates whether or not the
       *     connection was cleanly closed
       */
      constructor(type, options = {}) {
        super(type);
        this[kCode] = options.code === void 0 ? 0 : options.code;
        this[kReason] = options.reason === void 0 ? "" : options.reason;
        this[kWasClean] = options.wasClean === void 0 ? false : options.wasClean;
      }
      /**
       * @type {Number}
       */
      get code() {
        return this[kCode];
      }
      /**
       * @type {String}
       */
      get reason() {
        return this[kReason];
      }
      /**
       * @type {Boolean}
       */
      get wasClean() {
        return this[kWasClean];
      }
    };
    Object.defineProperty(CloseEvent.prototype, "code", { enumerable: true });
    Object.defineProperty(CloseEvent.prototype, "reason", { enumerable: true });
    Object.defineProperty(CloseEvent.prototype, "wasClean", { enumerable: true });
    var ErrorEvent = class extends Event {
      /**
       * Create a new `ErrorEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {*} [options.error=null] The error that generated this event
       * @param {String} [options.message=''] The error message
       */
      constructor(type, options = {}) {
        super(type);
        this[kError] = options.error === void 0 ? null : options.error;
        this[kMessage] = options.message === void 0 ? "" : options.message;
      }
      /**
       * @type {*}
       */
      get error() {
        return this[kError];
      }
      /**
       * @type {String}
       */
      get message() {
        return this[kMessage];
      }
    };
    Object.defineProperty(ErrorEvent.prototype, "error", { enumerable: true });
    Object.defineProperty(ErrorEvent.prototype, "message", { enumerable: true });
    var MessageEvent = class extends Event {
      /**
       * Create a new `MessageEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {*} [options.data=null] The message content
       */
      constructor(type, options = {}) {
        super(type);
        this[kData] = options.data === void 0 ? null : options.data;
      }
      /**
       * @type {*}
       */
      get data() {
        return this[kData];
      }
    };
    Object.defineProperty(MessageEvent.prototype, "data", { enumerable: true });
    var EventTarget = {
      /**
       * Register an event listener.
       *
       * @param {String} type A string representing the event type to listen for
       * @param {(Function|Object)} handler The listener to add
       * @param {Object} [options] An options object specifies characteristics about
       *     the event listener
       * @param {Boolean} [options.once=false] A `Boolean` indicating that the
       *     listener should be invoked at most once after being added. If `true`,
       *     the listener would be automatically removed when invoked.
       * @public
       */
      addEventListener(type, handler, options = {}) {
        for (const listener of this.listeners(type)) {
          if (!options[kForOnEventAttribute] && listener[kListener] === handler && !listener[kForOnEventAttribute]) {
            return;
          }
        }
        let wrapper;
        if (type === "message") {
          wrapper = function onMessage(data, isBinary) {
            const event = new MessageEvent("message", {
              data: isBinary ? data : data.toString()
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "close") {
          wrapper = function onClose(code, message) {
            const event = new CloseEvent("close", {
              code,
              reason: message.toString(),
              wasClean: this._closeFrameReceived && this._closeFrameSent
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "error") {
          wrapper = function onError(error) {
            const event = new ErrorEvent("error", {
              error,
              message: error.message
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "open") {
          wrapper = function onOpen() {
            const event = new Event("open");
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else {
          return;
        }
        wrapper[kForOnEventAttribute] = !!options[kForOnEventAttribute];
        wrapper[kListener] = handler;
        if (options.once) {
          this.once(type, wrapper);
        } else {
          this.on(type, wrapper);
        }
      },
      /**
       * Remove an event listener.
       *
       * @param {String} type A string representing the event type to remove
       * @param {(Function|Object)} handler The listener to remove
       * @public
       */
      removeEventListener(type, handler) {
        for (const listener of this.listeners(type)) {
          if (listener[kListener] === handler && !listener[kForOnEventAttribute]) {
            this.removeListener(type, listener);
            break;
          }
        }
      }
    };
    module.exports = {
      CloseEvent,
      ErrorEvent,
      Event,
      EventTarget,
      MessageEvent
    };
    function callListener(listener, thisArg, event) {
      if (typeof listener === "object" && listener.handleEvent) {
        listener.handleEvent.call(listener, event);
      } else {
        listener.call(thisArg, event);
      }
    }
  }
});

// node_modules/ws/lib/extension.js
var require_extension = __commonJS({
  "node_modules/ws/lib/extension.js"(exports, module) {
    "use strict";
    var { tokenChars } = require_validation();
    function push(dest, name, elem) {
      if (dest[name] === void 0) dest[name] = [elem];
      else dest[name].push(elem);
    }
    function parse(header) {
      const offers = /* @__PURE__ */ Object.create(null);
      let params = /* @__PURE__ */ Object.create(null);
      let mustUnescape = false;
      let isEscaping = false;
      let inQuotes = false;
      let extensionName;
      let paramName;
      let start = -1;
      let code = -1;
      let end = -1;
      let i = 0;
      for (; i < header.length; i++) {
        code = header.charCodeAt(i);
        if (extensionName === void 0) {
          if (end === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (i !== 0 && (code === 32 || code === 9)) {
            if (end === -1 && start !== -1) end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end === -1) end = i;
            const name = header.slice(start, end);
            if (code === 44) {
              push(offers, name, params);
              params = /* @__PURE__ */ Object.create(null);
            } else {
              extensionName = name;
            }
            start = end = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        } else if (paramName === void 0) {
          if (end === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (code === 32 || code === 9) {
            if (end === -1 && start !== -1) end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end === -1) end = i;
            push(params, header.slice(start, end), true);
            if (code === 44) {
              push(offers, extensionName, params);
              params = /* @__PURE__ */ Object.create(null);
              extensionName = void 0;
            }
            start = end = -1;
          } else if (code === 61 && start !== -1 && end === -1) {
            paramName = header.slice(start, i);
            start = end = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        } else {
          if (isEscaping) {
            if (tokenChars[code] !== 1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (start === -1) start = i;
            else if (!mustUnescape) mustUnescape = true;
            isEscaping = false;
          } else if (inQuotes) {
            if (tokenChars[code] === 1) {
              if (start === -1) start = i;
            } else if (code === 34 && start !== -1) {
              inQuotes = false;
              end = i;
            } else if (code === 92) {
              isEscaping = true;
            } else {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
          } else if (code === 34 && header.charCodeAt(i - 1) === 61) {
            inQuotes = true;
          } else if (end === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (start !== -1 && (code === 32 || code === 9)) {
            if (end === -1) end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end === -1) end = i;
            let value = header.slice(start, end);
            if (mustUnescape) {
              value = value.replace(/\\/g, "");
              mustUnescape = false;
            }
            push(params, paramName, value);
            if (code === 44) {
              push(offers, extensionName, params);
              params = /* @__PURE__ */ Object.create(null);
              extensionName = void 0;
            }
            paramName = void 0;
            start = end = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        }
      }
      if (start === -1 || inQuotes || code === 32 || code === 9) {
        throw new SyntaxError("Unexpected end of input");
      }
      if (end === -1) end = i;
      const token = header.slice(start, end);
      if (extensionName === void 0) {
        push(offers, token, params);
      } else {
        if (paramName === void 0) {
          push(params, token, true);
        } else if (mustUnescape) {
          push(params, paramName, token.replace(/\\/g, ""));
        } else {
          push(params, paramName, token);
        }
        push(offers, extensionName, params);
      }
      return offers;
    }
    function format(extensions) {
      return Object.keys(extensions).map((extension2) => {
        let configurations = extensions[extension2];
        if (!Array.isArray(configurations)) configurations = [configurations];
        return configurations.map((params) => {
          return [extension2].concat(
            Object.keys(params).map((k) => {
              let values = params[k];
              if (!Array.isArray(values)) values = [values];
              return values.map((v) => v === true ? k : `${k}=${v}`).join("; ");
            })
          ).join("; ");
        }).join(", ");
      }).join(", ");
    }
    module.exports = { format, parse };
  }
});

// node_modules/ws/lib/websocket.js
var require_websocket = __commonJS({
  "node_modules/ws/lib/websocket.js"(exports, module) {
    "use strict";
    var EventEmitter3 = __require("events");
    var https2 = __require("https");
    var http3 = __require("http");
    var net = __require("net");
    var tls = __require("tls");
    var { randomBytes, createHash: createHash3 } = __require("crypto");
    var { Duplex, Readable: Readable2 } = __require("stream");
    var { URL: URL2 } = __require("url");
    var PerMessageDeflate2 = require_permessage_deflate();
    var Receiver2 = require_receiver();
    var Sender2 = require_sender();
    var { isBlob: isBlob2 } = require_validation();
    var {
      BINARY_TYPES,
      CLOSE_TIMEOUT,
      EMPTY_BUFFER,
      GUID,
      kForOnEventAttribute,
      kListener,
      kStatusCode,
      kWebSocket,
      NOOP
    } = require_constants();
    var {
      EventTarget: { addEventListener, removeEventListener }
    } = require_event_target();
    var { format, parse } = require_extension();
    var { toBuffer } = require_buffer_util();
    var kAborted = /* @__PURE__ */ Symbol("kAborted");
    var protocolVersions = [8, 13];
    var readyStates = ["CONNECTING", "OPEN", "CLOSING", "CLOSED"];
    var subprotocolRegex = /^[!#$%&'*+\-.0-9A-Z^_`|a-z~]+$/;
    var WebSocket2 = class _WebSocket extends EventEmitter3 {
      /**
       * Create a new `WebSocket`.
       *
       * @param {(String|URL)} address The URL to which to connect
       * @param {(String|String[])} [protocols] The subprotocols
       * @param {Object} [options] Connection options
       */
      constructor(address, protocols, options) {
        super();
        this._binaryType = BINARY_TYPES[0];
        this._closeCode = 1006;
        this._closeFrameReceived = false;
        this._closeFrameSent = false;
        this._closeMessage = EMPTY_BUFFER;
        this._closeTimer = null;
        this._errorEmitted = false;
        this._extensions = {};
        this._paused = false;
        this._protocol = "";
        this._readyState = _WebSocket.CONNECTING;
        this._receiver = null;
        this._sender = null;
        this._socket = null;
        if (address !== null) {
          this._bufferedAmount = 0;
          this._isServer = false;
          this._redirects = 0;
          if (protocols === void 0) {
            if (!options || options.protocols === void 0) {
              protocols = [];
            } else if (Array.isArray(options.protocols)) {
              protocols = options.protocols;
            } else {
              protocols = [options.protocols];
            }
          } else if (!Array.isArray(protocols)) {
            if (typeof protocols === "object" && protocols !== null) {
              options = protocols;
              if (options.protocols === void 0) {
                protocols = [];
              } else if (Array.isArray(options.protocols)) {
                protocols = options.protocols;
              } else {
                protocols = [options.protocols];
              }
            } else {
              protocols = [protocols];
            }
          }
          initAsClient(this, address, protocols, options);
        } else {
          this._autoPong = options.autoPong;
          this._closeTimeout = options.closeTimeout;
          this._isServer = true;
        }
      }
      /**
       * For historical reasons, the custom "nodebuffer" type is used by the default
       * instead of "blob".
       *
       * @type {String}
       */
      get binaryType() {
        return this._binaryType;
      }
      set binaryType(type) {
        if (!BINARY_TYPES.includes(type)) return;
        this._binaryType = type;
        if (this._receiver) this._receiver._binaryType = type;
      }
      /**
       * @type {Number}
       */
      get bufferedAmount() {
        if (!this._socket) return this._bufferedAmount;
        return this._socket._writableState.length + this._sender._bufferedBytes;
      }
      /**
       * @type {String}
       */
      get extensions() {
        return Object.keys(this._extensions).join();
      }
      /**
       * @type {Boolean}
       */
      get isPaused() {
        return this._paused;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onclose() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onerror() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onopen() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onmessage() {
        return null;
      }
      /**
       * @type {String}
       */
      get protocol() {
        return this._protocol;
      }
      /**
       * @type {Number}
       */
      get readyState() {
        return this._readyState;
      }
      /**
       * @type {String}
       */
      get url() {
        return this._url;
      }
      /**
       * Set up the socket and the internal resources.
       *
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Object} options Options object
       * @param {Boolean} [options.allowSynchronousEvents=false] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Number} [options.maxBufferedChunks=0] The maximum number of
       *     buffered data chunks
       * @param {Number} [options.maxFragments=0] The maximum number of message
       *     fragments
       * @param {Number} [options.maxPayload=0] The maximum allowed message size
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       * @private
       */
      setSocket(socket, head, options) {
        const receiver = new Receiver2({
          allowSynchronousEvents: options.allowSynchronousEvents,
          binaryType: this.binaryType,
          extensions: this._extensions,
          isServer: this._isServer,
          maxBufferedChunks: options.maxBufferedChunks,
          maxFragments: options.maxFragments,
          maxPayload: options.maxPayload,
          skipUTF8Validation: options.skipUTF8Validation
        });
        const sender = new Sender2(socket, this._extensions, options.generateMask);
        this._receiver = receiver;
        this._sender = sender;
        this._socket = socket;
        receiver[kWebSocket] = this;
        sender[kWebSocket] = this;
        socket[kWebSocket] = this;
        receiver.on("conclude", receiverOnConclude);
        receiver.on("drain", receiverOnDrain);
        receiver.on("error", receiverOnError);
        receiver.on("message", receiverOnMessage);
        receiver.on("ping", receiverOnPing);
        receiver.on("pong", receiverOnPong);
        sender.onerror = senderOnError;
        if (socket.setTimeout) socket.setTimeout(0);
        if (socket.setNoDelay) socket.setNoDelay();
        if (head.length > 0) socket.unshift(head);
        socket.on("close", socketOnClose);
        socket.on("data", socketOnData);
        socket.on("end", socketOnEnd);
        socket.on("error", socketOnError);
        this._readyState = _WebSocket.OPEN;
        this.emit("open");
      }
      /**
       * Emit the `'close'` event.
       *
       * @private
       */
      emitClose() {
        if (!this._socket) {
          this._readyState = _WebSocket.CLOSED;
          this.emit("close", this._closeCode, this._closeMessage);
          return;
        }
        if (this._extensions[PerMessageDeflate2.extensionName]) {
          this._extensions[PerMessageDeflate2.extensionName].cleanup();
        }
        this._receiver.removeAllListeners();
        this._readyState = _WebSocket.CLOSED;
        this.emit("close", this._closeCode, this._closeMessage);
      }
      /**
       * Start a closing handshake.
       *
       *          +----------+   +-----------+   +----------+
       *     - - -|ws.close()|-->|close frame|-->|ws.close()|- - -
       *    |     +----------+   +-----------+   +----------+     |
       *          +----------+   +-----------+         |
       * CLOSING  |ws.close()|<--|close frame|<--+-----+       CLOSING
       *          +----------+   +-----------+   |
       *    |           |                        |   +---+        |
       *                +------------------------+-->|fin| - - - -
       *    |         +---+                      |   +---+
       *     - - - - -|fin|<---------------------+
       *              +---+
       *
       * @param {Number} [code] Status code explaining why the connection is closing
       * @param {(String|Buffer)} [data] The reason why the connection is
       *     closing
       * @public
       */
      close(code, data) {
        if (this.readyState === _WebSocket.CLOSED) return;
        if (this.readyState === _WebSocket.CONNECTING) {
          const msg = "WebSocket was closed before the connection was established";
          abortHandshake(this, this._req, msg);
          return;
        }
        if (this.readyState === _WebSocket.CLOSING) {
          if (this._closeFrameSent && (this._closeFrameReceived || this._receiver._writableState.errorEmitted)) {
            this._socket.end();
          }
          return;
        }
        this._sender.close(code, data, !this._isServer, (err) => {
          if (err) return;
          this._closeFrameSent = true;
          if (this._closeFrameReceived || this._receiver._writableState.errorEmitted) {
            this._socket.end();
          }
        });
        this._readyState = _WebSocket.CLOSING;
        setCloseTimer(this);
      }
      /**
       * Pause the socket.
       *
       * @public
       */
      pause() {
        if (this.readyState === _WebSocket.CONNECTING || this.readyState === _WebSocket.CLOSED) {
          return;
        }
        this._paused = true;
        this._socket.pause();
      }
      /**
       * Send a ping.
       *
       * @param {*} [data] The data to send
       * @param {Boolean} [mask] Indicates whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when the ping is sent
       * @public
       */
      ping(data, mask, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof data === "function") {
          cb = data;
          data = mask = void 0;
        } else if (typeof mask === "function") {
          cb = mask;
          mask = void 0;
        }
        if (typeof data === "number") data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        if (mask === void 0) mask = !this._isServer;
        this._sender.ping(data || EMPTY_BUFFER, mask, cb);
      }
      /**
       * Send a pong.
       *
       * @param {*} [data] The data to send
       * @param {Boolean} [mask] Indicates whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when the pong is sent
       * @public
       */
      pong(data, mask, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof data === "function") {
          cb = data;
          data = mask = void 0;
        } else if (typeof mask === "function") {
          cb = mask;
          mask = void 0;
        }
        if (typeof data === "number") data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        if (mask === void 0) mask = !this._isServer;
        this._sender.pong(data || EMPTY_BUFFER, mask, cb);
      }
      /**
       * Resume the socket.
       *
       * @public
       */
      resume() {
        if (this.readyState === _WebSocket.CONNECTING || this.readyState === _WebSocket.CLOSED) {
          return;
        }
        this._paused = false;
        if (!this._receiver._writableState.needDrain) this._socket.resume();
      }
      /**
       * Send a data message.
       *
       * @param {*} data The message to send
       * @param {Object} [options] Options object
       * @param {Boolean} [options.binary] Specifies whether `data` is binary or
       *     text
       * @param {Boolean} [options.compress] Specifies whether or not to compress
       *     `data`
       * @param {Boolean} [options.fin=true] Specifies whether the fragment is the
       *     last one
       * @param {Boolean} [options.mask] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when data is written out
       * @public
       */
      send(data, options, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof options === "function") {
          cb = options;
          options = {};
        }
        if (typeof data === "number") data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        const opts = {
          binary: typeof data !== "string",
          mask: !this._isServer,
          compress: true,
          fin: true,
          ...options
        };
        if (!this._extensions[PerMessageDeflate2.extensionName]) {
          opts.compress = false;
        }
        this._sender.send(data || EMPTY_BUFFER, opts, cb);
      }
      /**
       * Forcibly close the connection.
       *
       * @public
       */
      terminate() {
        if (this.readyState === _WebSocket.CLOSED) return;
        if (this.readyState === _WebSocket.CONNECTING) {
          const msg = "WebSocket was closed before the connection was established";
          abortHandshake(this, this._req, msg);
          return;
        }
        if (this._socket) {
          this._readyState = _WebSocket.CLOSING;
          this._socket.destroy();
        }
      }
    };
    Object.defineProperty(WebSocket2, "CONNECTING", {
      enumerable: true,
      value: readyStates.indexOf("CONNECTING")
    });
    Object.defineProperty(WebSocket2.prototype, "CONNECTING", {
      enumerable: true,
      value: readyStates.indexOf("CONNECTING")
    });
    Object.defineProperty(WebSocket2, "OPEN", {
      enumerable: true,
      value: readyStates.indexOf("OPEN")
    });
    Object.defineProperty(WebSocket2.prototype, "OPEN", {
      enumerable: true,
      value: readyStates.indexOf("OPEN")
    });
    Object.defineProperty(WebSocket2, "CLOSING", {
      enumerable: true,
      value: readyStates.indexOf("CLOSING")
    });
    Object.defineProperty(WebSocket2.prototype, "CLOSING", {
      enumerable: true,
      value: readyStates.indexOf("CLOSING")
    });
    Object.defineProperty(WebSocket2, "CLOSED", {
      enumerable: true,
      value: readyStates.indexOf("CLOSED")
    });
    Object.defineProperty(WebSocket2.prototype, "CLOSED", {
      enumerable: true,
      value: readyStates.indexOf("CLOSED")
    });
    [
      "binaryType",
      "bufferedAmount",
      "extensions",
      "isPaused",
      "protocol",
      "readyState",
      "url"
    ].forEach((property) => {
      Object.defineProperty(WebSocket2.prototype, property, { enumerable: true });
    });
    ["open", "error", "close", "message"].forEach((method) => {
      Object.defineProperty(WebSocket2.prototype, `on${method}`, {
        enumerable: true,
        get() {
          for (const listener of this.listeners(method)) {
            if (listener[kForOnEventAttribute]) return listener[kListener];
          }
          return null;
        },
        set(handler) {
          for (const listener of this.listeners(method)) {
            if (listener[kForOnEventAttribute]) {
              this.removeListener(method, listener);
              break;
            }
          }
          if (typeof handler !== "function") return;
          this.addEventListener(method, handler, {
            [kForOnEventAttribute]: true
          });
        }
      });
    });
    WebSocket2.prototype.addEventListener = addEventListener;
    WebSocket2.prototype.removeEventListener = removeEventListener;
    module.exports = WebSocket2;
    function initAsClient(websocket, address, protocols, options) {
      const opts = {
        allowSynchronousEvents: true,
        autoPong: true,
        closeTimeout: CLOSE_TIMEOUT,
        protocolVersion: protocolVersions[1],
        maxBufferedChunks: 256 * 1024,
        maxFragments: 16 * 1024,
        maxPayload: 100 * 1024 * 1024,
        skipUTF8Validation: false,
        perMessageDeflate: true,
        followRedirects: false,
        maxRedirects: 10,
        ...options,
        socketPath: void 0,
        hostname: void 0,
        protocol: void 0,
        protocols: void 0,
        timeout: void 0,
        method: "GET",
        host: void 0,
        path: void 0,
        port: void 0
      };
      websocket._autoPong = opts.autoPong;
      websocket._closeTimeout = opts.closeTimeout;
      if (!protocolVersions.includes(opts.protocolVersion)) {
        throw new RangeError(
          `Unsupported protocol version: ${opts.protocolVersion} (supported versions: ${protocolVersions.join(", ")})`
        );
      }
      let parsedUrl;
      if (address instanceof URL2) {
        parsedUrl = address;
      } else {
        try {
          parsedUrl = new URL2(address);
        } catch {
          throw new SyntaxError(`Invalid URL: ${address}`);
        }
      }
      if (parsedUrl.protocol === "http:") {
        parsedUrl.protocol = "ws:";
      } else if (parsedUrl.protocol === "https:") {
        parsedUrl.protocol = "wss:";
      }
      websocket._url = parsedUrl.href;
      const isSecure = parsedUrl.protocol === "wss:";
      const isIpcUrl = parsedUrl.protocol === "ws+unix:";
      let invalidUrlMessage;
      if (parsedUrl.protocol !== "ws:" && !isSecure && !isIpcUrl) {
        invalidUrlMessage = `The URL's protocol must be one of "ws:", "wss:", "http:", "https:", or "ws+unix:"`;
      } else if (isIpcUrl && !parsedUrl.pathname) {
        invalidUrlMessage = "The URL's pathname is empty";
      } else if (parsedUrl.hash) {
        invalidUrlMessage = "The URL contains a fragment identifier";
      }
      if (invalidUrlMessage) {
        const err = new SyntaxError(invalidUrlMessage);
        if (websocket._redirects === 0) {
          throw err;
        } else {
          emitErrorAndClose(websocket, err);
          return;
        }
      }
      const defaultPort = isSecure ? 443 : 80;
      const key = randomBytes(16).toString("base64");
      const request = isSecure ? https2.request : http3.request;
      const protocolSet = /* @__PURE__ */ new Set();
      let perMessageDeflate;
      opts.createConnection = opts.createConnection || (isSecure ? tlsConnect : netConnect);
      opts.defaultPort = opts.defaultPort || defaultPort;
      opts.port = parsedUrl.port || defaultPort;
      opts.host = parsedUrl.hostname.startsWith("[") ? parsedUrl.hostname.slice(1, -1) : parsedUrl.hostname;
      opts.headers = {
        ...opts.headers,
        "Sec-WebSocket-Version": opts.protocolVersion,
        "Sec-WebSocket-Key": key,
        Connection: "Upgrade",
        Upgrade: "websocket"
      };
      opts.path = parsedUrl.pathname + parsedUrl.search;
      opts.timeout = opts.handshakeTimeout;
      if (opts.perMessageDeflate) {
        perMessageDeflate = new PerMessageDeflate2({
          ...opts.perMessageDeflate,
          isServer: false,
          maxPayload: opts.maxPayload
        });
        opts.headers["Sec-WebSocket-Extensions"] = format({
          [PerMessageDeflate2.extensionName]: perMessageDeflate.offer()
        });
      }
      if (protocols.length) {
        for (const protocol of protocols) {
          if (typeof protocol !== "string" || !subprotocolRegex.test(protocol) || protocolSet.has(protocol)) {
            throw new SyntaxError(
              "An invalid or duplicated subprotocol was specified"
            );
          }
          protocolSet.add(protocol);
        }
        opts.headers["Sec-WebSocket-Protocol"] = protocols.join(",");
      }
      if (opts.origin) {
        if (opts.protocolVersion < 13) {
          opts.headers["Sec-WebSocket-Origin"] = opts.origin;
        } else {
          opts.headers.Origin = opts.origin;
        }
      }
      if (parsedUrl.username || parsedUrl.password) {
        opts.auth = `${parsedUrl.username}:${parsedUrl.password}`;
      }
      if (isIpcUrl) {
        const i = opts.path.indexOf(":");
        if (i === -1) {
          opts.socketPath = opts.path;
          opts.path = "";
        } else {
          opts.socketPath = opts.path.slice(0, i);
          opts.path = opts.path.slice(i + 1);
        }
      }
      let req;
      if (opts.followRedirects) {
        if (websocket._redirects === 0) {
          websocket._originalIpc = isIpcUrl;
          websocket._originalSecure = isSecure;
          websocket._originalHostOrSocketPath = isIpcUrl ? opts.socketPath : parsedUrl.host;
          const headers = options && options.headers;
          options = { ...options, headers: {} };
          if (headers) {
            for (const [key2, value] of Object.entries(headers)) {
              options.headers[key2.toLowerCase()] = value;
            }
          }
        } else if (websocket.listenerCount("redirect") === 0) {
          const isSameHost = isIpcUrl ? websocket._originalIpc ? opts.socketPath === websocket._originalHostOrSocketPath : false : websocket._originalIpc ? false : parsedUrl.host === websocket._originalHostOrSocketPath;
          if (!isSameHost || websocket._originalSecure && !isSecure) {
            delete opts.headers.authorization;
            delete opts.headers.cookie;
            if (!isSameHost) delete opts.headers.host;
            opts.auth = void 0;
          }
        }
        if (opts.auth && !options.headers.authorization) {
          options.headers.authorization = "Basic " + Buffer.from(opts.auth).toString("base64");
        }
        req = websocket._req = request(opts);
        if (websocket._redirects) {
          websocket.emit("redirect", websocket.url, req);
        }
      } else {
        req = websocket._req = request(opts);
      }
      if (opts.timeout) {
        req.on("timeout", () => {
          abortHandshake(websocket, req, "Opening handshake has timed out");
        });
      }
      req.on("error", (err) => {
        if (req === null || req[kAborted]) return;
        req = websocket._req = null;
        emitErrorAndClose(websocket, err);
      });
      req.on("response", (res) => {
        const location = res.headers.location;
        const statusCode = res.statusCode;
        if (location && opts.followRedirects && statusCode >= 300 && statusCode < 400) {
          if (++websocket._redirects > opts.maxRedirects) {
            abortHandshake(websocket, req, "Maximum redirects exceeded");
            return;
          }
          req.abort();
          let addr;
          try {
            addr = new URL2(location, address);
          } catch (e) {
            const err = new SyntaxError(`Invalid URL: ${location}`);
            emitErrorAndClose(websocket, err);
            return;
          }
          initAsClient(websocket, addr, protocols, options);
        } else if (!websocket.emit("unexpected-response", req, res)) {
          abortHandshake(
            websocket,
            req,
            `Unexpected server response: ${res.statusCode}`
          );
        }
      });
      req.on("upgrade", (res, socket, head) => {
        websocket.emit("upgrade", res);
        if (websocket.readyState !== WebSocket2.CONNECTING) return;
        req = websocket._req = null;
        const upgrade = res.headers.upgrade;
        if (upgrade === void 0 || upgrade.toLowerCase() !== "websocket") {
          abortHandshake(websocket, socket, "Invalid Upgrade header");
          return;
        }
        const digest = createHash3("sha1").update(key + GUID).digest("base64");
        if (res.headers["sec-websocket-accept"] !== digest) {
          abortHandshake(websocket, socket, "Invalid Sec-WebSocket-Accept header");
          return;
        }
        const serverProt = res.headers["sec-websocket-protocol"];
        let protError;
        if (serverProt !== void 0) {
          if (!protocolSet.size) {
            protError = "Server sent a subprotocol but none was requested";
          } else if (!protocolSet.has(serverProt)) {
            protError = "Server sent an invalid subprotocol";
          }
        } else if (protocolSet.size) {
          protError = "Server sent no subprotocol";
        }
        if (protError) {
          abortHandshake(websocket, socket, protError);
          return;
        }
        if (serverProt) websocket._protocol = serverProt;
        const secWebSocketExtensions = res.headers["sec-websocket-extensions"];
        if (secWebSocketExtensions !== void 0) {
          if (!perMessageDeflate) {
            const message = "Server sent a Sec-WebSocket-Extensions header but no extension was requested";
            abortHandshake(websocket, socket, message);
            return;
          }
          let extensions;
          try {
            extensions = parse(secWebSocketExtensions);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Extensions header";
            abortHandshake(websocket, socket, message);
            return;
          }
          const extensionNames = Object.keys(extensions);
          if (extensionNames.length !== 1 || extensionNames[0] !== PerMessageDeflate2.extensionName) {
            const message = "Server indicated an extension that was not requested";
            abortHandshake(websocket, socket, message);
            return;
          }
          try {
            perMessageDeflate.accept(extensions[PerMessageDeflate2.extensionName]);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Extensions header";
            abortHandshake(websocket, socket, message);
            return;
          }
          websocket._extensions[PerMessageDeflate2.extensionName] = perMessageDeflate;
        }
        websocket.setSocket(socket, head, {
          allowSynchronousEvents: opts.allowSynchronousEvents,
          generateMask: opts.generateMask,
          maxBufferedChunks: opts.maxBufferedChunks,
          maxFragments: opts.maxFragments,
          maxPayload: opts.maxPayload,
          skipUTF8Validation: opts.skipUTF8Validation
        });
      });
      if (opts.finishRequest) {
        opts.finishRequest(req, websocket);
      } else {
        req.end();
      }
    }
    function emitErrorAndClose(websocket, err) {
      websocket._readyState = WebSocket2.CLOSING;
      websocket._errorEmitted = true;
      websocket.emit("error", err);
      websocket.emitClose();
    }
    function netConnect(options) {
      options.path = options.socketPath;
      return net.connect(options);
    }
    function tlsConnect(options) {
      options.path = void 0;
      if (!options.servername && options.servername !== "") {
        options.servername = net.isIP(options.host) ? "" : options.host;
      }
      return tls.connect(options);
    }
    function abortHandshake(websocket, stream4, message) {
      websocket._readyState = WebSocket2.CLOSING;
      const err = new Error(message);
      Error.captureStackTrace(err, abortHandshake);
      if (stream4.setHeader) {
        stream4[kAborted] = true;
        stream4.abort();
        if (stream4.socket && !stream4.socket.destroyed) {
          stream4.socket.destroy();
        }
        process.nextTick(emitErrorAndClose, websocket, err);
      } else {
        stream4.destroy(err);
        stream4.once("error", websocket.emit.bind(websocket, "error"));
        stream4.once("close", websocket.emitClose.bind(websocket));
      }
    }
    function sendAfterClose(websocket, data, cb) {
      if (data) {
        const length = isBlob2(data) ? data.size : toBuffer(data).length;
        if (websocket._socket) websocket._sender._bufferedBytes += length;
        else websocket._bufferedAmount += length;
      }
      if (cb) {
        const err = new Error(
          `WebSocket is not open: readyState ${websocket.readyState} (${readyStates[websocket.readyState]})`
        );
        process.nextTick(cb, err);
      }
    }
    function receiverOnConclude(code, reason) {
      const websocket = this[kWebSocket];
      websocket._closeFrameReceived = true;
      websocket._closeMessage = reason;
      websocket._closeCode = code;
      if (websocket._socket[kWebSocket] === void 0) return;
      websocket._socket.removeListener("data", socketOnData);
      process.nextTick(resume, websocket._socket);
      if (code === 1005) websocket.close();
      else websocket.close(code, reason);
    }
    function receiverOnDrain() {
      const websocket = this[kWebSocket];
      if (!websocket.isPaused) websocket._socket.resume();
    }
    function receiverOnError(err) {
      const websocket = this[kWebSocket];
      if (websocket._socket[kWebSocket] !== void 0) {
        websocket._socket.removeListener("data", socketOnData);
        process.nextTick(resume, websocket._socket);
        websocket.close(err[kStatusCode]);
      }
      if (!websocket._errorEmitted) {
        websocket._errorEmitted = true;
        websocket.emit("error", err);
      }
    }
    function receiverOnFinish() {
      this[kWebSocket].emitClose();
    }
    function receiverOnMessage(data, isBinary) {
      this[kWebSocket].emit("message", data, isBinary);
    }
    function receiverOnPing(data) {
      const websocket = this[kWebSocket];
      if (websocket._autoPong) websocket.pong(data, !this._isServer, NOOP);
      websocket.emit("ping", data);
    }
    function receiverOnPong(data) {
      this[kWebSocket].emit("pong", data);
    }
    function resume(stream4) {
      stream4.resume();
    }
    function senderOnError(err) {
      const websocket = this[kWebSocket];
      if (websocket.readyState === WebSocket2.CLOSED) return;
      if (websocket.readyState === WebSocket2.OPEN) {
        websocket._readyState = WebSocket2.CLOSING;
        setCloseTimer(websocket);
      }
      this._socket.end();
      if (!websocket._errorEmitted) {
        websocket._errorEmitted = true;
        websocket.emit("error", err);
      }
    }
    function setCloseTimer(websocket) {
      websocket._closeTimer = setTimeout(
        websocket._socket.destroy.bind(websocket._socket),
        websocket._closeTimeout
      );
    }
    function socketOnClose() {
      const websocket = this[kWebSocket];
      this.removeListener("close", socketOnClose);
      this.removeListener("data", socketOnData);
      this.removeListener("end", socketOnEnd);
      websocket._readyState = WebSocket2.CLOSING;
      if (!this._readableState.endEmitted && !websocket._closeFrameReceived && !websocket._receiver._writableState.errorEmitted && this._readableState.length !== 0) {
        const chunk = this.read(this._readableState.length);
        websocket._receiver.write(chunk);
      }
      websocket._receiver.end();
      this[kWebSocket] = void 0;
      clearTimeout(websocket._closeTimer);
      if (websocket._receiver._writableState.finished || websocket._receiver._writableState.errorEmitted) {
        websocket.emitClose();
      } else {
        websocket._receiver.on("error", receiverOnFinish);
        websocket._receiver.on("finish", receiverOnFinish);
      }
    }
    function socketOnData(chunk) {
      if (!this[kWebSocket]._receiver.write(chunk)) {
        this.pause();
      }
    }
    function socketOnEnd() {
      const websocket = this[kWebSocket];
      websocket._readyState = WebSocket2.CLOSING;
      websocket._receiver.end();
      this.end();
    }
    function socketOnError() {
      const websocket = this[kWebSocket];
      this.removeListener("error", socketOnError);
      this.on("error", NOOP);
      if (websocket) {
        websocket._readyState = WebSocket2.CLOSING;
        this.destroy();
      }
    }
  }
});

// node_modules/ws/lib/stream.js
var require_stream = __commonJS({
  "node_modules/ws/lib/stream.js"(exports, module) {
    "use strict";
    var WebSocket2 = require_websocket();
    var { Duplex } = __require("stream");
    function emitClose(stream4) {
      stream4.emit("close");
    }
    function duplexOnEnd() {
      if (!this.destroyed && this._writableState.finished) {
        this.destroy();
      }
    }
    function duplexOnError(err) {
      this.removeListener("error", duplexOnError);
      this.destroy();
      if (this.listenerCount("error") === 0) {
        this.emit("error", err);
      }
    }
    function createWebSocketStream2(ws, options) {
      let terminateOnDestroy = true;
      const duplex = new Duplex({
        ...options,
        autoDestroy: false,
        emitClose: false,
        objectMode: false,
        writableObjectMode: false
      });
      ws.on("message", function message(msg, isBinary) {
        const data = !isBinary && duplex._readableState.objectMode ? msg.toString() : msg;
        if (!duplex.push(data)) ws.pause();
      });
      ws.once("error", function error(err) {
        if (duplex.destroyed) return;
        terminateOnDestroy = false;
        duplex.destroy(err);
      });
      ws.once("close", function close() {
        if (duplex.destroyed) return;
        duplex.push(null);
      });
      duplex._destroy = function(err, callback) {
        if (ws.readyState === ws.CLOSED) {
          callback(err);
          process.nextTick(emitClose, duplex);
          return;
        }
        let called = false;
        ws.once("error", function error(err2) {
          called = true;
          callback(err2);
        });
        ws.once("close", function close() {
          if (!called) callback(err);
          process.nextTick(emitClose, duplex);
        });
        if (terminateOnDestroy) ws.terminate();
      };
      duplex._final = function(callback) {
        if (ws.readyState === ws.CONNECTING) {
          ws.once("open", function open() {
            duplex._final(callback);
          });
          return;
        }
        if (ws._socket === null) return;
        if (ws._socket._writableState.finished) {
          callback();
          if (duplex._readableState.endEmitted) duplex.destroy();
        } else {
          ws._socket.once("finish", function finish() {
            callback();
          });
          ws.close();
        }
      };
      duplex._read = function() {
        if (ws.isPaused) ws.resume();
      };
      duplex._write = function(chunk, encoding, callback) {
        if (ws.readyState === ws.CONNECTING) {
          ws.once("open", function open() {
            duplex._write(chunk, encoding, callback);
          });
          return;
        }
        ws.send(chunk, callback);
      };
      duplex.on("end", duplexOnEnd);
      duplex.on("error", duplexOnError);
      return duplex;
    }
    module.exports = createWebSocketStream2;
  }
});

// node_modules/ws/lib/subprotocol.js
var require_subprotocol = __commonJS({
  "node_modules/ws/lib/subprotocol.js"(exports, module) {
    "use strict";
    var { tokenChars } = require_validation();
    function parse(header) {
      const protocols = /* @__PURE__ */ new Set();
      let start = -1;
      let end = -1;
      let i = 0;
      for (i; i < header.length; i++) {
        const code = header.charCodeAt(i);
        if (end === -1 && tokenChars[code] === 1) {
          if (start === -1) start = i;
        } else if (i !== 0 && (code === 32 || code === 9)) {
          if (end === -1 && start !== -1) end = i;
        } else if (code === 44) {
          if (start === -1) {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
          if (end === -1) end = i;
          const protocol2 = header.slice(start, end);
          if (protocols.has(protocol2)) {
            throw new SyntaxError(`The "${protocol2}" subprotocol is duplicated`);
          }
          protocols.add(protocol2);
          start = end = -1;
        } else {
          throw new SyntaxError(`Unexpected character at index ${i}`);
        }
      }
      if (start === -1 || end !== -1) {
        throw new SyntaxError("Unexpected end of input");
      }
      const protocol = header.slice(start, i);
      if (protocols.has(protocol)) {
        throw new SyntaxError(`The "${protocol}" subprotocol is duplicated`);
      }
      protocols.add(protocol);
      return protocols;
    }
    module.exports = { parse };
  }
});

// node_modules/ws/lib/websocket-server.js
var require_websocket_server = __commonJS({
  "node_modules/ws/lib/websocket-server.js"(exports, module) {
    "use strict";
    var EventEmitter3 = __require("events");
    var http3 = __require("http");
    var { Duplex } = __require("stream");
    var { createHash: createHash3 } = __require("crypto");
    var extension2 = require_extension();
    var PerMessageDeflate2 = require_permessage_deflate();
    var subprotocol2 = require_subprotocol();
    var WebSocket2 = require_websocket();
    var { CLOSE_TIMEOUT, GUID, kWebSocket } = require_constants();
    var keyRegex = /^[+/0-9A-Za-z]{22}==$/;
    var RUNNING = 0;
    var CLOSING = 1;
    var CLOSED = 2;
    var WebSocketServer2 = class extends EventEmitter3 {
      /**
       * Create a `WebSocketServer` instance.
       *
       * @param {Object} options Configuration options
       * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {Boolean} [options.autoPong=true] Specifies whether or not to
       *     automatically send a pong in response to a ping
       * @param {Number} [options.backlog=511] The maximum length of the queue of
       *     pending connections
       * @param {Boolean} [options.clientTracking=true] Specifies whether or not to
       *     track clients
       * @param {Number} [options.closeTimeout=30000] Duration in milliseconds to
       *     wait for the closing handshake to finish after `websocket.close()` is
       *     called
       * @param {Function} [options.handleProtocols] A hook to handle protocols
       * @param {String} [options.host] The hostname where to bind the server
       * @param {Number} [options.maxBufferedChunks=262144] The maximum number of
       *     buffered data chunks
       * @param {Number} [options.maxFragments=16384] The maximum number of message
       *     fragments
       * @param {Number} [options.maxPayload=104857600] The maximum allowed message
       *     size
       * @param {Boolean} [options.noServer=false] Enable no server mode
       * @param {String} [options.path] Accept only connections matching this path
       * @param {(Boolean|Object)} [options.perMessageDeflate=false] Enable/disable
       *     permessage-deflate
       * @param {Number} [options.port] The port where to bind the server
       * @param {(http.Server|https.Server)} [options.server] A pre-created HTTP/S
       *     server to use
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       * @param {Function} [options.verifyClient] A hook to reject connections
       * @param {Function} [options.WebSocket=WebSocket] Specifies the `WebSocket`
       *     class to use. It must be the `WebSocket` class or class that extends it
       * @param {Function} [callback] A listener for the `listening` event
       */
      constructor(options, callback) {
        super();
        options = {
          allowSynchronousEvents: true,
          autoPong: true,
          maxBufferedChunks: 256 * 1024,
          maxFragments: 16 * 1024,
          maxPayload: 100 * 1024 * 1024,
          skipUTF8Validation: false,
          perMessageDeflate: false,
          handleProtocols: null,
          clientTracking: true,
          closeTimeout: CLOSE_TIMEOUT,
          verifyClient: null,
          noServer: false,
          backlog: null,
          // use default (511 as implemented in net.js)
          server: null,
          host: null,
          path: null,
          port: null,
          WebSocket: WebSocket2,
          ...options
        };
        if (options.port == null && !options.server && !options.noServer || options.port != null && (options.server || options.noServer) || options.server && options.noServer) {
          throw new TypeError(
            'One and only one of the "port", "server", or "noServer" options must be specified'
          );
        }
        if (options.port != null) {
          this._server = http3.createServer((req, res) => {
            const body = http3.STATUS_CODES[426];
            res.writeHead(426, {
              "Content-Length": body.length,
              "Content-Type": "text/plain"
            });
            res.end(body);
          });
          this._server.listen(
            options.port,
            options.host,
            options.backlog,
            callback
          );
        } else if (options.server) {
          this._server = options.server;
        }
        if (this._server) {
          const emitConnection = this.emit.bind(this, "connection");
          this._removeListeners = addListeners(this._server, {
            listening: this.emit.bind(this, "listening"),
            error: this.emit.bind(this, "error"),
            upgrade: (req, socket, head) => {
              this.handleUpgrade(req, socket, head, emitConnection);
            }
          });
        }
        if (options.perMessageDeflate === true) options.perMessageDeflate = {};
        if (options.clientTracking) {
          this.clients = /* @__PURE__ */ new Set();
          this._shouldEmitClose = false;
        }
        this.options = options;
        this._state = RUNNING;
      }
      /**
       * Returns the bound address, the address family name, and port of the server
       * as reported by the operating system if listening on an IP socket.
       * If the server is listening on a pipe or UNIX domain socket, the name is
       * returned as a string.
       *
       * @return {(Object|String|null)} The address of the server
       * @public
       */
      address() {
        if (this.options.noServer) {
          throw new Error('The server is operating in "noServer" mode');
        }
        if (!this._server) return null;
        return this._server.address();
      }
      /**
       * Stop the server from accepting new connections and emit the `'close'` event
       * when all existing connections are closed.
       *
       * @param {Function} [cb] A one-time listener for the `'close'` event
       * @public
       */
      close(cb) {
        if (this._state === CLOSED) {
          if (cb) {
            this.once("close", () => {
              cb(new Error("The server is not running"));
            });
          }
          process.nextTick(emitClose, this);
          return;
        }
        if (cb) this.once("close", cb);
        if (this._state === CLOSING) return;
        this._state = CLOSING;
        if (this.options.noServer || this.options.server) {
          if (this._server) {
            this._removeListeners();
            this._removeListeners = this._server = null;
          }
          if (this.clients) {
            if (!this.clients.size) {
              process.nextTick(emitClose, this);
            } else {
              this._shouldEmitClose = true;
            }
          } else {
            process.nextTick(emitClose, this);
          }
        } else {
          const server = this._server;
          this._removeListeners();
          this._removeListeners = this._server = null;
          server.close(() => {
            emitClose(this);
          });
        }
      }
      /**
       * See if a given request should be handled by this server instance.
       *
       * @param {http.IncomingMessage} req Request object to inspect
       * @return {Boolean} `true` if the request is valid, else `false`
       * @public
       */
      shouldHandle(req) {
        if (this.options.path) {
          const index = req.url.indexOf("?");
          const pathname = index !== -1 ? req.url.slice(0, index) : req.url;
          if (pathname !== this.options.path) return false;
        }
        return true;
      }
      /**
       * Handle a HTTP Upgrade request.
       *
       * @param {http.IncomingMessage} req The request object
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Function} cb Callback
       * @public
       */
      handleUpgrade(req, socket, head, cb) {
        socket.on("error", socketOnError);
        const key = req.headers["sec-websocket-key"];
        const upgrade = req.headers.upgrade;
        const version = +req.headers["sec-websocket-version"];
        if (req.method !== "GET") {
          const message = "Invalid HTTP method";
          abortHandshakeOrEmitwsClientError(this, req, socket, 405, message);
          return;
        }
        if (upgrade === void 0 || upgrade.toLowerCase() !== "websocket") {
          const message = "Invalid Upgrade header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
          return;
        }
        if (key === void 0 || !keyRegex.test(key)) {
          const message = "Missing or invalid Sec-WebSocket-Key header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
          return;
        }
        if (version !== 13 && version !== 8) {
          const message = "Missing or invalid Sec-WebSocket-Version header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message, {
            "Sec-WebSocket-Version": "13, 8"
          });
          return;
        }
        if (!this.shouldHandle(req)) {
          abortHandshake(socket, 400);
          return;
        }
        const secWebSocketProtocol = req.headers["sec-websocket-protocol"];
        let protocols = /* @__PURE__ */ new Set();
        if (secWebSocketProtocol !== void 0) {
          try {
            protocols = subprotocol2.parse(secWebSocketProtocol);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Protocol header";
            abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
            return;
          }
        }
        const secWebSocketExtensions = req.headers["sec-websocket-extensions"];
        const extensions = {};
        if (this.options.perMessageDeflate && secWebSocketExtensions !== void 0) {
          const perMessageDeflate = new PerMessageDeflate2({
            ...this.options.perMessageDeflate,
            isServer: true,
            maxPayload: this.options.maxPayload
          });
          try {
            const offers = extension2.parse(secWebSocketExtensions);
            if (offers[PerMessageDeflate2.extensionName]) {
              perMessageDeflate.accept(offers[PerMessageDeflate2.extensionName]);
              extensions[PerMessageDeflate2.extensionName] = perMessageDeflate;
            }
          } catch (err) {
            const message = "Invalid or unacceptable Sec-WebSocket-Extensions header";
            abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
            return;
          }
        }
        if (this.options.verifyClient) {
          const info = {
            origin: req.headers[`${version === 8 ? "sec-websocket-origin" : "origin"}`],
            secure: !!(req.socket.authorized || req.socket.encrypted),
            req
          };
          if (this.options.verifyClient.length === 2) {
            this.options.verifyClient(info, (verified, code, message, headers) => {
              if (!verified) {
                return abortHandshake(socket, code || 401, message, headers);
              }
              this.completeUpgrade(
                extensions,
                key,
                protocols,
                req,
                socket,
                head,
                cb
              );
            });
            return;
          }
          if (!this.options.verifyClient(info)) return abortHandshake(socket, 401);
        }
        this.completeUpgrade(extensions, key, protocols, req, socket, head, cb);
      }
      /**
       * Upgrade the connection to WebSocket.
       *
       * @param {Object} extensions The accepted extensions
       * @param {String} key The value of the `Sec-WebSocket-Key` header
       * @param {Set} protocols The subprotocols
       * @param {http.IncomingMessage} req The request object
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Function} cb Callback
       * @throws {Error} If called more than once with the same socket
       * @private
       */
      completeUpgrade(extensions, key, protocols, req, socket, head, cb) {
        if (!socket.readable || !socket.writable) return socket.destroy();
        if (socket[kWebSocket]) {
          throw new Error(
            "server.handleUpgrade() was called more than once with the same socket, possibly due to a misconfiguration"
          );
        }
        if (this._state > RUNNING) return abortHandshake(socket, 503);
        const digest = createHash3("sha1").update(key + GUID).digest("base64");
        const headers = [
          "HTTP/1.1 101 Switching Protocols",
          "Upgrade: websocket",
          "Connection: Upgrade",
          `Sec-WebSocket-Accept: ${digest}`
        ];
        const ws = new this.options.WebSocket(null, void 0, this.options);
        if (protocols.size) {
          const protocol = this.options.handleProtocols ? this.options.handleProtocols(protocols, req) : protocols.values().next().value;
          if (protocol) {
            headers.push(`Sec-WebSocket-Protocol: ${protocol}`);
            ws._protocol = protocol;
          }
        }
        if (extensions[PerMessageDeflate2.extensionName]) {
          const params = extensions[PerMessageDeflate2.extensionName].params;
          const value = extension2.format({
            [PerMessageDeflate2.extensionName]: [params]
          });
          headers.push(`Sec-WebSocket-Extensions: ${value}`);
          ws._extensions = extensions;
        }
        this.emit("headers", headers, req);
        socket.write(headers.concat("\r\n").join("\r\n"));
        socket.removeListener("error", socketOnError);
        ws.setSocket(socket, head, {
          allowSynchronousEvents: this.options.allowSynchronousEvents,
          maxBufferedChunks: this.options.maxBufferedChunks,
          maxFragments: this.options.maxFragments,
          maxPayload: this.options.maxPayload,
          skipUTF8Validation: this.options.skipUTF8Validation
        });
        if (this.clients) {
          this.clients.add(ws);
          ws.on("close", () => {
            this.clients.delete(ws);
            if (this._shouldEmitClose && !this.clients.size) {
              process.nextTick(emitClose, this);
            }
          });
        }
        cb(ws, req);
      }
    };
    module.exports = WebSocketServer2;
    function addListeners(server, map) {
      for (const event of Object.keys(map)) server.on(event, map[event]);
      return function removeListeners() {
        for (const event of Object.keys(map)) {
          server.removeListener(event, map[event]);
        }
      };
    }
    function emitClose(server) {
      server._state = CLOSED;
      server.emit("close");
    }
    function socketOnError() {
      this.destroy();
    }
    function abortHandshake(socket, code, message, headers) {
      message = message || http3.STATUS_CODES[code];
      headers = {
        Connection: "close",
        "Content-Type": "text/html",
        "Content-Length": Buffer.byteLength(message),
        ...headers
      };
      socket.once("finish", socket.destroy);
      socket.end(
        `HTTP/1.1 ${code} ${http3.STATUS_CODES[code]}\r
` + Object.keys(headers).map((h) => `${h}: ${headers[h]}`).join("\r\n") + "\r\n\r\n" + message
      );
    }
    function abortHandshakeOrEmitwsClientError(server, req, socket, code, message, headers) {
      if (server.listenerCount("wsClientError")) {
        const err = new Error(message);
        Error.captureStackTrace(err, abortHandshakeOrEmitwsClientError);
        server.emit("wsClientError", err, socket, req);
      } else {
        abortHandshake(socket, code, message, headers);
      }
    }
  }
});

// node_modules/ws/wrapper.mjs
var import_stream, import_extension, import_permessage_deflate, import_receiver, import_sender, import_subprotocol, import_websocket, import_websocket_server, wrapper_default;
var init_wrapper = __esm({
  "node_modules/ws/wrapper.mjs"() {
    import_stream = __toESM(require_stream(), 1);
    import_extension = __toESM(require_extension(), 1);
    import_permessage_deflate = __toESM(require_permessage_deflate(), 1);
    import_receiver = __toESM(require_receiver(), 1);
    import_sender = __toESM(require_sender(), 1);
    import_subprotocol = __toESM(require_subprotocol(), 1);
    import_websocket = __toESM(require_websocket(), 1);
    import_websocket_server = __toESM(require_websocket_server(), 1);
    wrapper_default = import_websocket.default;
  }
});

// node_modules/axios/lib/helpers/bind.js
function bind(fn, thisArg) {
  return function wrap() {
    return fn.apply(thisArg, arguments);
  };
}
var init_bind = __esm({
  "node_modules/axios/lib/helpers/bind.js"() {
    "use strict";
  }
});

// node_modules/axios/lib/utils.js
function isBuffer(val) {
  return val !== null && !isUndefined(val) && val.constructor !== null && !isUndefined(val.constructor) && isFunction(val.constructor.isBuffer) && val.constructor.isBuffer(val);
}
function isArrayBufferView(val) {
  let result;
  if (typeof ArrayBuffer !== "undefined" && ArrayBuffer.isView) {
    result = ArrayBuffer.isView(val);
  } else {
    result = val && val.buffer && isArrayBuffer(val.buffer);
  }
  return result;
}
function getGlobal() {
  if (typeof globalThis !== "undefined") return globalThis;
  if (typeof self !== "undefined") return self;
  if (typeof window !== "undefined") return window;
  if (typeof global !== "undefined") return global;
  return {};
}
function forEach(obj, fn, { allOwnKeys = false } = {}) {
  if (obj === null || typeof obj === "undefined") {
    return;
  }
  let i;
  let l;
  if (typeof obj !== "object") {
    obj = [obj];
  }
  if (isArray(obj)) {
    for (i = 0, l = obj.length; i < l; i++) {
      fn.call(null, obj[i], i, obj);
    }
  } else {
    if (isBuffer(obj)) {
      return;
    }
    const keys = allOwnKeys ? Object.getOwnPropertyNames(obj) : Object.keys(obj);
    const len = keys.length;
    let key;
    for (i = 0; i < len; i++) {
      key = keys[i];
      fn.call(null, obj[key], key, obj);
    }
  }
}
function findKey(obj, key) {
  if (isBuffer(obj)) {
    return null;
  }
  key = key.toLowerCase();
  const keys = Object.keys(obj);
  let i = keys.length;
  let _key;
  while (i-- > 0) {
    _key = keys[i];
    if (key === _key.toLowerCase()) {
      return _key;
    }
  }
  return null;
}
function merge(...objs) {
  const { caseless, skipUndefined } = isContextDefined(this) && this || {};
  const result = {};
  const lowerCaseKeys = caseless ? /* @__PURE__ */ Object.create(null) : null;
  const assignValue = (val, key) => {
    if (key === "__proto__" || key === "constructor" || key === "prototype") {
      return;
    }
    let targetKey = key;
    if (lowerCaseKeys && typeof key === "string") {
      const lowerKey = key.toLowerCase();
      const knownKey = lowerCaseKeys[lowerKey];
      if (knownKey !== void 0 && hasOwnProperty(result, knownKey)) {
        targetKey = knownKey;
      } else {
        lowerCaseKeys[lowerKey] = key;
      }
    }
    const existing = hasOwnProperty(result, targetKey) ? result[targetKey] : void 0;
    if (isPlainObject(existing) && isPlainObject(val)) {
      result[targetKey] = merge(existing, val);
    } else if (isPlainObject(val)) {
      result[targetKey] = merge({}, val);
    } else if (isArray(val)) {
      result[targetKey] = val.slice();
    } else if (!skipUndefined || !isUndefined(val)) {
      result[targetKey] = val;
    }
  };
  for (let i = 0, l = objs.length; i < l; i++) {
    const source = objs[i];
    if (!source || isBuffer(source)) {
      continue;
    }
    forEach(source, assignValue);
    if (typeof source !== "object" || isArray(source) || !Object.getOwnPropertySymbols) {
      continue;
    }
    const symbols = Object.getOwnPropertySymbols(source);
    for (let j = 0; j < symbols.length; j++) {
      const symbol = symbols[j];
      if (propertyIsEnumerable.call(source, symbol)) {
        assignValue(source[symbol], symbol);
      }
    }
  }
  return result;
}
function isSpecCompliantForm(thing) {
  return !!(thing && isFunction(thing.append) && thing[toStringTag] === "FormData" && thing[iterator]);
}
var toString, getPrototypeOf, iterator, toStringTag, hasOwnProperty, isUnsafeObjectKey, isPrototypeBoundary, isSafeAndFullyMutable, hasOwnInPrototypeChain, getSafeProp, toSafeFlatObject, kindOf, kindOfTest, typeOfTest, isArray, isUndefined, isArrayBuffer, isString, isFunction, isNumber, isObject, isBoolean, isPlainObject, isEmptyObject, isDate, isFile, isReactNativeBlob, isReactNative, isBlob, isFileList, isSet, isStream, G, FormDataCtor, isFormData, isURLSearchParams, isReadableStream, isRequest, isResponse, isHeaders, trim, _global, isContextDefined, extend, stripBOM, inherits, toFlatObject, endsWith, toArray, isTypedArray, forEachEntry, matchAll, isHTMLForm, toCamelCase, propertyIsEnumerable, isRegExp, reduceDescriptors, freezeMethods, toObjectSet, noop, toFiniteNumber, toJSONObject, isAsyncFn, isThenable, _setImmediate, asap, isIterable, isSafeIterable, utils_default;
var init_utils = __esm({
  "node_modules/axios/lib/utils.js"() {
    "use strict";
    init_bind();
    ({ toString } = Object.prototype);
    ({ getPrototypeOf } = Object);
    ({ iterator, toStringTag } = Symbol);
    hasOwnProperty = (({ hasOwnProperty: hasOwnProperty2 }) => (obj, prop) => hasOwnProperty2.call(obj, prop))(Object.prototype);
    isUnsafeObjectKey = (prop) => typeof prop === "string" && (prop === "__proto__" || prop === "constructor" || prop === "prototype");
    isPrototypeBoundary = (obj, prototype2, source) => obj === Object.prototype || !source && prototype2 === null;
    isSafeAndFullyMutable = (obj) => {
      if (!Object.isExtensible(obj)) {
        return false;
      }
      const props = Object.getOwnPropertyNames(obj);
      if (Object.getOwnPropertySymbols) {
        props.push(...Object.getOwnPropertySymbols(obj));
      }
      return props.every((prop) => {
        if (isUnsafeObjectKey(prop)) {
          return false;
        }
        const descriptor = Object.getOwnPropertyDescriptor(obj, prop);
        return !!descriptor && descriptor.configurable && descriptor.writable === true;
      });
    };
    hasOwnInPrototypeChain = (thing, prop) => {
      let obj = thing;
      const seen = [];
      while (obj != null) {
        if (seen.indexOf(obj) !== -1) {
          return false;
        }
        seen.push(obj);
        const prototype2 = getPrototypeOf(obj);
        if (isPrototypeBoundary(obj, prototype2, obj === thing)) {
          return false;
        }
        if (hasOwnProperty(obj, prop)) {
          return true;
        }
        obj = prototype2;
      }
      return false;
    };
    getSafeProp = (obj, prop) => obj != null && hasOwnInPrototypeChain(obj, prop) ? obj[prop] : void 0;
    toSafeFlatObject = (thing) => {
      if (thing == null || typeof thing !== "object" && typeof thing !== "function") {
        return thing;
      }
      const sourcePrototype = getPrototypeOf(thing);
      if (sourcePrototype === null && isSafeAndFullyMutable(thing)) {
        return thing;
      }
      const result = /* @__PURE__ */ Object.create(null);
      const merged = /* @__PURE__ */ Object.create(null);
      const seen = [];
      let current = thing;
      while (current != null) {
        if (seen.indexOf(current) !== -1) {
          break;
        }
        seen.push(current);
        const prototype2 = current === thing ? sourcePrototype : getPrototypeOf(current);
        if (isPrototypeBoundary(current, prototype2, current === thing)) {
          break;
        }
        const props = Object.getOwnPropertyNames(current);
        if (Object.getOwnPropertySymbols) {
          props.push(...Object.getOwnPropertySymbols(current));
        }
        for (const prop of props) {
          if (isUnsafeObjectKey(prop)) {
            continue;
          }
          if (!hasOwnProperty(merged, prop)) {
            result[prop] = thing[prop];
            merged[prop] = true;
          }
        }
        current = prototype2;
      }
      return result;
    };
    kindOf = /* @__PURE__ */ ((cache) => (thing) => {
      const str = toString.call(thing);
      return cache[str] || (cache[str] = str.slice(8, -1).toLowerCase());
    })(/* @__PURE__ */ Object.create(null));
    kindOfTest = (type) => {
      type = type.toLowerCase();
      return (thing) => kindOf(thing) === type;
    };
    typeOfTest = (type) => (thing) => typeof thing === type;
    ({ isArray } = Array);
    isUndefined = typeOfTest("undefined");
    isArrayBuffer = kindOfTest("ArrayBuffer");
    isString = typeOfTest("string");
    isFunction = typeOfTest("function");
    isNumber = typeOfTest("number");
    isObject = (thing) => thing !== null && typeof thing === "object";
    isBoolean = (thing) => thing === true || thing === false;
    isPlainObject = (val) => {
      if (!isObject(val)) {
        return false;
      }
      const prototype2 = getPrototypeOf(val);
      return (prototype2 === null || prototype2 === Object.prototype || getPrototypeOf(prototype2) === null) && // Treat own Symbol.toStringTag or Symbol.iterator members as evidence the
      // value is tagged/iterable, ignoring members reachable only through shared
      // or terminal prototype boundaries. This is exactly what
      // hasOwnInPrototypeChain(val, prop) computes here: every prototype admitted
      // above (null, Object.prototype, or a terminal null-prototype template) is
      // a boundary, so the walk never inspects anything but `val`'s own members,
      // and stops before even those when `val` is itself the boundary
      // Object.prototype. Checking that inline avoids allocating the walker's
      // `seen` list twice per call on the mergeConfig / merge hot path.
      (isPrototypeBoundary(val, prototype2, true) || !hasOwnProperty(val, toStringTag) && !hasOwnProperty(val, iterator));
    };
    isEmptyObject = (val) => {
      if (!isObject(val) || isBuffer(val)) {
        return false;
      }
      try {
        return Object.keys(val).length === 0 && Object.getPrototypeOf(val) === Object.prototype;
      } catch (e) {
        return false;
      }
    };
    isDate = kindOfTest("Date");
    isFile = kindOfTest("File");
    isReactNativeBlob = (value) => {
      return !!(value && typeof value.uri !== "undefined");
    };
    isReactNative = (formData) => formData && typeof formData.getParts !== "undefined";
    isBlob = kindOfTest("Blob");
    isFileList = kindOfTest("FileList");
    isSet = kindOfTest("Set");
    isStream = (val) => isObject(val) && isFunction(val.pipe);
    G = getGlobal();
    FormDataCtor = typeof G.FormData !== "undefined" ? G.FormData : void 0;
    isFormData = (thing) => {
      if (!thing) return false;
      if (FormDataCtor && thing instanceof FormDataCtor) return true;
      const proto = getPrototypeOf(thing);
      if (!proto || proto === Object.prototype) return false;
      if (!isFunction(thing.append)) return false;
      const kind = kindOf(thing);
      return kind === "formdata" || // detect form-data instance
      kind === "object" && isFunction(thing.toString) && thing.toString() === "[object FormData]";
    };
    isURLSearchParams = kindOfTest("URLSearchParams");
    [isReadableStream, isRequest, isResponse, isHeaders] = [
      "ReadableStream",
      "Request",
      "Response",
      "Headers"
    ].map(kindOfTest);
    trim = (str) => {
      return str.trim ? str.trim() : str.replace(/^[\s\uFEFF\xA0]+|[\s\uFEFF\xA0]+$/g, "");
    };
    _global = (() => {
      if (typeof globalThis !== "undefined") return globalThis;
      return typeof self !== "undefined" ? self : typeof window !== "undefined" ? window : global;
    })();
    isContextDefined = (context) => !isUndefined(context) && context !== _global;
    extend = (a, b, thisArg, { allOwnKeys } = {}) => {
      forEach(
        b,
        (val, key) => {
          if (thisArg && isFunction(val)) {
            Object.defineProperty(a, key, {
              // Null-proto descriptor so a polluted Object.prototype.get cannot
              // hijack defineProperty's accessor-vs-data resolution.
              __proto__: null,
              value: bind(val, thisArg),
              writable: true,
              enumerable: true,
              configurable: true
            });
          } else {
            Object.defineProperty(a, key, {
              __proto__: null,
              value: val,
              writable: true,
              enumerable: true,
              configurable: true
            });
          }
        },
        { allOwnKeys }
      );
      return a;
    };
    stripBOM = (content) => {
      if (content.charCodeAt(0) === 65279) {
        content = content.slice(1);
      }
      return content;
    };
    inherits = (constructor, superConstructor, props, descriptors) => {
      constructor.prototype = Object.create(superConstructor.prototype, descriptors);
      Object.defineProperty(constructor.prototype, "constructor", {
        __proto__: null,
        value: constructor,
        writable: true,
        enumerable: false,
        configurable: true
      });
      Object.defineProperty(constructor, "super", {
        __proto__: null,
        value: superConstructor.prototype
      });
      props && Object.assign(constructor.prototype, props);
    };
    toFlatObject = (sourceObj, destObj, filter2, propFilter) => {
      let props;
      let i;
      let prop;
      const merged = {};
      destObj = destObj || {};
      if (sourceObj == null) return destObj;
      do {
        props = Object.getOwnPropertyNames(sourceObj);
        i = props.length;
        while (i-- > 0) {
          prop = props[i];
          if ((!propFilter || propFilter(prop, sourceObj, destObj)) && !merged[prop]) {
            destObj[prop] = sourceObj[prop];
            merged[prop] = true;
          }
        }
        sourceObj = filter2 !== false && getPrototypeOf(sourceObj);
      } while (sourceObj && (!filter2 || filter2(sourceObj, destObj)) && sourceObj !== Object.prototype);
      return destObj;
    };
    endsWith = (str, searchString, position) => {
      str = String(str);
      if (position === void 0 || position > str.length) {
        position = str.length;
      }
      position -= searchString.length;
      const lastIndex = str.indexOf(searchString, position);
      return lastIndex !== -1 && lastIndex === position;
    };
    toArray = (thing) => {
      if (!thing) return null;
      if (isArray(thing)) return thing;
      let i = thing.length;
      if (!Number.isSafeInteger(i) || i < 0 || i > 4294967295) return null;
      const indexes = /* @__PURE__ */ new Set();
      const seen = /* @__PURE__ */ new Set();
      let source = thing;
      while (source != null && indexes.size < i) {
        if (seen.has(source)) return null;
        seen.add(source);
        const prototype2 = getPrototypeOf(source);
        if (isPrototypeBoundary(source, prototype2, source === thing)) break;
        Object.getOwnPropertyNames(source).forEach((key) => {
          const index = +key;
          if (Number.isInteger(index) && index >= 0 && index < i && String(index) === key && hasOwnProperty(source, key)) {
            indexes.add(index);
          }
        });
        source = prototype2;
      }
      if (indexes.size !== i) return null;
      const arr = new Array(i);
      while (i-- > 0) {
        arr[i] = thing[i];
      }
      return arr;
    };
    isTypedArray = /* @__PURE__ */ ((TypedArray) => {
      return (thing) => {
        return TypedArray && thing instanceof TypedArray;
      };
    })(typeof Uint8Array !== "undefined" && getPrototypeOf(Uint8Array));
    forEachEntry = (obj, fn) => {
      const generator = obj && obj[iterator];
      const _iterator = generator.call(obj);
      let result;
      while ((result = _iterator.next()) && !result.done) {
        const pair = result.value;
        fn.call(obj, pair[0], pair[1]);
      }
    };
    matchAll = (regExp, str) => {
      let matches;
      const arr = [];
      while ((matches = regExp.exec(str)) !== null) {
        arr.push(matches);
      }
      return arr;
    };
    isHTMLForm = kindOfTest("HTMLFormElement");
    toCamelCase = (str) => {
      return str.toLowerCase().replace(/[-_\s]([a-z\d])(\w*)/g, function replacer(m, p1, p2) {
        return p1.toUpperCase() + p2;
      });
    };
    ({ propertyIsEnumerable } = Object.prototype);
    isRegExp = kindOfTest("RegExp");
    reduceDescriptors = (obj, reducer) => {
      const descriptors = Object.getOwnPropertyDescriptors(obj);
      const reducedDescriptors = {};
      forEach(descriptors, (descriptor, name) => {
        let ret;
        if ((ret = reducer(descriptor, name, obj)) !== false) {
          reducedDescriptors[name] = ret || descriptor;
        }
      });
      Object.defineProperties(obj, reducedDescriptors);
    };
    freezeMethods = (obj) => {
      reduceDescriptors(obj, (descriptor, name) => {
        if (isFunction(obj) && ["arguments", "caller", "callee"].includes(name)) {
          return false;
        }
        const value = obj[name];
        if (!isFunction(value)) return;
        descriptor.enumerable = false;
        if ("writable" in descriptor) {
          descriptor.writable = false;
          return;
        }
        if (!descriptor.set) {
          descriptor.set = () => {
            throw Error("Can not rewrite read-only method '" + name + "'");
          };
        }
      });
    };
    toObjectSet = (arrayOrString, delimiter) => {
      const obj = {};
      const define = (arr) => {
        arr.forEach((value) => {
          obj[value] = true;
        });
      };
      isArray(arrayOrString) ? define(arrayOrString) : define(String(arrayOrString).split(delimiter));
      return obj;
    };
    noop = () => {
    };
    toFiniteNumber = (value, defaultValue) => {
      return value != null && Number.isFinite(value = +value) ? value : defaultValue;
    };
    toJSONObject = (obj) => {
      const visited = /* @__PURE__ */ new WeakSet();
      const visit = (source) => {
        if (isObject(source)) {
          if (visited.has(source)) {
            return;
          }
          if (isBuffer(source)) {
            return source;
          }
          if (!("toJSON" in source)) {
            visited.add(source);
            let target;
            if (isSet(source)) {
              target = [];
              for (const value of source) {
                const reducedValue = visit(value);
                !isUndefined(reducedValue) && target.push(reducedValue);
              }
            } else {
              target = isArray(source) ? [] : {};
              forEach(source, (value, key) => {
                const reducedValue = visit(value);
                !isUndefined(reducedValue) && (target[key] = reducedValue);
              });
            }
            visited.delete(source);
            return target;
          }
        }
        return source;
      };
      return visit(obj);
    };
    isAsyncFn = kindOfTest("AsyncFunction");
    isThenable = (thing) => thing && (isObject(thing) || isFunction(thing)) && isFunction(thing.then) && isFunction(thing.catch);
    _setImmediate = ((setImmediateSupported, postMessageSupported) => {
      if (setImmediateSupported) {
        return setImmediate;
      }
      return postMessageSupported ? ((token, callbacks) => {
        _global.addEventListener(
          "message",
          ({ source, data }) => {
            if (source === _global && data === token) {
              callbacks.length && callbacks.shift()();
            }
          },
          false
        );
        return (cb) => {
          callbacks.push(cb);
          _global.postMessage(token, "*");
        };
      })(`axios@${Math.random()}`, []) : (cb) => setTimeout(cb);
    })(typeof setImmediate === "function", isFunction(_global.postMessage));
    asap = typeof queueMicrotask !== "undefined" ? queueMicrotask.bind(_global) : typeof process !== "undefined" && process.nextTick || _setImmediate;
    isIterable = (thing) => thing != null && isFunction(thing[iterator]);
    isSafeIterable = (thing) => thing != null && hasOwnInPrototypeChain(thing, iterator) && isIterable(thing);
    utils_default = {
      isArray,
      isArrayBuffer,
      isBuffer,
      isFormData,
      isArrayBufferView,
      isString,
      isNumber,
      isBoolean,
      isObject,
      isPlainObject,
      isEmptyObject,
      isReadableStream,
      isRequest,
      isResponse,
      isHeaders,
      isUndefined,
      isDate,
      isFile,
      isReactNativeBlob,
      isReactNative,
      isBlob,
      isRegExp,
      isFunction,
      isStream,
      isURLSearchParams,
      isTypedArray,
      isFileList,
      forEach,
      merge,
      extend,
      trim,
      stripBOM,
      inherits,
      toFlatObject,
      kindOf,
      kindOfTest,
      endsWith,
      toArray,
      forEachEntry,
      matchAll,
      isHTMLForm,
      hasOwnProperty,
      hasOwnProp: hasOwnProperty,
      // an alias to avoid ESLint no-prototype-builtins detection
      hasOwnInPrototypeChain,
      getSafeProp,
      toSafeFlatObject,
      reduceDescriptors,
      freezeMethods,
      toObjectSet,
      toCamelCase,
      noop,
      toFiniteNumber,
      findKey,
      global: _global,
      isContextDefined,
      isSpecCompliantForm,
      toJSONObject,
      isAsyncFn,
      isThenable,
      setImmediate: _setImmediate,
      asap,
      isIterable,
      isSafeIterable
    };
  }
});

// node_modules/axios/lib/helpers/parseHeaders.js
var ignoreDuplicateOf, parseHeaders_default;
var init_parseHeaders = __esm({
  "node_modules/axios/lib/helpers/parseHeaders.js"() {
    "use strict";
    init_utils();
    ignoreDuplicateOf = utils_default.toObjectSet([
      "age",
      "authorization",
      "content-length",
      "content-type",
      "etag",
      "expires",
      "from",
      "host",
      "if-modified-since",
      "if-unmodified-since",
      "last-modified",
      "location",
      "max-forwards",
      "proxy-authorization",
      "referer",
      "retry-after",
      "user-agent"
    ]);
    parseHeaders_default = (rawHeaders) => {
      const parsed = /* @__PURE__ */ Object.create(null);
      let key;
      let val;
      let i;
      rawHeaders && rawHeaders.split("\n").forEach(function parser(line) {
        i = line.indexOf(":");
        key = line.substring(0, i).trim().toLowerCase();
        val = line.substring(i + 1).trim();
        const hasKey = utils_default.hasOwnProp(parsed, key);
        if (!key || hasKey && utils_default.hasOwnProp(ignoreDuplicateOf, key)) {
          return;
        }
        if (key === "set-cookie") {
          if (hasKey) {
            parsed[key].push(val);
          } else {
            parsed[key] = [val];
          }
        } else {
          parsed[key] = hasKey ? parsed[key] + ", " + val : val;
        }
      });
      return parsed;
    };
  }
});

// node_modules/axios/lib/helpers/sanitizeHeaderValue.js
function trimSPorHTAB(str) {
  let start = 0;
  let end = str.length;
  while (start < end) {
    const code = str.charCodeAt(start);
    if (code !== 9 && code !== 32) {
      break;
    }
    start += 1;
  }
  while (end > start) {
    const code = str.charCodeAt(end - 1);
    if (code !== 9 && code !== 32) {
      break;
    }
    end -= 1;
  }
  return start === 0 && end === str.length ? str : str.slice(start, end);
}
function sanitizeValue(value, invalidChars) {
  if (utils_default.isArray(value)) {
    return value.map((item) => sanitizeValue(item, invalidChars));
  }
  return trimSPorHTAB(String(value).replace(invalidChars, ""));
}
function toByteStringHeaderObject(headers) {
  const byteStringHeaders = /* @__PURE__ */ Object.create(null);
  utils_default.forEach(headers.toJSON(), (value, header) => {
    byteStringHeaders[header] = sanitizeByteStringHeaderValue(value);
  });
  return byteStringHeaders;
}
var INVALID_UNICODE_HEADER_VALUE_CHARS, INVALID_BYTE_STRING_HEADER_VALUE_CHARS, sanitizeHeaderValue, sanitizeByteStringHeaderValue;
var init_sanitizeHeaderValue = __esm({
  "node_modules/axios/lib/helpers/sanitizeHeaderValue.js"() {
    "use strict";
    init_utils();
    INVALID_UNICODE_HEADER_VALUE_CHARS = new RegExp("[\\u0000-\\u0008\\u000a-\\u001f\\u007f]+", "g");
    INVALID_BYTE_STRING_HEADER_VALUE_CHARS = new RegExp("[^\\u0009\\u0020-\\u007e\\u0080-\\u00ff]+", "g");
    sanitizeHeaderValue = (value) => sanitizeValue(value, INVALID_UNICODE_HEADER_VALUE_CHARS);
    sanitizeByteStringHeaderValue = (value) => sanitizeValue(value, INVALID_BYTE_STRING_HEADER_VALUE_CHARS);
  }
});

// node_modules/axios/lib/core/AxiosHeaders.js
function normalizeHeader(header) {
  return header && String(header).trim().toLowerCase();
}
function normalizeValue(value) {
  if (value === false || value == null) {
    return value;
  }
  return utils_default.isArray(value) ? value.map(normalizeValue) : sanitizeHeaderValue(String(value));
}
function parseTokens(str) {
  const tokens = /* @__PURE__ */ Object.create(null);
  const tokensRE = /([^\s,;=]+)\s*(?:=\s*([^,;]+))?/g;
  let match;
  while (match = tokensRE.exec(str)) {
    tokens[match[1]] = match[2];
  }
  return tokens;
}
function trimOWS(value) {
  let start = 0;
  let end = value.length;
  while (start < end) {
    const code = value.charCodeAt(start);
    if (code !== 9 && code !== 32) {
      break;
    }
    start += 1;
  }
  while (end > start) {
    const code = value.charCodeAt(end - 1);
    if (code !== 9 && code !== 32) {
      break;
    }
    end -= 1;
  }
  return start === 0 && end === value.length ? value : value.slice(start, end);
}
function decodeQuotedString(value) {
  const last = value.length - 1;
  if (last < 1 || value.charCodeAt(0) !== 34 || value.charCodeAt(last) !== 34) {
    return value;
  }
  let decoded = "";
  for (let i = 1; i < last; i++) {
    const code = value.charCodeAt(i);
    if (code === 34) {
      return value;
    }
    if (code === 92) {
      i += 1;
      if (i >= last) {
        return value;
      }
    }
    decoded += value[i];
  }
  return decoded;
}
function parseParameters(value) {
  const parameters = /* @__PURE__ */ Object.create(null);
  const str = String(value);
  let start = 0;
  let quoted = false;
  let escaped = false;
  function parseParameter(end) {
    const part = trimOWS(str.slice(start, end));
    const equals = part.indexOf("=");
    if (equals < 1) {
      return;
    }
    const name = trimOWS(part.slice(0, equals));
    if (!parameterNameRE.test(name)) {
      return;
    }
    const normalizedName = name.toLowerCase();
    if (normalizedName === "__proto__" || normalizedName === "constructor" || normalizedName === "prototype") {
      return;
    }
    const parameterValue = trimOWS(part.slice(equals + 1));
    parameters[normalizedName] = decodeQuotedString(parameterValue);
  }
  for (let i = 0; i < str.length; i++) {
    const code = str.charCodeAt(i);
    if (quoted) {
      if (escaped) {
        escaped = false;
      } else if (code === 92) {
        escaped = true;
      } else if (code === 34) {
        quoted = false;
      }
    } else if (code === 34) {
      quoted = true;
    } else if (code === 44 || code === 59) {
      parseParameter(i);
      start = i + 1;
    }
  }
  parseParameter(str.length);
  return parameters;
}
function matchHeaderValue(context, value, header, filter2, isHeaderNameFilter) {
  if (utils_default.isFunction(filter2)) {
    return filter2.call(context, value, header);
  }
  if (isHeaderNameFilter) {
    value = header;
  }
  if (!utils_default.isString(value)) return;
  if (utils_default.isString(filter2)) {
    return value.indexOf(filter2) !== -1;
  }
  if (utils_default.isRegExp(filter2)) {
    return filter2.test(value);
  }
}
function formatHeader(header) {
  return header.trim().toLowerCase().replace(/([a-z\d])(\w*)/g, (w, char, str) => {
    return char.toUpperCase() + str;
  });
}
function buildAccessors(obj, header) {
  const accessorName = utils_default.toCamelCase(" " + header);
  ["get", "set", "has"].forEach((methodName) => {
    Object.defineProperty(obj, methodName + accessorName, {
      // Null-proto descriptor so a polluted Object.prototype.get cannot turn
      // this data descriptor into an accessor descriptor on the way in.
      __proto__: null,
      value: function(arg1, arg2, arg3) {
        return this[methodName].call(this, header, arg1, arg2, arg3);
      },
      configurable: true
    });
  });
}
var $internals, parameterNameRE, isValidHeaderName, PROTO_HEADER_SLOT, headerSlot, AxiosHeaders, AxiosHeaders_default;
var init_AxiosHeaders = __esm({
  "node_modules/axios/lib/core/AxiosHeaders.js"() {
    "use strict";
    init_utils();
    init_parseHeaders();
    init_sanitizeHeaderValue();
    $internals = /* @__PURE__ */ Symbol("internals");
    parameterNameRE = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;
    isValidHeaderName = (str) => /^[-_a-zA-Z0-9^`|~,!#$%&'*+.]+$/.test(str.trim());
    PROTO_HEADER_SLOT = "__Proto__";
    headerSlot = (name) => name === "__proto__" ? PROTO_HEADER_SLOT : name;
    AxiosHeaders = class {
      constructor(headers) {
        headers && this.set(headers);
      }
      set(header, valueOrRewrite, rewrite) {
        const self2 = this;
        function setHeader(_value, _header, _rewrite) {
          const lHeader = normalizeHeader(_header);
          if (!lHeader) {
            return;
          }
          const key = utils_default.findKey(self2, lHeader);
          if (!key || self2[key] === void 0 || _rewrite === true || _rewrite === void 0 && self2[key] !== false) {
            self2[headerSlot(key || _header)] = normalizeValue(_value);
          }
        }
        const setHeaders = (headers, _rewrite) => utils_default.forEach(headers, (_value, _header) => setHeader(_value, _header, _rewrite));
        if (utils_default.isPlainObject(header) || header instanceof this.constructor) {
          setHeaders(header, valueOrRewrite);
        } else if (utils_default.isString(header) && (header = header.trim()) && !isValidHeaderName(header)) {
          setHeaders(parseHeaders_default(header), valueOrRewrite);
        } else if (utils_default.isObject(header) && utils_default.isSafeIterable(header)) {
          let obj = /* @__PURE__ */ Object.create(null), dest, key;
          for (const entry of header) {
            if (!utils_default.isArray(entry)) {
              throw new TypeError("Object iterator must return a key-value pair");
            }
            key = entry[0];
            if (utils_default.hasOwnProp(obj, key)) {
              dest = obj[key];
              obj[key] = utils_default.isArray(dest) ? [...dest, entry[1]] : [dest, entry[1]];
            } else {
              obj[key] = entry[1];
            }
          }
          setHeaders(obj, valueOrRewrite);
        } else {
          header != null && setHeader(valueOrRewrite, header, rewrite);
        }
        return this;
      }
      get(header, parser) {
        header = normalizeHeader(header);
        if (header) {
          const key = utils_default.findKey(this, header);
          if (key) {
            const value = this[key];
            if (!parser) {
              return value;
            }
            if (parser === true) {
              return parseTokens(value);
            }
            if (utils_default.isFunction(parser)) {
              return parser.call(this, value, key);
            }
            if (utils_default.isRegExp(parser)) {
              return parser.exec(value);
            }
            throw new TypeError("parser must be boolean|regexp|function");
          }
        }
      }
      has(header, matcher) {
        header = normalizeHeader(header);
        if (header) {
          const key = utils_default.findKey(this, header);
          return !!(key && this[key] !== void 0 && (!matcher || matchHeaderValue(this, this[key], key, matcher)));
        }
        return false;
      }
      delete(header, matcher) {
        const self2 = this;
        let deleted = false;
        function deleteHeader(_header) {
          _header = normalizeHeader(_header);
          if (_header) {
            const key = utils_default.findKey(self2, _header);
            if (key && (!matcher || matchHeaderValue(self2, self2[key], key, matcher))) {
              delete self2[key];
              deleted = true;
            }
          }
        }
        if (utils_default.isArray(header)) {
          header.forEach(deleteHeader);
        } else {
          deleteHeader(header);
        }
        return deleted;
      }
      clear(matcher) {
        const keys = Object.keys(this);
        let i = keys.length;
        let deleted = false;
        while (i--) {
          const key = keys[i];
          if (!matcher || matchHeaderValue(this, this[key], key, matcher, true)) {
            delete this[key];
            deleted = true;
          }
        }
        return deleted;
      }
      normalize(format) {
        const self2 = this;
        const headers = /* @__PURE__ */ Object.create(null);
        utils_default.forEach(this, (value, header) => {
          const key = utils_default.findKey(headers, header);
          if (key) {
            self2[key] = normalizeValue(value);
            delete self2[header];
            return;
          }
          const normalized = headerSlot(format ? formatHeader(header) : String(header).trim());
          if (normalized !== header) {
            delete self2[header];
          }
          self2[normalized] = normalizeValue(value);
          headers[normalized] = true;
        });
        return this;
      }
      concat(...targets) {
        return this.constructor.concat(this, ...targets);
      }
      toJSON(asStrings) {
        const obj = /* @__PURE__ */ Object.create(null);
        utils_default.forEach(this, (value, header) => {
          value != null && value !== false && (obj[header] = asStrings && utils_default.isArray(value) ? value.join(", ") : value);
        });
        return obj;
      }
      [Symbol.iterator]() {
        return Object.entries(this.toJSON())[Symbol.iterator]();
      }
      toString() {
        return Object.entries(this.toJSON()).map(([header, value]) => header + ": " + value).join("\n");
      }
      getSetCookie() {
        const value = this.get("set-cookie");
        return utils_default.isArray(value) ? value : value == null || value === false ? [] : [value];
      }
      get [Symbol.toStringTag]() {
        return "AxiosHeaders";
      }
      static from(thing) {
        return thing instanceof this ? thing : new this(thing);
      }
      static parseParameters(value) {
        return parseParameters(value);
      }
      static concat(first, ...targets) {
        const computed = new this(first);
        targets.forEach((target) => computed.set(target));
        return computed;
      }
      static accessor(header) {
        const internals = this[$internals] = this[$internals] = {
          accessors: {}
        };
        const accessors = internals.accessors;
        const prototype2 = this.prototype;
        function defineAccessor(_header) {
          const lHeader = normalizeHeader(_header);
          if (!accessors[lHeader]) {
            buildAccessors(prototype2, _header);
            accessors[lHeader] = true;
          }
        }
        utils_default.isArray(header) ? header.forEach(defineAccessor) : defineAccessor(header);
        return this;
      }
    };
    AxiosHeaders.accessor([
      "Content-Type",
      "Content-Length",
      "Accept",
      "Accept-Encoding",
      "User-Agent",
      "Authorization"
    ]);
    utils_default.reduceDescriptors(AxiosHeaders.prototype, ({ value }, key) => {
      let mapped = key[0].toUpperCase() + key.slice(1);
      return {
        get: () => value,
        set(headerValue) {
          this[mapped] = headerValue;
        }
      };
    });
    utils_default.freezeMethods(AxiosHeaders);
    AxiosHeaders_default = AxiosHeaders;
  }
});

// node_modules/axios/lib/core/AxiosError.js
function hasOwnOrPrototypeToJSON(source) {
  if (utils_default.hasOwnProp(source, "toJSON")) {
    return true;
  }
  let prototype2 = Object.getPrototypeOf(source);
  while (prototype2 && prototype2 !== Object.prototype) {
    if (utils_default.hasOwnProp(prototype2, "toJSON")) {
      return true;
    }
    prototype2 = Object.getPrototypeOf(prototype2);
  }
  return false;
}
function redactConfig(config, redactKeys) {
  const lowerKeys = new Set(redactKeys.map((k) => String(k).toLowerCase()));
  const seen = [];
  const visit = (source) => {
    if (source === null || typeof source !== "object") return source;
    if (utils_default.isBuffer(source)) return source;
    if (seen.indexOf(source) !== -1) return void 0;
    if (source instanceof AxiosHeaders_default) {
      source = source.toJSON();
    }
    seen.push(source);
    let result;
    if (utils_default.isArray(source)) {
      result = [];
      source.forEach((v, i) => {
        const reducedValue = visit(v);
        if (!utils_default.isUndefined(reducedValue)) {
          result[i] = reducedValue;
        }
      });
    } else {
      if (!utils_default.isPlainObject(source) && hasOwnOrPrototypeToJSON(source)) {
        seen.pop();
        return source;
      }
      result = /* @__PURE__ */ Object.create(null);
      for (const [key, value] of Object.entries(source)) {
        const reducedValue = lowerKeys.has(key.toLowerCase()) ? REDACTED : visit(value);
        if (!utils_default.isUndefined(reducedValue)) {
          result[key] = reducedValue;
        }
      }
    }
    seen.pop();
    return result;
  };
  return visit(config);
}
function stringifySafely(value) {
  try {
    return String(value);
  } catch (err) {
    return "";
  }
}
function aggregateErrorMessage(error) {
  const message = error.errors.map((entry) => {
    try {
      return entry && entry.message ? stringifySafely(entry.message) : stringifySafely(entry);
    } catch (err) {
      return "";
    }
  }).filter(Boolean).join("; ");
  return message || error.name || "AggregateError";
}
function getCauseStackIndex(error, stack) {
  const causeStack = utils_default.hasOwnProp(error, $causeStack) && error[$causeStack];
  return causeStack && stack.endsWith(causeStack) ? stack.length - causeStack.length : -1;
}
function appendCauseStack(target, source) {
  try {
    const sourceStack = source && source.stack;
    if (typeof sourceStack !== "string" || !sourceStack) {
      return;
    }
    const targetStack = target.stack;
    if (typeof targetStack !== "string" || !targetStack) {
      return;
    }
    const causeStack = CAUSED_BY_SEPARATOR + sourceStack;
    target.stack = targetStack + causeStack;
    Object.defineProperty(target, $causeStack, { __proto__: null, value: causeStack });
  } catch (e) {
  }
}
var REDACTED, CAUSED_BY_SEPARATOR, $causeStack, AxiosError, AxiosError_default;
var init_AxiosError = __esm({
  "node_modules/axios/lib/core/AxiosError.js"() {
    "use strict";
    init_utils();
    init_AxiosHeaders();
    REDACTED = "[REDACTED ****]";
    CAUSED_BY_SEPARATOR = "\nCaused by: ";
    $causeStack = /* @__PURE__ */ Symbol("causeStack");
    AxiosError = class _AxiosError extends Error {
      static from(error, code, config, request, response, customProps) {
        let message = error.message;
        if (!message && utils_default.isArray(error.errors) && error.errors.length) {
          message = aggregateErrorMessage(error);
        }
        const axiosError = new _AxiosError(message, code || error.code, config, request, response);
        Object.defineProperty(axiosError, "cause", {
          __proto__: null,
          value: error,
          writable: true,
          enumerable: false,
          configurable: true
        });
        axiosError.name = error.name;
        appendCauseStack(axiosError, error);
        if (error.status != null && axiosError.status == null) {
          axiosError.status = error.status;
        }
        customProps && Object.assign(axiosError, customProps);
        return axiosError;
      }
      /**
       * Create an Error with the specified message, config, error code, request and response.
       *
       * @param {string} message The error message.
       * @param {string} [code] The error code (for example, 'ECONNABORTED').
       * @param {Object} [config] The config.
       * @param {Object} [request] The request.
       * @param {Object} [response] The response.
       *
       * @returns {Error} The created error.
       */
      constructor(message, code, config, request, response) {
        super(message);
        Object.defineProperty(this, "message", {
          // Null-proto descriptor so a polluted Object.prototype.get cannot turn
          // this data descriptor into an accessor descriptor on the way in.
          __proto__: null,
          value: message,
          enumerable: true,
          writable: true,
          configurable: true
        });
        this.name = "AxiosError";
        this.isAxiosError = true;
        code && (this.code = code);
        config && (this.config = config);
        request && (this.request = request);
        if (response) {
          this.response = response;
          this.status = response.status;
        }
      }
      toJSON() {
        const config = this.config;
        const redactKeys = config && utils_default.hasOwnProp(config, "redact") ? config.redact : void 0;
        const serializedConfig = utils_default.isArray(redactKeys) && redactKeys.length > 0 ? redactConfig(config, redactKeys) : utils_default.toJSONObject(config);
        return {
          // Standard
          message: this.message,
          name: this.name,
          // Microsoft
          description: this.description,
          number: this.number,
          // Mozilla
          fileName: this.fileName,
          lineNumber: this.lineNumber,
          columnNumber: this.columnNumber,
          stack: this.stack,
          // Axios
          config: serializedConfig,
          code: this.code,
          status: this.status
        };
      }
    };
    AxiosError.ERR_BAD_OPTION_VALUE = "ERR_BAD_OPTION_VALUE";
    AxiosError.ERR_BAD_OPTION = "ERR_BAD_OPTION";
    AxiosError.ECONNABORTED = "ECONNABORTED";
    AxiosError.ETIMEDOUT = "ETIMEDOUT";
    AxiosError.ECONNREFUSED = "ECONNREFUSED";
    AxiosError.ERR_NETWORK = "ERR_NETWORK";
    AxiosError.ERR_FR_TOO_MANY_REDIRECTS = "ERR_FR_TOO_MANY_REDIRECTS";
    AxiosError.ERR_DEPRECATED = "ERR_DEPRECATED";
    AxiosError.ERR_BAD_RESPONSE = "ERR_BAD_RESPONSE";
    AxiosError.ERR_BAD_REQUEST = "ERR_BAD_REQUEST";
    AxiosError.ERR_CANCELED = "ERR_CANCELED";
    AxiosError.ERR_NOT_SUPPORT = "ERR_NOT_SUPPORT";
    AxiosError.ERR_INVALID_URL = "ERR_INVALID_URL";
    AxiosError.ERR_FORM_DATA_DEPTH_EXCEEDED = "ERR_FORM_DATA_DEPTH_EXCEEDED";
    AxiosError_default = AxiosError;
  }
});

// node_modules/delayed-stream/lib/delayed_stream.js
var require_delayed_stream = __commonJS({
  "node_modules/delayed-stream/lib/delayed_stream.js"(exports, module) {
    var Stream = __require("stream").Stream;
    var util4 = __require("util");
    module.exports = DelayedStream;
    function DelayedStream() {
      this.source = null;
      this.dataSize = 0;
      this.maxDataSize = 1024 * 1024;
      this.pauseStream = true;
      this._maxDataSizeExceeded = false;
      this._released = false;
      this._bufferedEvents = [];
    }
    util4.inherits(DelayedStream, Stream);
    DelayedStream.create = function(source, options) {
      var delayedStream = new this();
      options = options || {};
      for (var option in options) {
        delayedStream[option] = options[option];
      }
      delayedStream.source = source;
      var realEmit = source.emit;
      source.emit = function() {
        delayedStream._handleEmit(arguments);
        return realEmit.apply(source, arguments);
      };
      source.on("error", function() {
      });
      if (delayedStream.pauseStream) {
        source.pause();
      }
      return delayedStream;
    };
    Object.defineProperty(DelayedStream.prototype, "readable", {
      configurable: true,
      enumerable: true,
      get: function() {
        return this.source.readable;
      }
    });
    DelayedStream.prototype.setEncoding = function() {
      return this.source.setEncoding.apply(this.source, arguments);
    };
    DelayedStream.prototype.resume = function() {
      if (!this._released) {
        this.release();
      }
      this.source.resume();
    };
    DelayedStream.prototype.pause = function() {
      this.source.pause();
    };
    DelayedStream.prototype.release = function() {
      this._released = true;
      this._bufferedEvents.forEach(function(args) {
        this.emit.apply(this, args);
      }.bind(this));
      this._bufferedEvents = [];
    };
    DelayedStream.prototype.pipe = function() {
      var r = Stream.prototype.pipe.apply(this, arguments);
      this.resume();
      return r;
    };
    DelayedStream.prototype._handleEmit = function(args) {
      if (this._released) {
        this.emit.apply(this, args);
        return;
      }
      if (args[0] === "data") {
        this.dataSize += args[1].length;
        this._checkIfMaxDataSizeExceeded();
      }
      this._bufferedEvents.push(args);
    };
    DelayedStream.prototype._checkIfMaxDataSizeExceeded = function() {
      if (this._maxDataSizeExceeded) {
        return;
      }
      if (this.dataSize <= this.maxDataSize) {
        return;
      }
      this._maxDataSizeExceeded = true;
      var message = "DelayedStream#maxDataSize of " + this.maxDataSize + " bytes exceeded.";
      this.emit("error", new Error(message));
    };
  }
});

// node_modules/combined-stream/lib/combined_stream.js
var require_combined_stream = __commonJS({
  "node_modules/combined-stream/lib/combined_stream.js"(exports, module) {
    var util4 = __require("util");
    var Stream = __require("stream").Stream;
    var DelayedStream = require_delayed_stream();
    module.exports = CombinedStream;
    function CombinedStream() {
      this.writable = false;
      this.readable = true;
      this.dataSize = 0;
      this.maxDataSize = 2 * 1024 * 1024;
      this.pauseStreams = true;
      this._released = false;
      this._streams = [];
      this._currentStream = null;
      this._insideLoop = false;
      this._pendingNext = false;
    }
    util4.inherits(CombinedStream, Stream);
    CombinedStream.create = function(options) {
      var combinedStream = new this();
      options = options || {};
      for (var option in options) {
        combinedStream[option] = options[option];
      }
      return combinedStream;
    };
    CombinedStream.isStreamLike = function(stream4) {
      return typeof stream4 !== "function" && typeof stream4 !== "string" && typeof stream4 !== "boolean" && typeof stream4 !== "number" && !Buffer.isBuffer(stream4);
    };
    CombinedStream.prototype.append = function(stream4) {
      var isStreamLike = CombinedStream.isStreamLike(stream4);
      if (isStreamLike) {
        if (!(stream4 instanceof DelayedStream)) {
          var newStream = DelayedStream.create(stream4, {
            maxDataSize: Infinity,
            pauseStream: this.pauseStreams
          });
          stream4.on("data", this._checkDataSize.bind(this));
          stream4 = newStream;
        }
        this._handleErrors(stream4);
        if (this.pauseStreams) {
          stream4.pause();
        }
      }
      this._streams.push(stream4);
      return this;
    };
    CombinedStream.prototype.pipe = function(dest, options) {
      Stream.prototype.pipe.call(this, dest, options);
      this.resume();
      return dest;
    };
    CombinedStream.prototype._getNext = function() {
      this._currentStream = null;
      if (this._insideLoop) {
        this._pendingNext = true;
        return;
      }
      this._insideLoop = true;
      try {
        do {
          this._pendingNext = false;
          this._realGetNext();
        } while (this._pendingNext);
      } finally {
        this._insideLoop = false;
      }
    };
    CombinedStream.prototype._realGetNext = function() {
      var stream4 = this._streams.shift();
      if (typeof stream4 == "undefined") {
        this.end();
        return;
      }
      if (typeof stream4 !== "function") {
        this._pipeNext(stream4);
        return;
      }
      var getStream = stream4;
      getStream(function(stream5) {
        var isStreamLike = CombinedStream.isStreamLike(stream5);
        if (isStreamLike) {
          stream5.on("data", this._checkDataSize.bind(this));
          this._handleErrors(stream5);
        }
        this._pipeNext(stream5);
      }.bind(this));
    };
    CombinedStream.prototype._pipeNext = function(stream4) {
      this._currentStream = stream4;
      var isStreamLike = CombinedStream.isStreamLike(stream4);
      if (isStreamLike) {
        stream4.on("end", this._getNext.bind(this));
        stream4.pipe(this, { end: false });
        return;
      }
      var value = stream4;
      this.write(value);
      this._getNext();
    };
    CombinedStream.prototype._handleErrors = function(stream4) {
      var self2 = this;
      stream4.on("error", function(err) {
        self2._emitError(err);
      });
    };
    CombinedStream.prototype.write = function(data) {
      this.emit("data", data);
    };
    CombinedStream.prototype.pause = function() {
      if (!this.pauseStreams) {
        return;
      }
      if (this.pauseStreams && this._currentStream && typeof this._currentStream.pause == "function") this._currentStream.pause();
      this.emit("pause");
    };
    CombinedStream.prototype.resume = function() {
      if (!this._released) {
        this._released = true;
        this.writable = true;
        this._getNext();
      }
      if (this.pauseStreams && this._currentStream && typeof this._currentStream.resume == "function") this._currentStream.resume();
      this.emit("resume");
    };
    CombinedStream.prototype.end = function() {
      this._reset();
      this.emit("end");
    };
    CombinedStream.prototype.destroy = function() {
      this._reset();
      this.emit("close");
    };
    CombinedStream.prototype._reset = function() {
      this.writable = false;
      this._streams = [];
      this._currentStream = null;
    };
    CombinedStream.prototype._checkDataSize = function() {
      this._updateDataSize();
      if (this.dataSize <= this.maxDataSize) {
        return;
      }
      var message = "DelayedStream#maxDataSize of " + this.maxDataSize + " bytes exceeded.";
      this._emitError(new Error(message));
    };
    CombinedStream.prototype._updateDataSize = function() {
      this.dataSize = 0;
      var self2 = this;
      this._streams.forEach(function(stream4) {
        if (!stream4.dataSize) {
          return;
        }
        self2.dataSize += stream4.dataSize;
      });
      if (this._currentStream && this._currentStream.dataSize) {
        this.dataSize += this._currentStream.dataSize;
      }
    };
    CombinedStream.prototype._emitError = function(err) {
      this._reset();
      this.emit("error", err);
    };
  }
});

// node_modules/mime-db/db.json
var require_db = __commonJS({
  "node_modules/mime-db/db.json"(exports, module) {
    module.exports = {
      "application/1d-interleaved-parityfec": {
        source: "iana"
      },
      "application/3gpdash-qoe-report+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/3gpp-ims+xml": {
        source: "iana",
        compressible: true
      },
      "application/3gpphal+json": {
        source: "iana",
        compressible: true
      },
      "application/3gpphalforms+json": {
        source: "iana",
        compressible: true
      },
      "application/a2l": {
        source: "iana"
      },
      "application/ace+cbor": {
        source: "iana"
      },
      "application/activemessage": {
        source: "iana"
      },
      "application/activity+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-costmap+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-costmapfilter+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-directory+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-endpointcost+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-endpointcostparams+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-endpointprop+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-endpointpropparams+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-error+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-networkmap+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-networkmapfilter+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-updatestreamcontrol+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-updatestreamparams+json": {
        source: "iana",
        compressible: true
      },
      "application/aml": {
        source: "iana"
      },
      "application/andrew-inset": {
        source: "iana",
        extensions: ["ez"]
      },
      "application/applefile": {
        source: "iana"
      },
      "application/applixware": {
        source: "apache",
        extensions: ["aw"]
      },
      "application/at+jwt": {
        source: "iana"
      },
      "application/atf": {
        source: "iana"
      },
      "application/atfx": {
        source: "iana"
      },
      "application/atom+xml": {
        source: "iana",
        compressible: true,
        extensions: ["atom"]
      },
      "application/atomcat+xml": {
        source: "iana",
        compressible: true,
        extensions: ["atomcat"]
      },
      "application/atomdeleted+xml": {
        source: "iana",
        compressible: true,
        extensions: ["atomdeleted"]
      },
      "application/atomicmail": {
        source: "iana"
      },
      "application/atomsvc+xml": {
        source: "iana",
        compressible: true,
        extensions: ["atomsvc"]
      },
      "application/atsc-dwd+xml": {
        source: "iana",
        compressible: true,
        extensions: ["dwd"]
      },
      "application/atsc-dynamic-event-message": {
        source: "iana"
      },
      "application/atsc-held+xml": {
        source: "iana",
        compressible: true,
        extensions: ["held"]
      },
      "application/atsc-rdt+json": {
        source: "iana",
        compressible: true
      },
      "application/atsc-rsat+xml": {
        source: "iana",
        compressible: true,
        extensions: ["rsat"]
      },
      "application/atxml": {
        source: "iana"
      },
      "application/auth-policy+xml": {
        source: "iana",
        compressible: true
      },
      "application/bacnet-xdd+zip": {
        source: "iana",
        compressible: false
      },
      "application/batch-smtp": {
        source: "iana"
      },
      "application/bdoc": {
        compressible: false,
        extensions: ["bdoc"]
      },
      "application/beep+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/calendar+json": {
        source: "iana",
        compressible: true
      },
      "application/calendar+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xcs"]
      },
      "application/call-completion": {
        source: "iana"
      },
      "application/cals-1840": {
        source: "iana"
      },
      "application/captive+json": {
        source: "iana",
        compressible: true
      },
      "application/cbor": {
        source: "iana"
      },
      "application/cbor-seq": {
        source: "iana"
      },
      "application/cccex": {
        source: "iana"
      },
      "application/ccmp+xml": {
        source: "iana",
        compressible: true
      },
      "application/ccxml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["ccxml"]
      },
      "application/cdfx+xml": {
        source: "iana",
        compressible: true,
        extensions: ["cdfx"]
      },
      "application/cdmi-capability": {
        source: "iana",
        extensions: ["cdmia"]
      },
      "application/cdmi-container": {
        source: "iana",
        extensions: ["cdmic"]
      },
      "application/cdmi-domain": {
        source: "iana",
        extensions: ["cdmid"]
      },
      "application/cdmi-object": {
        source: "iana",
        extensions: ["cdmio"]
      },
      "application/cdmi-queue": {
        source: "iana",
        extensions: ["cdmiq"]
      },
      "application/cdni": {
        source: "iana"
      },
      "application/cea": {
        source: "iana"
      },
      "application/cea-2018+xml": {
        source: "iana",
        compressible: true
      },
      "application/cellml+xml": {
        source: "iana",
        compressible: true
      },
      "application/cfw": {
        source: "iana"
      },
      "application/city+json": {
        source: "iana",
        compressible: true
      },
      "application/clr": {
        source: "iana"
      },
      "application/clue+xml": {
        source: "iana",
        compressible: true
      },
      "application/clue_info+xml": {
        source: "iana",
        compressible: true
      },
      "application/cms": {
        source: "iana"
      },
      "application/cnrp+xml": {
        source: "iana",
        compressible: true
      },
      "application/coap-group+json": {
        source: "iana",
        compressible: true
      },
      "application/coap-payload": {
        source: "iana"
      },
      "application/commonground": {
        source: "iana"
      },
      "application/conference-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/cose": {
        source: "iana"
      },
      "application/cose-key": {
        source: "iana"
      },
      "application/cose-key-set": {
        source: "iana"
      },
      "application/cpl+xml": {
        source: "iana",
        compressible: true,
        extensions: ["cpl"]
      },
      "application/csrattrs": {
        source: "iana"
      },
      "application/csta+xml": {
        source: "iana",
        compressible: true
      },
      "application/cstadata+xml": {
        source: "iana",
        compressible: true
      },
      "application/csvm+json": {
        source: "iana",
        compressible: true
      },
      "application/cu-seeme": {
        source: "apache",
        extensions: ["cu"]
      },
      "application/cwt": {
        source: "iana"
      },
      "application/cybercash": {
        source: "iana"
      },
      "application/dart": {
        compressible: true
      },
      "application/dash+xml": {
        source: "iana",
        compressible: true,
        extensions: ["mpd"]
      },
      "application/dash-patch+xml": {
        source: "iana",
        compressible: true,
        extensions: ["mpp"]
      },
      "application/dashdelta": {
        source: "iana"
      },
      "application/davmount+xml": {
        source: "iana",
        compressible: true,
        extensions: ["davmount"]
      },
      "application/dca-rft": {
        source: "iana"
      },
      "application/dcd": {
        source: "iana"
      },
      "application/dec-dx": {
        source: "iana"
      },
      "application/dialog-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/dicom": {
        source: "iana"
      },
      "application/dicom+json": {
        source: "iana",
        compressible: true
      },
      "application/dicom+xml": {
        source: "iana",
        compressible: true
      },
      "application/dii": {
        source: "iana"
      },
      "application/dit": {
        source: "iana"
      },
      "application/dns": {
        source: "iana"
      },
      "application/dns+json": {
        source: "iana",
        compressible: true
      },
      "application/dns-message": {
        source: "iana"
      },
      "application/docbook+xml": {
        source: "apache",
        compressible: true,
        extensions: ["dbk"]
      },
      "application/dots+cbor": {
        source: "iana"
      },
      "application/dskpp+xml": {
        source: "iana",
        compressible: true
      },
      "application/dssc+der": {
        source: "iana",
        extensions: ["dssc"]
      },
      "application/dssc+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xdssc"]
      },
      "application/dvcs": {
        source: "iana"
      },
      "application/ecmascript": {
        source: "iana",
        compressible: true,
        extensions: ["es", "ecma"]
      },
      "application/edi-consent": {
        source: "iana"
      },
      "application/edi-x12": {
        source: "iana",
        compressible: false
      },
      "application/edifact": {
        source: "iana",
        compressible: false
      },
      "application/efi": {
        source: "iana"
      },
      "application/elm+json": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/elm+xml": {
        source: "iana",
        compressible: true
      },
      "application/emergencycalldata.cap+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/emergencycalldata.comment+xml": {
        source: "iana",
        compressible: true
      },
      "application/emergencycalldata.control+xml": {
        source: "iana",
        compressible: true
      },
      "application/emergencycalldata.deviceinfo+xml": {
        source: "iana",
        compressible: true
      },
      "application/emergencycalldata.ecall.msd": {
        source: "iana"
      },
      "application/emergencycalldata.providerinfo+xml": {
        source: "iana",
        compressible: true
      },
      "application/emergencycalldata.serviceinfo+xml": {
        source: "iana",
        compressible: true
      },
      "application/emergencycalldata.subscriberinfo+xml": {
        source: "iana",
        compressible: true
      },
      "application/emergencycalldata.veds+xml": {
        source: "iana",
        compressible: true
      },
      "application/emma+xml": {
        source: "iana",
        compressible: true,
        extensions: ["emma"]
      },
      "application/emotionml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["emotionml"]
      },
      "application/encaprtp": {
        source: "iana"
      },
      "application/epp+xml": {
        source: "iana",
        compressible: true
      },
      "application/epub+zip": {
        source: "iana",
        compressible: false,
        extensions: ["epub"]
      },
      "application/eshop": {
        source: "iana"
      },
      "application/exi": {
        source: "iana",
        extensions: ["exi"]
      },
      "application/expect-ct-report+json": {
        source: "iana",
        compressible: true
      },
      "application/express": {
        source: "iana",
        extensions: ["exp"]
      },
      "application/fastinfoset": {
        source: "iana"
      },
      "application/fastsoap": {
        source: "iana"
      },
      "application/fdt+xml": {
        source: "iana",
        compressible: true,
        extensions: ["fdt"]
      },
      "application/fhir+json": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/fhir+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/fido.trusted-apps+json": {
        compressible: true
      },
      "application/fits": {
        source: "iana"
      },
      "application/flexfec": {
        source: "iana"
      },
      "application/font-sfnt": {
        source: "iana"
      },
      "application/font-tdpfr": {
        source: "iana",
        extensions: ["pfr"]
      },
      "application/font-woff": {
        source: "iana",
        compressible: false
      },
      "application/framework-attributes+xml": {
        source: "iana",
        compressible: true
      },
      "application/geo+json": {
        source: "iana",
        compressible: true,
        extensions: ["geojson"]
      },
      "application/geo+json-seq": {
        source: "iana"
      },
      "application/geopackage+sqlite3": {
        source: "iana"
      },
      "application/geoxacml+xml": {
        source: "iana",
        compressible: true
      },
      "application/gltf-buffer": {
        source: "iana"
      },
      "application/gml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["gml"]
      },
      "application/gpx+xml": {
        source: "apache",
        compressible: true,
        extensions: ["gpx"]
      },
      "application/gxf": {
        source: "apache",
        extensions: ["gxf"]
      },
      "application/gzip": {
        source: "iana",
        compressible: false,
        extensions: ["gz"]
      },
      "application/h224": {
        source: "iana"
      },
      "application/held+xml": {
        source: "iana",
        compressible: true
      },
      "application/hjson": {
        extensions: ["hjson"]
      },
      "application/http": {
        source: "iana"
      },
      "application/hyperstudio": {
        source: "iana",
        extensions: ["stk"]
      },
      "application/ibe-key-request+xml": {
        source: "iana",
        compressible: true
      },
      "application/ibe-pkg-reply+xml": {
        source: "iana",
        compressible: true
      },
      "application/ibe-pp-data": {
        source: "iana"
      },
      "application/iges": {
        source: "iana"
      },
      "application/im-iscomposing+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/index": {
        source: "iana"
      },
      "application/index.cmd": {
        source: "iana"
      },
      "application/index.obj": {
        source: "iana"
      },
      "application/index.response": {
        source: "iana"
      },
      "application/index.vnd": {
        source: "iana"
      },
      "application/inkml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["ink", "inkml"]
      },
      "application/iotp": {
        source: "iana"
      },
      "application/ipfix": {
        source: "iana",
        extensions: ["ipfix"]
      },
      "application/ipp": {
        source: "iana"
      },
      "application/isup": {
        source: "iana"
      },
      "application/its+xml": {
        source: "iana",
        compressible: true,
        extensions: ["its"]
      },
      "application/java-archive": {
        source: "apache",
        compressible: false,
        extensions: ["jar", "war", "ear"]
      },
      "application/java-serialized-object": {
        source: "apache",
        compressible: false,
        extensions: ["ser"]
      },
      "application/java-vm": {
        source: "apache",
        compressible: false,
        extensions: ["class"]
      },
      "application/javascript": {
        source: "iana",
        charset: "UTF-8",
        compressible: true,
        extensions: ["js", "mjs"]
      },
      "application/jf2feed+json": {
        source: "iana",
        compressible: true
      },
      "application/jose": {
        source: "iana"
      },
      "application/jose+json": {
        source: "iana",
        compressible: true
      },
      "application/jrd+json": {
        source: "iana",
        compressible: true
      },
      "application/jscalendar+json": {
        source: "iana",
        compressible: true
      },
      "application/json": {
        source: "iana",
        charset: "UTF-8",
        compressible: true,
        extensions: ["json", "map"]
      },
      "application/json-patch+json": {
        source: "iana",
        compressible: true
      },
      "application/json-seq": {
        source: "iana"
      },
      "application/json5": {
        extensions: ["json5"]
      },
      "application/jsonml+json": {
        source: "apache",
        compressible: true,
        extensions: ["jsonml"]
      },
      "application/jwk+json": {
        source: "iana",
        compressible: true
      },
      "application/jwk-set+json": {
        source: "iana",
        compressible: true
      },
      "application/jwt": {
        source: "iana"
      },
      "application/kpml-request+xml": {
        source: "iana",
        compressible: true
      },
      "application/kpml-response+xml": {
        source: "iana",
        compressible: true
      },
      "application/ld+json": {
        source: "iana",
        compressible: true,
        extensions: ["jsonld"]
      },
      "application/lgr+xml": {
        source: "iana",
        compressible: true,
        extensions: ["lgr"]
      },
      "application/link-format": {
        source: "iana"
      },
      "application/load-control+xml": {
        source: "iana",
        compressible: true
      },
      "application/lost+xml": {
        source: "iana",
        compressible: true,
        extensions: ["lostxml"]
      },
      "application/lostsync+xml": {
        source: "iana",
        compressible: true
      },
      "application/lpf+zip": {
        source: "iana",
        compressible: false
      },
      "application/lxf": {
        source: "iana"
      },
      "application/mac-binhex40": {
        source: "iana",
        extensions: ["hqx"]
      },
      "application/mac-compactpro": {
        source: "apache",
        extensions: ["cpt"]
      },
      "application/macwriteii": {
        source: "iana"
      },
      "application/mads+xml": {
        source: "iana",
        compressible: true,
        extensions: ["mads"]
      },
      "application/manifest+json": {
        source: "iana",
        charset: "UTF-8",
        compressible: true,
        extensions: ["webmanifest"]
      },
      "application/marc": {
        source: "iana",
        extensions: ["mrc"]
      },
      "application/marcxml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["mrcx"]
      },
      "application/mathematica": {
        source: "iana",
        extensions: ["ma", "nb", "mb"]
      },
      "application/mathml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["mathml"]
      },
      "application/mathml-content+xml": {
        source: "iana",
        compressible: true
      },
      "application/mathml-presentation+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbms-associated-procedure-description+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbms-deregister+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbms-envelope+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbms-msk+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbms-msk-response+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbms-protection-description+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbms-reception-report+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbms-register+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbms-register-response+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbms-schedule+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbms-user-service-description+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbox": {
        source: "iana",
        extensions: ["mbox"]
      },
      "application/media-policy-dataset+xml": {
        source: "iana",
        compressible: true,
        extensions: ["mpf"]
      },
      "application/media_control+xml": {
        source: "iana",
        compressible: true
      },
      "application/mediaservercontrol+xml": {
        source: "iana",
        compressible: true,
        extensions: ["mscml"]
      },
      "application/merge-patch+json": {
        source: "iana",
        compressible: true
      },
      "application/metalink+xml": {
        source: "apache",
        compressible: true,
        extensions: ["metalink"]
      },
      "application/metalink4+xml": {
        source: "iana",
        compressible: true,
        extensions: ["meta4"]
      },
      "application/mets+xml": {
        source: "iana",
        compressible: true,
        extensions: ["mets"]
      },
      "application/mf4": {
        source: "iana"
      },
      "application/mikey": {
        source: "iana"
      },
      "application/mipc": {
        source: "iana"
      },
      "application/missing-blocks+cbor-seq": {
        source: "iana"
      },
      "application/mmt-aei+xml": {
        source: "iana",
        compressible: true,
        extensions: ["maei"]
      },
      "application/mmt-usd+xml": {
        source: "iana",
        compressible: true,
        extensions: ["musd"]
      },
      "application/mods+xml": {
        source: "iana",
        compressible: true,
        extensions: ["mods"]
      },
      "application/moss-keys": {
        source: "iana"
      },
      "application/moss-signature": {
        source: "iana"
      },
      "application/mosskey-data": {
        source: "iana"
      },
      "application/mosskey-request": {
        source: "iana"
      },
      "application/mp21": {
        source: "iana",
        extensions: ["m21", "mp21"]
      },
      "application/mp4": {
        source: "iana",
        extensions: ["mp4s", "m4p"]
      },
      "application/mpeg4-generic": {
        source: "iana"
      },
      "application/mpeg4-iod": {
        source: "iana"
      },
      "application/mpeg4-iod-xmt": {
        source: "iana"
      },
      "application/mrb-consumer+xml": {
        source: "iana",
        compressible: true
      },
      "application/mrb-publish+xml": {
        source: "iana",
        compressible: true
      },
      "application/msc-ivr+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/msc-mixer+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/msword": {
        source: "iana",
        compressible: false,
        extensions: ["doc", "dot"]
      },
      "application/mud+json": {
        source: "iana",
        compressible: true
      },
      "application/multipart-core": {
        source: "iana"
      },
      "application/mxf": {
        source: "iana",
        extensions: ["mxf"]
      },
      "application/n-quads": {
        source: "iana",
        extensions: ["nq"]
      },
      "application/n-triples": {
        source: "iana",
        extensions: ["nt"]
      },
      "application/nasdata": {
        source: "iana"
      },
      "application/news-checkgroups": {
        source: "iana",
        charset: "US-ASCII"
      },
      "application/news-groupinfo": {
        source: "iana",
        charset: "US-ASCII"
      },
      "application/news-transmission": {
        source: "iana"
      },
      "application/nlsml+xml": {
        source: "iana",
        compressible: true
      },
      "application/node": {
        source: "iana",
        extensions: ["cjs"]
      },
      "application/nss": {
        source: "iana"
      },
      "application/oauth-authz-req+jwt": {
        source: "iana"
      },
      "application/oblivious-dns-message": {
        source: "iana"
      },
      "application/ocsp-request": {
        source: "iana"
      },
      "application/ocsp-response": {
        source: "iana"
      },
      "application/octet-stream": {
        source: "iana",
        compressible: false,
        extensions: ["bin", "dms", "lrf", "mar", "so", "dist", "distz", "pkg", "bpk", "dump", "elc", "deploy", "exe", "dll", "deb", "dmg", "iso", "img", "msi", "msp", "msm", "buffer"]
      },
      "application/oda": {
        source: "iana",
        extensions: ["oda"]
      },
      "application/odm+xml": {
        source: "iana",
        compressible: true
      },
      "application/odx": {
        source: "iana"
      },
      "application/oebps-package+xml": {
        source: "iana",
        compressible: true,
        extensions: ["opf"]
      },
      "application/ogg": {
        source: "iana",
        compressible: false,
        extensions: ["ogx"]
      },
      "application/omdoc+xml": {
        source: "apache",
        compressible: true,
        extensions: ["omdoc"]
      },
      "application/onenote": {
        source: "apache",
        extensions: ["onetoc", "onetoc2", "onetmp", "onepkg"]
      },
      "application/opc-nodeset+xml": {
        source: "iana",
        compressible: true
      },
      "application/oscore": {
        source: "iana"
      },
      "application/oxps": {
        source: "iana",
        extensions: ["oxps"]
      },
      "application/p21": {
        source: "iana"
      },
      "application/p21+zip": {
        source: "iana",
        compressible: false
      },
      "application/p2p-overlay+xml": {
        source: "iana",
        compressible: true,
        extensions: ["relo"]
      },
      "application/parityfec": {
        source: "iana"
      },
      "application/passport": {
        source: "iana"
      },
      "application/patch-ops-error+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xer"]
      },
      "application/pdf": {
        source: "iana",
        compressible: false,
        extensions: ["pdf"]
      },
      "application/pdx": {
        source: "iana"
      },
      "application/pem-certificate-chain": {
        source: "iana"
      },
      "application/pgp-encrypted": {
        source: "iana",
        compressible: false,
        extensions: ["pgp"]
      },
      "application/pgp-keys": {
        source: "iana",
        extensions: ["asc"]
      },
      "application/pgp-signature": {
        source: "iana",
        extensions: ["asc", "sig"]
      },
      "application/pics-rules": {
        source: "apache",
        extensions: ["prf"]
      },
      "application/pidf+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/pidf-diff+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/pkcs10": {
        source: "iana",
        extensions: ["p10"]
      },
      "application/pkcs12": {
        source: "iana"
      },
      "application/pkcs7-mime": {
        source: "iana",
        extensions: ["p7m", "p7c"]
      },
      "application/pkcs7-signature": {
        source: "iana",
        extensions: ["p7s"]
      },
      "application/pkcs8": {
        source: "iana",
        extensions: ["p8"]
      },
      "application/pkcs8-encrypted": {
        source: "iana"
      },
      "application/pkix-attr-cert": {
        source: "iana",
        extensions: ["ac"]
      },
      "application/pkix-cert": {
        source: "iana",
        extensions: ["cer"]
      },
      "application/pkix-crl": {
        source: "iana",
        extensions: ["crl"]
      },
      "application/pkix-pkipath": {
        source: "iana",
        extensions: ["pkipath"]
      },
      "application/pkixcmp": {
        source: "iana",
        extensions: ["pki"]
      },
      "application/pls+xml": {
        source: "iana",
        compressible: true,
        extensions: ["pls"]
      },
      "application/poc-settings+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/postscript": {
        source: "iana",
        compressible: true,
        extensions: ["ai", "eps", "ps"]
      },
      "application/ppsp-tracker+json": {
        source: "iana",
        compressible: true
      },
      "application/problem+json": {
        source: "iana",
        compressible: true
      },
      "application/problem+xml": {
        source: "iana",
        compressible: true
      },
      "application/provenance+xml": {
        source: "iana",
        compressible: true,
        extensions: ["provx"]
      },
      "application/prs.alvestrand.titrax-sheet": {
        source: "iana"
      },
      "application/prs.cww": {
        source: "iana",
        extensions: ["cww"]
      },
      "application/prs.cyn": {
        source: "iana",
        charset: "7-BIT"
      },
      "application/prs.hpub+zip": {
        source: "iana",
        compressible: false
      },
      "application/prs.nprend": {
        source: "iana"
      },
      "application/prs.plucker": {
        source: "iana"
      },
      "application/prs.rdf-xml-crypt": {
        source: "iana"
      },
      "application/prs.xsf+xml": {
        source: "iana",
        compressible: true
      },
      "application/pskc+xml": {
        source: "iana",
        compressible: true,
        extensions: ["pskcxml"]
      },
      "application/pvd+json": {
        source: "iana",
        compressible: true
      },
      "application/qsig": {
        source: "iana"
      },
      "application/raml+yaml": {
        compressible: true,
        extensions: ["raml"]
      },
      "application/raptorfec": {
        source: "iana"
      },
      "application/rdap+json": {
        source: "iana",
        compressible: true
      },
      "application/rdf+xml": {
        source: "iana",
        compressible: true,
        extensions: ["rdf", "owl"]
      },
      "application/reginfo+xml": {
        source: "iana",
        compressible: true,
        extensions: ["rif"]
      },
      "application/relax-ng-compact-syntax": {
        source: "iana",
        extensions: ["rnc"]
      },
      "application/remote-printing": {
        source: "iana"
      },
      "application/reputon+json": {
        source: "iana",
        compressible: true
      },
      "application/resource-lists+xml": {
        source: "iana",
        compressible: true,
        extensions: ["rl"]
      },
      "application/resource-lists-diff+xml": {
        source: "iana",
        compressible: true,
        extensions: ["rld"]
      },
      "application/rfc+xml": {
        source: "iana",
        compressible: true
      },
      "application/riscos": {
        source: "iana"
      },
      "application/rlmi+xml": {
        source: "iana",
        compressible: true
      },
      "application/rls-services+xml": {
        source: "iana",
        compressible: true,
        extensions: ["rs"]
      },
      "application/route-apd+xml": {
        source: "iana",
        compressible: true,
        extensions: ["rapd"]
      },
      "application/route-s-tsid+xml": {
        source: "iana",
        compressible: true,
        extensions: ["sls"]
      },
      "application/route-usd+xml": {
        source: "iana",
        compressible: true,
        extensions: ["rusd"]
      },
      "application/rpki-ghostbusters": {
        source: "iana",
        extensions: ["gbr"]
      },
      "application/rpki-manifest": {
        source: "iana",
        extensions: ["mft"]
      },
      "application/rpki-publication": {
        source: "iana"
      },
      "application/rpki-roa": {
        source: "iana",
        extensions: ["roa"]
      },
      "application/rpki-updown": {
        source: "iana"
      },
      "application/rsd+xml": {
        source: "apache",
        compressible: true,
        extensions: ["rsd"]
      },
      "application/rss+xml": {
        source: "apache",
        compressible: true,
        extensions: ["rss"]
      },
      "application/rtf": {
        source: "iana",
        compressible: true,
        extensions: ["rtf"]
      },
      "application/rtploopback": {
        source: "iana"
      },
      "application/rtx": {
        source: "iana"
      },
      "application/samlassertion+xml": {
        source: "iana",
        compressible: true
      },
      "application/samlmetadata+xml": {
        source: "iana",
        compressible: true
      },
      "application/sarif+json": {
        source: "iana",
        compressible: true
      },
      "application/sarif-external-properties+json": {
        source: "iana",
        compressible: true
      },
      "application/sbe": {
        source: "iana"
      },
      "application/sbml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["sbml"]
      },
      "application/scaip+xml": {
        source: "iana",
        compressible: true
      },
      "application/scim+json": {
        source: "iana",
        compressible: true
      },
      "application/scvp-cv-request": {
        source: "iana",
        extensions: ["scq"]
      },
      "application/scvp-cv-response": {
        source: "iana",
        extensions: ["scs"]
      },
      "application/scvp-vp-request": {
        source: "iana",
        extensions: ["spq"]
      },
      "application/scvp-vp-response": {
        source: "iana",
        extensions: ["spp"]
      },
      "application/sdp": {
        source: "iana",
        extensions: ["sdp"]
      },
      "application/secevent+jwt": {
        source: "iana"
      },
      "application/senml+cbor": {
        source: "iana"
      },
      "application/senml+json": {
        source: "iana",
        compressible: true
      },
      "application/senml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["senmlx"]
      },
      "application/senml-etch+cbor": {
        source: "iana"
      },
      "application/senml-etch+json": {
        source: "iana",
        compressible: true
      },
      "application/senml-exi": {
        source: "iana"
      },
      "application/sensml+cbor": {
        source: "iana"
      },
      "application/sensml+json": {
        source: "iana",
        compressible: true
      },
      "application/sensml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["sensmlx"]
      },
      "application/sensml-exi": {
        source: "iana"
      },
      "application/sep+xml": {
        source: "iana",
        compressible: true
      },
      "application/sep-exi": {
        source: "iana"
      },
      "application/session-info": {
        source: "iana"
      },
      "application/set-payment": {
        source: "iana"
      },
      "application/set-payment-initiation": {
        source: "iana",
        extensions: ["setpay"]
      },
      "application/set-registration": {
        source: "iana"
      },
      "application/set-registration-initiation": {
        source: "iana",
        extensions: ["setreg"]
      },
      "application/sgml": {
        source: "iana"
      },
      "application/sgml-open-catalog": {
        source: "iana"
      },
      "application/shf+xml": {
        source: "iana",
        compressible: true,
        extensions: ["shf"]
      },
      "application/sieve": {
        source: "iana",
        extensions: ["siv", "sieve"]
      },
      "application/simple-filter+xml": {
        source: "iana",
        compressible: true
      },
      "application/simple-message-summary": {
        source: "iana"
      },
      "application/simplesymbolcontainer": {
        source: "iana"
      },
      "application/sipc": {
        source: "iana"
      },
      "application/slate": {
        source: "iana"
      },
      "application/smil": {
        source: "iana"
      },
      "application/smil+xml": {
        source: "iana",
        compressible: true,
        extensions: ["smi", "smil"]
      },
      "application/smpte336m": {
        source: "iana"
      },
      "application/soap+fastinfoset": {
        source: "iana"
      },
      "application/soap+xml": {
        source: "iana",
        compressible: true
      },
      "application/sparql-query": {
        source: "iana",
        extensions: ["rq"]
      },
      "application/sparql-results+xml": {
        source: "iana",
        compressible: true,
        extensions: ["srx"]
      },
      "application/spdx+json": {
        source: "iana",
        compressible: true
      },
      "application/spirits-event+xml": {
        source: "iana",
        compressible: true
      },
      "application/sql": {
        source: "iana"
      },
      "application/srgs": {
        source: "iana",
        extensions: ["gram"]
      },
      "application/srgs+xml": {
        source: "iana",
        compressible: true,
        extensions: ["grxml"]
      },
      "application/sru+xml": {
        source: "iana",
        compressible: true,
        extensions: ["sru"]
      },
      "application/ssdl+xml": {
        source: "apache",
        compressible: true,
        extensions: ["ssdl"]
      },
      "application/ssml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["ssml"]
      },
      "application/stix+json": {
        source: "iana",
        compressible: true
      },
      "application/swid+xml": {
        source: "iana",
        compressible: true,
        extensions: ["swidtag"]
      },
      "application/tamp-apex-update": {
        source: "iana"
      },
      "application/tamp-apex-update-confirm": {
        source: "iana"
      },
      "application/tamp-community-update": {
        source: "iana"
      },
      "application/tamp-community-update-confirm": {
        source: "iana"
      },
      "application/tamp-error": {
        source: "iana"
      },
      "application/tamp-sequence-adjust": {
        source: "iana"
      },
      "application/tamp-sequence-adjust-confirm": {
        source: "iana"
      },
      "application/tamp-status-query": {
        source: "iana"
      },
      "application/tamp-status-response": {
        source: "iana"
      },
      "application/tamp-update": {
        source: "iana"
      },
      "application/tamp-update-confirm": {
        source: "iana"
      },
      "application/tar": {
        compressible: true
      },
      "application/taxii+json": {
        source: "iana",
        compressible: true
      },
      "application/td+json": {
        source: "iana",
        compressible: true
      },
      "application/tei+xml": {
        source: "iana",
        compressible: true,
        extensions: ["tei", "teicorpus"]
      },
      "application/tetra_isi": {
        source: "iana"
      },
      "application/thraud+xml": {
        source: "iana",
        compressible: true,
        extensions: ["tfi"]
      },
      "application/timestamp-query": {
        source: "iana"
      },
      "application/timestamp-reply": {
        source: "iana"
      },
      "application/timestamped-data": {
        source: "iana",
        extensions: ["tsd"]
      },
      "application/tlsrpt+gzip": {
        source: "iana"
      },
      "application/tlsrpt+json": {
        source: "iana",
        compressible: true
      },
      "application/tnauthlist": {
        source: "iana"
      },
      "application/token-introspection+jwt": {
        source: "iana"
      },
      "application/toml": {
        compressible: true,
        extensions: ["toml"]
      },
      "application/trickle-ice-sdpfrag": {
        source: "iana"
      },
      "application/trig": {
        source: "iana",
        extensions: ["trig"]
      },
      "application/ttml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["ttml"]
      },
      "application/tve-trigger": {
        source: "iana"
      },
      "application/tzif": {
        source: "iana"
      },
      "application/tzif-leap": {
        source: "iana"
      },
      "application/ubjson": {
        compressible: false,
        extensions: ["ubj"]
      },
      "application/ulpfec": {
        source: "iana"
      },
      "application/urc-grpsheet+xml": {
        source: "iana",
        compressible: true
      },
      "application/urc-ressheet+xml": {
        source: "iana",
        compressible: true,
        extensions: ["rsheet"]
      },
      "application/urc-targetdesc+xml": {
        source: "iana",
        compressible: true,
        extensions: ["td"]
      },
      "application/urc-uisocketdesc+xml": {
        source: "iana",
        compressible: true
      },
      "application/vcard+json": {
        source: "iana",
        compressible: true
      },
      "application/vcard+xml": {
        source: "iana",
        compressible: true
      },
      "application/vemmi": {
        source: "iana"
      },
      "application/vividence.scriptfile": {
        source: "apache"
      },
      "application/vnd.1000minds.decision-model+xml": {
        source: "iana",
        compressible: true,
        extensions: ["1km"]
      },
      "application/vnd.3gpp-prose+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp-prose-pc3ch+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp-v2x-local-service-information": {
        source: "iana"
      },
      "application/vnd.3gpp.5gnas": {
        source: "iana"
      },
      "application/vnd.3gpp.access-transfer-events+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.bsf+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.gmop+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.gtpc": {
        source: "iana"
      },
      "application/vnd.3gpp.interworking-data": {
        source: "iana"
      },
      "application/vnd.3gpp.lpp": {
        source: "iana"
      },
      "application/vnd.3gpp.mc-signalling-ear": {
        source: "iana"
      },
      "application/vnd.3gpp.mcdata-affiliation-command+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcdata-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcdata-payload": {
        source: "iana"
      },
      "application/vnd.3gpp.mcdata-service-config+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcdata-signalling": {
        source: "iana"
      },
      "application/vnd.3gpp.mcdata-ue-config+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcdata-user-profile+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcptt-affiliation-command+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcptt-floor-request+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcptt-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcptt-location-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcptt-mbms-usage-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcptt-service-config+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcptt-signed+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcptt-ue-config+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcptt-ue-init-config+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcptt-user-profile+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcvideo-affiliation-command+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcvideo-affiliation-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcvideo-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcvideo-location-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcvideo-mbms-usage-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcvideo-service-config+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcvideo-transmission-request+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcvideo-ue-config+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcvideo-user-profile+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mid-call+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.ngap": {
        source: "iana"
      },
      "application/vnd.3gpp.pfcp": {
        source: "iana"
      },
      "application/vnd.3gpp.pic-bw-large": {
        source: "iana",
        extensions: ["plb"]
      },
      "application/vnd.3gpp.pic-bw-small": {
        source: "iana",
        extensions: ["psb"]
      },
      "application/vnd.3gpp.pic-bw-var": {
        source: "iana",
        extensions: ["pvb"]
      },
      "application/vnd.3gpp.s1ap": {
        source: "iana"
      },
      "application/vnd.3gpp.sms": {
        source: "iana"
      },
      "application/vnd.3gpp.sms+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.srvcc-ext+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.srvcc-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.state-and-event-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.ussd+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp2.bcmcsinfo+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp2.sms": {
        source: "iana"
      },
      "application/vnd.3gpp2.tcap": {
        source: "iana",
        extensions: ["tcap"]
      },
      "application/vnd.3lightssoftware.imagescal": {
        source: "iana"
      },
      "application/vnd.3m.post-it-notes": {
        source: "iana",
        extensions: ["pwn"]
      },
      "application/vnd.accpac.simply.aso": {
        source: "iana",
        extensions: ["aso"]
      },
      "application/vnd.accpac.simply.imp": {
        source: "iana",
        extensions: ["imp"]
      },
      "application/vnd.acucobol": {
        source: "iana",
        extensions: ["acu"]
      },
      "application/vnd.acucorp": {
        source: "iana",
        extensions: ["atc", "acutc"]
      },
      "application/vnd.adobe.air-application-installer-package+zip": {
        source: "apache",
        compressible: false,
        extensions: ["air"]
      },
      "application/vnd.adobe.flash.movie": {
        source: "iana"
      },
      "application/vnd.adobe.formscentral.fcdt": {
        source: "iana",
        extensions: ["fcdt"]
      },
      "application/vnd.adobe.fxp": {
        source: "iana",
        extensions: ["fxp", "fxpl"]
      },
      "application/vnd.adobe.partial-upload": {
        source: "iana"
      },
      "application/vnd.adobe.xdp+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xdp"]
      },
      "application/vnd.adobe.xfdf": {
        source: "iana",
        extensions: ["xfdf"]
      },
      "application/vnd.aether.imp": {
        source: "iana"
      },
      "application/vnd.afpc.afplinedata": {
        source: "iana"
      },
      "application/vnd.afpc.afplinedata-pagedef": {
        source: "iana"
      },
      "application/vnd.afpc.cmoca-cmresource": {
        source: "iana"
      },
      "application/vnd.afpc.foca-charset": {
        source: "iana"
      },
      "application/vnd.afpc.foca-codedfont": {
        source: "iana"
      },
      "application/vnd.afpc.foca-codepage": {
        source: "iana"
      },
      "application/vnd.afpc.modca": {
        source: "iana"
      },
      "application/vnd.afpc.modca-cmtable": {
        source: "iana"
      },
      "application/vnd.afpc.modca-formdef": {
        source: "iana"
      },
      "application/vnd.afpc.modca-mediummap": {
        source: "iana"
      },
      "application/vnd.afpc.modca-objectcontainer": {
        source: "iana"
      },
      "application/vnd.afpc.modca-overlay": {
        source: "iana"
      },
      "application/vnd.afpc.modca-pagesegment": {
        source: "iana"
      },
      "application/vnd.age": {
        source: "iana",
        extensions: ["age"]
      },
      "application/vnd.ah-barcode": {
        source: "iana"
      },
      "application/vnd.ahead.space": {
        source: "iana",
        extensions: ["ahead"]
      },
      "application/vnd.airzip.filesecure.azf": {
        source: "iana",
        extensions: ["azf"]
      },
      "application/vnd.airzip.filesecure.azs": {
        source: "iana",
        extensions: ["azs"]
      },
      "application/vnd.amadeus+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.amazon.ebook": {
        source: "apache",
        extensions: ["azw"]
      },
      "application/vnd.amazon.mobi8-ebook": {
        source: "iana"
      },
      "application/vnd.americandynamics.acc": {
        source: "iana",
        extensions: ["acc"]
      },
      "application/vnd.amiga.ami": {
        source: "iana",
        extensions: ["ami"]
      },
      "application/vnd.amundsen.maze+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.android.ota": {
        source: "iana"
      },
      "application/vnd.android.package-archive": {
        source: "apache",
        compressible: false,
        extensions: ["apk"]
      },
      "application/vnd.anki": {
        source: "iana"
      },
      "application/vnd.anser-web-certificate-issue-initiation": {
        source: "iana",
        extensions: ["cii"]
      },
      "application/vnd.anser-web-funds-transfer-initiation": {
        source: "apache",
        extensions: ["fti"]
      },
      "application/vnd.antix.game-component": {
        source: "iana",
        extensions: ["atx"]
      },
      "application/vnd.apache.arrow.file": {
        source: "iana"
      },
      "application/vnd.apache.arrow.stream": {
        source: "iana"
      },
      "application/vnd.apache.thrift.binary": {
        source: "iana"
      },
      "application/vnd.apache.thrift.compact": {
        source: "iana"
      },
      "application/vnd.apache.thrift.json": {
        source: "iana"
      },
      "application/vnd.api+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.aplextor.warrp+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.apothekende.reservation+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.apple.installer+xml": {
        source: "iana",
        compressible: true,
        extensions: ["mpkg"]
      },
      "application/vnd.apple.keynote": {
        source: "iana",
        extensions: ["key"]
      },
      "application/vnd.apple.mpegurl": {
        source: "iana",
        extensions: ["m3u8"]
      },
      "application/vnd.apple.numbers": {
        source: "iana",
        extensions: ["numbers"]
      },
      "application/vnd.apple.pages": {
        source: "iana",
        extensions: ["pages"]
      },
      "application/vnd.apple.pkpass": {
        compressible: false,
        extensions: ["pkpass"]
      },
      "application/vnd.arastra.swi": {
        source: "iana"
      },
      "application/vnd.aristanetworks.swi": {
        source: "iana",
        extensions: ["swi"]
      },
      "application/vnd.artisan+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.artsquare": {
        source: "iana"
      },
      "application/vnd.astraea-software.iota": {
        source: "iana",
        extensions: ["iota"]
      },
      "application/vnd.audiograph": {
        source: "iana",
        extensions: ["aep"]
      },
      "application/vnd.autopackage": {
        source: "iana"
      },
      "application/vnd.avalon+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.avistar+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.balsamiq.bmml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["bmml"]
      },
      "application/vnd.balsamiq.bmpr": {
        source: "iana"
      },
      "application/vnd.banana-accounting": {
        source: "iana"
      },
      "application/vnd.bbf.usp.error": {
        source: "iana"
      },
      "application/vnd.bbf.usp.msg": {
        source: "iana"
      },
      "application/vnd.bbf.usp.msg+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.bekitzur-stech+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.bint.med-content": {
        source: "iana"
      },
      "application/vnd.biopax.rdf+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.blink-idb-value-wrapper": {
        source: "iana"
      },
      "application/vnd.blueice.multipass": {
        source: "iana",
        extensions: ["mpm"]
      },
      "application/vnd.bluetooth.ep.oob": {
        source: "iana"
      },
      "application/vnd.bluetooth.le.oob": {
        source: "iana"
      },
      "application/vnd.bmi": {
        source: "iana",
        extensions: ["bmi"]
      },
      "application/vnd.bpf": {
        source: "iana"
      },
      "application/vnd.bpf3": {
        source: "iana"
      },
      "application/vnd.businessobjects": {
        source: "iana",
        extensions: ["rep"]
      },
      "application/vnd.byu.uapi+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.cab-jscript": {
        source: "iana"
      },
      "application/vnd.canon-cpdl": {
        source: "iana"
      },
      "application/vnd.canon-lips": {
        source: "iana"
      },
      "application/vnd.capasystems-pg+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.cendio.thinlinc.clientconf": {
        source: "iana"
      },
      "application/vnd.century-systems.tcp_stream": {
        source: "iana"
      },
      "application/vnd.chemdraw+xml": {
        source: "iana",
        compressible: true,
        extensions: ["cdxml"]
      },
      "application/vnd.chess-pgn": {
        source: "iana"
      },
      "application/vnd.chipnuts.karaoke-mmd": {
        source: "iana",
        extensions: ["mmd"]
      },
      "application/vnd.ciedi": {
        source: "iana"
      },
      "application/vnd.cinderella": {
        source: "iana",
        extensions: ["cdy"]
      },
      "application/vnd.cirpack.isdn-ext": {
        source: "iana"
      },
      "application/vnd.citationstyles.style+xml": {
        source: "iana",
        compressible: true,
        extensions: ["csl"]
      },
      "application/vnd.claymore": {
        source: "iana",
        extensions: ["cla"]
      },
      "application/vnd.cloanto.rp9": {
        source: "iana",
        extensions: ["rp9"]
      },
      "application/vnd.clonk.c4group": {
        source: "iana",
        extensions: ["c4g", "c4d", "c4f", "c4p", "c4u"]
      },
      "application/vnd.cluetrust.cartomobile-config": {
        source: "iana",
        extensions: ["c11amc"]
      },
      "application/vnd.cluetrust.cartomobile-config-pkg": {
        source: "iana",
        extensions: ["c11amz"]
      },
      "application/vnd.coffeescript": {
        source: "iana"
      },
      "application/vnd.collabio.xodocuments.document": {
        source: "iana"
      },
      "application/vnd.collabio.xodocuments.document-template": {
        source: "iana"
      },
      "application/vnd.collabio.xodocuments.presentation": {
        source: "iana"
      },
      "application/vnd.collabio.xodocuments.presentation-template": {
        source: "iana"
      },
      "application/vnd.collabio.xodocuments.spreadsheet": {
        source: "iana"
      },
      "application/vnd.collabio.xodocuments.spreadsheet-template": {
        source: "iana"
      },
      "application/vnd.collection+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.collection.doc+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.collection.next+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.comicbook+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.comicbook-rar": {
        source: "iana"
      },
      "application/vnd.commerce-battelle": {
        source: "iana"
      },
      "application/vnd.commonspace": {
        source: "iana",
        extensions: ["csp"]
      },
      "application/vnd.contact.cmsg": {
        source: "iana",
        extensions: ["cdbcmsg"]
      },
      "application/vnd.coreos.ignition+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.cosmocaller": {
        source: "iana",
        extensions: ["cmc"]
      },
      "application/vnd.crick.clicker": {
        source: "iana",
        extensions: ["clkx"]
      },
      "application/vnd.crick.clicker.keyboard": {
        source: "iana",
        extensions: ["clkk"]
      },
      "application/vnd.crick.clicker.palette": {
        source: "iana",
        extensions: ["clkp"]
      },
      "application/vnd.crick.clicker.template": {
        source: "iana",
        extensions: ["clkt"]
      },
      "application/vnd.crick.clicker.wordbank": {
        source: "iana",
        extensions: ["clkw"]
      },
      "application/vnd.criticaltools.wbs+xml": {
        source: "iana",
        compressible: true,
        extensions: ["wbs"]
      },
      "application/vnd.cryptii.pipe+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.crypto-shade-file": {
        source: "iana"
      },
      "application/vnd.cryptomator.encrypted": {
        source: "iana"
      },
      "application/vnd.cryptomator.vault": {
        source: "iana"
      },
      "application/vnd.ctc-posml": {
        source: "iana",
        extensions: ["pml"]
      },
      "application/vnd.ctct.ws+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.cups-pdf": {
        source: "iana"
      },
      "application/vnd.cups-postscript": {
        source: "iana"
      },
      "application/vnd.cups-ppd": {
        source: "iana",
        extensions: ["ppd"]
      },
      "application/vnd.cups-raster": {
        source: "iana"
      },
      "application/vnd.cups-raw": {
        source: "iana"
      },
      "application/vnd.curl": {
        source: "iana"
      },
      "application/vnd.curl.car": {
        source: "apache",
        extensions: ["car"]
      },
      "application/vnd.curl.pcurl": {
        source: "apache",
        extensions: ["pcurl"]
      },
      "application/vnd.cyan.dean.root+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.cybank": {
        source: "iana"
      },
      "application/vnd.cyclonedx+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.cyclonedx+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.d2l.coursepackage1p0+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.d3m-dataset": {
        source: "iana"
      },
      "application/vnd.d3m-problem": {
        source: "iana"
      },
      "application/vnd.dart": {
        source: "iana",
        compressible: true,
        extensions: ["dart"]
      },
      "application/vnd.data-vision.rdz": {
        source: "iana",
        extensions: ["rdz"]
      },
      "application/vnd.datapackage+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dataresource+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dbf": {
        source: "iana",
        extensions: ["dbf"]
      },
      "application/vnd.debian.binary-package": {
        source: "iana"
      },
      "application/vnd.dece.data": {
        source: "iana",
        extensions: ["uvf", "uvvf", "uvd", "uvvd"]
      },
      "application/vnd.dece.ttml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["uvt", "uvvt"]
      },
      "application/vnd.dece.unspecified": {
        source: "iana",
        extensions: ["uvx", "uvvx"]
      },
      "application/vnd.dece.zip": {
        source: "iana",
        extensions: ["uvz", "uvvz"]
      },
      "application/vnd.denovo.fcselayout-link": {
        source: "iana",
        extensions: ["fe_launch"]
      },
      "application/vnd.desmume.movie": {
        source: "iana"
      },
      "application/vnd.dir-bi.plate-dl-nosuffix": {
        source: "iana"
      },
      "application/vnd.dm.delegation+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dna": {
        source: "iana",
        extensions: ["dna"]
      },
      "application/vnd.document+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dolby.mlp": {
        source: "apache",
        extensions: ["mlp"]
      },
      "application/vnd.dolby.mobile.1": {
        source: "iana"
      },
      "application/vnd.dolby.mobile.2": {
        source: "iana"
      },
      "application/vnd.doremir.scorecloud-binary-document": {
        source: "iana"
      },
      "application/vnd.dpgraph": {
        source: "iana",
        extensions: ["dpg"]
      },
      "application/vnd.dreamfactory": {
        source: "iana",
        extensions: ["dfac"]
      },
      "application/vnd.drive+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ds-keypoint": {
        source: "apache",
        extensions: ["kpxx"]
      },
      "application/vnd.dtg.local": {
        source: "iana"
      },
      "application/vnd.dtg.local.flash": {
        source: "iana"
      },
      "application/vnd.dtg.local.html": {
        source: "iana"
      },
      "application/vnd.dvb.ait": {
        source: "iana",
        extensions: ["ait"]
      },
      "application/vnd.dvb.dvbisl+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dvb.dvbj": {
        source: "iana"
      },
      "application/vnd.dvb.esgcontainer": {
        source: "iana"
      },
      "application/vnd.dvb.ipdcdftnotifaccess": {
        source: "iana"
      },
      "application/vnd.dvb.ipdcesgaccess": {
        source: "iana"
      },
      "application/vnd.dvb.ipdcesgaccess2": {
        source: "iana"
      },
      "application/vnd.dvb.ipdcesgpdd": {
        source: "iana"
      },
      "application/vnd.dvb.ipdcroaming": {
        source: "iana"
      },
      "application/vnd.dvb.iptv.alfec-base": {
        source: "iana"
      },
      "application/vnd.dvb.iptv.alfec-enhancement": {
        source: "iana"
      },
      "application/vnd.dvb.notif-aggregate-root+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dvb.notif-container+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dvb.notif-generic+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dvb.notif-ia-msglist+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dvb.notif-ia-registration-request+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dvb.notif-ia-registration-response+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dvb.notif-init+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dvb.pfr": {
        source: "iana"
      },
      "application/vnd.dvb.service": {
        source: "iana",
        extensions: ["svc"]
      },
      "application/vnd.dxr": {
        source: "iana"
      },
      "application/vnd.dynageo": {
        source: "iana",
        extensions: ["geo"]
      },
      "application/vnd.dzr": {
        source: "iana"
      },
      "application/vnd.easykaraoke.cdgdownload": {
        source: "iana"
      },
      "application/vnd.ecdis-update": {
        source: "iana"
      },
      "application/vnd.ecip.rlp": {
        source: "iana"
      },
      "application/vnd.eclipse.ditto+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ecowin.chart": {
        source: "iana",
        extensions: ["mag"]
      },
      "application/vnd.ecowin.filerequest": {
        source: "iana"
      },
      "application/vnd.ecowin.fileupdate": {
        source: "iana"
      },
      "application/vnd.ecowin.series": {
        source: "iana"
      },
      "application/vnd.ecowin.seriesrequest": {
        source: "iana"
      },
      "application/vnd.ecowin.seriesupdate": {
        source: "iana"
      },
      "application/vnd.efi.img": {
        source: "iana"
      },
      "application/vnd.efi.iso": {
        source: "iana"
      },
      "application/vnd.emclient.accessrequest+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.enliven": {
        source: "iana",
        extensions: ["nml"]
      },
      "application/vnd.enphase.envoy": {
        source: "iana"
      },
      "application/vnd.eprints.data+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.epson.esf": {
        source: "iana",
        extensions: ["esf"]
      },
      "application/vnd.epson.msf": {
        source: "iana",
        extensions: ["msf"]
      },
      "application/vnd.epson.quickanime": {
        source: "iana",
        extensions: ["qam"]
      },
      "application/vnd.epson.salt": {
        source: "iana",
        extensions: ["slt"]
      },
      "application/vnd.epson.ssf": {
        source: "iana",
        extensions: ["ssf"]
      },
      "application/vnd.ericsson.quickcall": {
        source: "iana"
      },
      "application/vnd.espass-espass+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.eszigno3+xml": {
        source: "iana",
        compressible: true,
        extensions: ["es3", "et3"]
      },
      "application/vnd.etsi.aoc+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.asic-e+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.etsi.asic-s+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.etsi.cug+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.iptvcommand+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.iptvdiscovery+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.iptvprofile+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.iptvsad-bc+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.iptvsad-cod+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.iptvsad-npvr+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.iptvservice+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.iptvsync+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.iptvueprofile+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.mcid+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.mheg5": {
        source: "iana"
      },
      "application/vnd.etsi.overload-control-policy-dataset+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.pstn+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.sci+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.simservs+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.timestamp-token": {
        source: "iana"
      },
      "application/vnd.etsi.tsl+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.tsl.der": {
        source: "iana"
      },
      "application/vnd.eu.kasparian.car+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.eudora.data": {
        source: "iana"
      },
      "application/vnd.evolv.ecig.profile": {
        source: "iana"
      },
      "application/vnd.evolv.ecig.settings": {
        source: "iana"
      },
      "application/vnd.evolv.ecig.theme": {
        source: "iana"
      },
      "application/vnd.exstream-empower+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.exstream-package": {
        source: "iana"
      },
      "application/vnd.ezpix-album": {
        source: "iana",
        extensions: ["ez2"]
      },
      "application/vnd.ezpix-package": {
        source: "iana",
        extensions: ["ez3"]
      },
      "application/vnd.f-secure.mobile": {
        source: "iana"
      },
      "application/vnd.familysearch.gedcom+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.fastcopy-disk-image": {
        source: "iana"
      },
      "application/vnd.fdf": {
        source: "iana",
        extensions: ["fdf"]
      },
      "application/vnd.fdsn.mseed": {
        source: "iana",
        extensions: ["mseed"]
      },
      "application/vnd.fdsn.seed": {
        source: "iana",
        extensions: ["seed", "dataless"]
      },
      "application/vnd.ffsns": {
        source: "iana"
      },
      "application/vnd.ficlab.flb+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.filmit.zfc": {
        source: "iana"
      },
      "application/vnd.fints": {
        source: "iana"
      },
      "application/vnd.firemonkeys.cloudcell": {
        source: "iana"
      },
      "application/vnd.flographit": {
        source: "iana",
        extensions: ["gph"]
      },
      "application/vnd.fluxtime.clip": {
        source: "iana",
        extensions: ["ftc"]
      },
      "application/vnd.font-fontforge-sfd": {
        source: "iana"
      },
      "application/vnd.framemaker": {
        source: "iana",
        extensions: ["fm", "frame", "maker", "book"]
      },
      "application/vnd.frogans.fnc": {
        source: "iana",
        extensions: ["fnc"]
      },
      "application/vnd.frogans.ltf": {
        source: "iana",
        extensions: ["ltf"]
      },
      "application/vnd.fsc.weblaunch": {
        source: "iana",
        extensions: ["fsc"]
      },
      "application/vnd.fujifilm.fb.docuworks": {
        source: "iana"
      },
      "application/vnd.fujifilm.fb.docuworks.binder": {
        source: "iana"
      },
      "application/vnd.fujifilm.fb.docuworks.container": {
        source: "iana"
      },
      "application/vnd.fujifilm.fb.jfi+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.fujitsu.oasys": {
        source: "iana",
        extensions: ["oas"]
      },
      "application/vnd.fujitsu.oasys2": {
        source: "iana",
        extensions: ["oa2"]
      },
      "application/vnd.fujitsu.oasys3": {
        source: "iana",
        extensions: ["oa3"]
      },
      "application/vnd.fujitsu.oasysgp": {
        source: "iana",
        extensions: ["fg5"]
      },
      "application/vnd.fujitsu.oasysprs": {
        source: "iana",
        extensions: ["bh2"]
      },
      "application/vnd.fujixerox.art-ex": {
        source: "iana"
      },
      "application/vnd.fujixerox.art4": {
        source: "iana"
      },
      "application/vnd.fujixerox.ddd": {
        source: "iana",
        extensions: ["ddd"]
      },
      "application/vnd.fujixerox.docuworks": {
        source: "iana",
        extensions: ["xdw"]
      },
      "application/vnd.fujixerox.docuworks.binder": {
        source: "iana",
        extensions: ["xbd"]
      },
      "application/vnd.fujixerox.docuworks.container": {
        source: "iana"
      },
      "application/vnd.fujixerox.hbpl": {
        source: "iana"
      },
      "application/vnd.fut-misnet": {
        source: "iana"
      },
      "application/vnd.futoin+cbor": {
        source: "iana"
      },
      "application/vnd.futoin+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.fuzzysheet": {
        source: "iana",
        extensions: ["fzs"]
      },
      "application/vnd.genomatix.tuxedo": {
        source: "iana",
        extensions: ["txd"]
      },
      "application/vnd.gentics.grd+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.geo+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.geocube+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.geogebra.file": {
        source: "iana",
        extensions: ["ggb"]
      },
      "application/vnd.geogebra.slides": {
        source: "iana"
      },
      "application/vnd.geogebra.tool": {
        source: "iana",
        extensions: ["ggt"]
      },
      "application/vnd.geometry-explorer": {
        source: "iana",
        extensions: ["gex", "gre"]
      },
      "application/vnd.geonext": {
        source: "iana",
        extensions: ["gxt"]
      },
      "application/vnd.geoplan": {
        source: "iana",
        extensions: ["g2w"]
      },
      "application/vnd.geospace": {
        source: "iana",
        extensions: ["g3w"]
      },
      "application/vnd.gerber": {
        source: "iana"
      },
      "application/vnd.globalplatform.card-content-mgt": {
        source: "iana"
      },
      "application/vnd.globalplatform.card-content-mgt-response": {
        source: "iana"
      },
      "application/vnd.gmx": {
        source: "iana",
        extensions: ["gmx"]
      },
      "application/vnd.google-apps.document": {
        compressible: false,
        extensions: ["gdoc"]
      },
      "application/vnd.google-apps.presentation": {
        compressible: false,
        extensions: ["gslides"]
      },
      "application/vnd.google-apps.spreadsheet": {
        compressible: false,
        extensions: ["gsheet"]
      },
      "application/vnd.google-earth.kml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["kml"]
      },
      "application/vnd.google-earth.kmz": {
        source: "iana",
        compressible: false,
        extensions: ["kmz"]
      },
      "application/vnd.gov.sk.e-form+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.gov.sk.e-form+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.gov.sk.xmldatacontainer+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.grafeq": {
        source: "iana",
        extensions: ["gqf", "gqs"]
      },
      "application/vnd.gridmp": {
        source: "iana"
      },
      "application/vnd.groove-account": {
        source: "iana",
        extensions: ["gac"]
      },
      "application/vnd.groove-help": {
        source: "iana",
        extensions: ["ghf"]
      },
      "application/vnd.groove-identity-message": {
        source: "iana",
        extensions: ["gim"]
      },
      "application/vnd.groove-injector": {
        source: "iana",
        extensions: ["grv"]
      },
      "application/vnd.groove-tool-message": {
        source: "iana",
        extensions: ["gtm"]
      },
      "application/vnd.groove-tool-template": {
        source: "iana",
        extensions: ["tpl"]
      },
      "application/vnd.groove-vcard": {
        source: "iana",
        extensions: ["vcg"]
      },
      "application/vnd.hal+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.hal+xml": {
        source: "iana",
        compressible: true,
        extensions: ["hal"]
      },
      "application/vnd.handheld-entertainment+xml": {
        source: "iana",
        compressible: true,
        extensions: ["zmm"]
      },
      "application/vnd.hbci": {
        source: "iana",
        extensions: ["hbci"]
      },
      "application/vnd.hc+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.hcl-bireports": {
        source: "iana"
      },
      "application/vnd.hdt": {
        source: "iana"
      },
      "application/vnd.heroku+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.hhe.lesson-player": {
        source: "iana",
        extensions: ["les"]
      },
      "application/vnd.hl7cda+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/vnd.hl7v2+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/vnd.hp-hpgl": {
        source: "iana",
        extensions: ["hpgl"]
      },
      "application/vnd.hp-hpid": {
        source: "iana",
        extensions: ["hpid"]
      },
      "application/vnd.hp-hps": {
        source: "iana",
        extensions: ["hps"]
      },
      "application/vnd.hp-jlyt": {
        source: "iana",
        extensions: ["jlt"]
      },
      "application/vnd.hp-pcl": {
        source: "iana",
        extensions: ["pcl"]
      },
      "application/vnd.hp-pclxl": {
        source: "iana",
        extensions: ["pclxl"]
      },
      "application/vnd.httphone": {
        source: "iana"
      },
      "application/vnd.hydrostatix.sof-data": {
        source: "iana",
        extensions: ["sfd-hdstx"]
      },
      "application/vnd.hyper+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.hyper-item+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.hyperdrive+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.hzn-3d-crossword": {
        source: "iana"
      },
      "application/vnd.ibm.afplinedata": {
        source: "iana"
      },
      "application/vnd.ibm.electronic-media": {
        source: "iana"
      },
      "application/vnd.ibm.minipay": {
        source: "iana",
        extensions: ["mpy"]
      },
      "application/vnd.ibm.modcap": {
        source: "iana",
        extensions: ["afp", "listafp", "list3820"]
      },
      "application/vnd.ibm.rights-management": {
        source: "iana",
        extensions: ["irm"]
      },
      "application/vnd.ibm.secure-container": {
        source: "iana",
        extensions: ["sc"]
      },
      "application/vnd.iccprofile": {
        source: "iana",
        extensions: ["icc", "icm"]
      },
      "application/vnd.ieee.1905": {
        source: "iana"
      },
      "application/vnd.igloader": {
        source: "iana",
        extensions: ["igl"]
      },
      "application/vnd.imagemeter.folder+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.imagemeter.image+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.immervision-ivp": {
        source: "iana",
        extensions: ["ivp"]
      },
      "application/vnd.immervision-ivu": {
        source: "iana",
        extensions: ["ivu"]
      },
      "application/vnd.ims.imsccv1p1": {
        source: "iana"
      },
      "application/vnd.ims.imsccv1p2": {
        source: "iana"
      },
      "application/vnd.ims.imsccv1p3": {
        source: "iana"
      },
      "application/vnd.ims.lis.v2.result+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ims.lti.v2.toolconsumerprofile+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ims.lti.v2.toolproxy+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ims.lti.v2.toolproxy.id+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ims.lti.v2.toolsettings+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ims.lti.v2.toolsettings.simple+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.informedcontrol.rms+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.informix-visionary": {
        source: "iana"
      },
      "application/vnd.infotech.project": {
        source: "iana"
      },
      "application/vnd.infotech.project+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.innopath.wamp.notification": {
        source: "iana"
      },
      "application/vnd.insors.igm": {
        source: "iana",
        extensions: ["igm"]
      },
      "application/vnd.intercon.formnet": {
        source: "iana",
        extensions: ["xpw", "xpx"]
      },
      "application/vnd.intergeo": {
        source: "iana",
        extensions: ["i2g"]
      },
      "application/vnd.intertrust.digibox": {
        source: "iana"
      },
      "application/vnd.intertrust.nncp": {
        source: "iana"
      },
      "application/vnd.intu.qbo": {
        source: "iana",
        extensions: ["qbo"]
      },
      "application/vnd.intu.qfx": {
        source: "iana",
        extensions: ["qfx"]
      },
      "application/vnd.iptc.g2.catalogitem+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.iptc.g2.conceptitem+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.iptc.g2.knowledgeitem+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.iptc.g2.newsitem+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.iptc.g2.newsmessage+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.iptc.g2.packageitem+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.iptc.g2.planningitem+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ipunplugged.rcprofile": {
        source: "iana",
        extensions: ["rcprofile"]
      },
      "application/vnd.irepository.package+xml": {
        source: "iana",
        compressible: true,
        extensions: ["irp"]
      },
      "application/vnd.is-xpr": {
        source: "iana",
        extensions: ["xpr"]
      },
      "application/vnd.isac.fcs": {
        source: "iana",
        extensions: ["fcs"]
      },
      "application/vnd.iso11783-10+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.jam": {
        source: "iana",
        extensions: ["jam"]
      },
      "application/vnd.japannet-directory-service": {
        source: "iana"
      },
      "application/vnd.japannet-jpnstore-wakeup": {
        source: "iana"
      },
      "application/vnd.japannet-payment-wakeup": {
        source: "iana"
      },
      "application/vnd.japannet-registration": {
        source: "iana"
      },
      "application/vnd.japannet-registration-wakeup": {
        source: "iana"
      },
      "application/vnd.japannet-setstore-wakeup": {
        source: "iana"
      },
      "application/vnd.japannet-verification": {
        source: "iana"
      },
      "application/vnd.japannet-verification-wakeup": {
        source: "iana"
      },
      "application/vnd.jcp.javame.midlet-rms": {
        source: "iana",
        extensions: ["rms"]
      },
      "application/vnd.jisp": {
        source: "iana",
        extensions: ["jisp"]
      },
      "application/vnd.joost.joda-archive": {
        source: "iana",
        extensions: ["joda"]
      },
      "application/vnd.jsk.isdn-ngn": {
        source: "iana"
      },
      "application/vnd.kahootz": {
        source: "iana",
        extensions: ["ktz", "ktr"]
      },
      "application/vnd.kde.karbon": {
        source: "iana",
        extensions: ["karbon"]
      },
      "application/vnd.kde.kchart": {
        source: "iana",
        extensions: ["chrt"]
      },
      "application/vnd.kde.kformula": {
        source: "iana",
        extensions: ["kfo"]
      },
      "application/vnd.kde.kivio": {
        source: "iana",
        extensions: ["flw"]
      },
      "application/vnd.kde.kontour": {
        source: "iana",
        extensions: ["kon"]
      },
      "application/vnd.kde.kpresenter": {
        source: "iana",
        extensions: ["kpr", "kpt"]
      },
      "application/vnd.kde.kspread": {
        source: "iana",
        extensions: ["ksp"]
      },
      "application/vnd.kde.kword": {
        source: "iana",
        extensions: ["kwd", "kwt"]
      },
      "application/vnd.kenameaapp": {
        source: "iana",
        extensions: ["htke"]
      },
      "application/vnd.kidspiration": {
        source: "iana",
        extensions: ["kia"]
      },
      "application/vnd.kinar": {
        source: "iana",
        extensions: ["kne", "knp"]
      },
      "application/vnd.koan": {
        source: "iana",
        extensions: ["skp", "skd", "skt", "skm"]
      },
      "application/vnd.kodak-descriptor": {
        source: "iana",
        extensions: ["sse"]
      },
      "application/vnd.las": {
        source: "iana"
      },
      "application/vnd.las.las+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.las.las+xml": {
        source: "iana",
        compressible: true,
        extensions: ["lasxml"]
      },
      "application/vnd.laszip": {
        source: "iana"
      },
      "application/vnd.leap+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.liberty-request+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.llamagraphics.life-balance.desktop": {
        source: "iana",
        extensions: ["lbd"]
      },
      "application/vnd.llamagraphics.life-balance.exchange+xml": {
        source: "iana",
        compressible: true,
        extensions: ["lbe"]
      },
      "application/vnd.logipipe.circuit+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.loom": {
        source: "iana"
      },
      "application/vnd.lotus-1-2-3": {
        source: "iana",
        extensions: ["123"]
      },
      "application/vnd.lotus-approach": {
        source: "iana",
        extensions: ["apr"]
      },
      "application/vnd.lotus-freelance": {
        source: "iana",
        extensions: ["pre"]
      },
      "application/vnd.lotus-notes": {
        source: "iana",
        extensions: ["nsf"]
      },
      "application/vnd.lotus-organizer": {
        source: "iana",
        extensions: ["org"]
      },
      "application/vnd.lotus-screencam": {
        source: "iana",
        extensions: ["scm"]
      },
      "application/vnd.lotus-wordpro": {
        source: "iana",
        extensions: ["lwp"]
      },
      "application/vnd.macports.portpkg": {
        source: "iana",
        extensions: ["portpkg"]
      },
      "application/vnd.mapbox-vector-tile": {
        source: "iana",
        extensions: ["mvt"]
      },
      "application/vnd.marlin.drm.actiontoken+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.marlin.drm.conftoken+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.marlin.drm.license+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.marlin.drm.mdcf": {
        source: "iana"
      },
      "application/vnd.mason+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.maxar.archive.3tz+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.maxmind.maxmind-db": {
        source: "iana"
      },
      "application/vnd.mcd": {
        source: "iana",
        extensions: ["mcd"]
      },
      "application/vnd.medcalcdata": {
        source: "iana",
        extensions: ["mc1"]
      },
      "application/vnd.mediastation.cdkey": {
        source: "iana",
        extensions: ["cdkey"]
      },
      "application/vnd.meridian-slingshot": {
        source: "iana"
      },
      "application/vnd.mfer": {
        source: "iana",
        extensions: ["mwf"]
      },
      "application/vnd.mfmp": {
        source: "iana",
        extensions: ["mfm"]
      },
      "application/vnd.micro+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.micrografx.flo": {
        source: "iana",
        extensions: ["flo"]
      },
      "application/vnd.micrografx.igx": {
        source: "iana",
        extensions: ["igx"]
      },
      "application/vnd.microsoft.portable-executable": {
        source: "iana"
      },
      "application/vnd.microsoft.windows.thumbnail-cache": {
        source: "iana"
      },
      "application/vnd.miele+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.mif": {
        source: "iana",
        extensions: ["mif"]
      },
      "application/vnd.minisoft-hp3000-save": {
        source: "iana"
      },
      "application/vnd.mitsubishi.misty-guard.trustweb": {
        source: "iana"
      },
      "application/vnd.mobius.daf": {
        source: "iana",
        extensions: ["daf"]
      },
      "application/vnd.mobius.dis": {
        source: "iana",
        extensions: ["dis"]
      },
      "application/vnd.mobius.mbk": {
        source: "iana",
        extensions: ["mbk"]
      },
      "application/vnd.mobius.mqy": {
        source: "iana",
        extensions: ["mqy"]
      },
      "application/vnd.mobius.msl": {
        source: "iana",
        extensions: ["msl"]
      },
      "application/vnd.mobius.plc": {
        source: "iana",
        extensions: ["plc"]
      },
      "application/vnd.mobius.txf": {
        source: "iana",
        extensions: ["txf"]
      },
      "application/vnd.mophun.application": {
        source: "iana",
        extensions: ["mpn"]
      },
      "application/vnd.mophun.certificate": {
        source: "iana",
        extensions: ["mpc"]
      },
      "application/vnd.motorola.flexsuite": {
        source: "iana"
      },
      "application/vnd.motorola.flexsuite.adsi": {
        source: "iana"
      },
      "application/vnd.motorola.flexsuite.fis": {
        source: "iana"
      },
      "application/vnd.motorola.flexsuite.gotap": {
        source: "iana"
      },
      "application/vnd.motorola.flexsuite.kmr": {
        source: "iana"
      },
      "application/vnd.motorola.flexsuite.ttc": {
        source: "iana"
      },
      "application/vnd.motorola.flexsuite.wem": {
        source: "iana"
      },
      "application/vnd.motorola.iprm": {
        source: "iana"
      },
      "application/vnd.mozilla.xul+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xul"]
      },
      "application/vnd.ms-3mfdocument": {
        source: "iana"
      },
      "application/vnd.ms-artgalry": {
        source: "iana",
        extensions: ["cil"]
      },
      "application/vnd.ms-asf": {
        source: "iana"
      },
      "application/vnd.ms-cab-compressed": {
        source: "iana",
        extensions: ["cab"]
      },
      "application/vnd.ms-color.iccprofile": {
        source: "apache"
      },
      "application/vnd.ms-excel": {
        source: "iana",
        compressible: false,
        extensions: ["xls", "xlm", "xla", "xlc", "xlt", "xlw"]
      },
      "application/vnd.ms-excel.addin.macroenabled.12": {
        source: "iana",
        extensions: ["xlam"]
      },
      "application/vnd.ms-excel.sheet.binary.macroenabled.12": {
        source: "iana",
        extensions: ["xlsb"]
      },
      "application/vnd.ms-excel.sheet.macroenabled.12": {
        source: "iana",
        extensions: ["xlsm"]
      },
      "application/vnd.ms-excel.template.macroenabled.12": {
        source: "iana",
        extensions: ["xltm"]
      },
      "application/vnd.ms-fontobject": {
        source: "iana",
        compressible: true,
        extensions: ["eot"]
      },
      "application/vnd.ms-htmlhelp": {
        source: "iana",
        extensions: ["chm"]
      },
      "application/vnd.ms-ims": {
        source: "iana",
        extensions: ["ims"]
      },
      "application/vnd.ms-lrm": {
        source: "iana",
        extensions: ["lrm"]
      },
      "application/vnd.ms-office.activex+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ms-officetheme": {
        source: "iana",
        extensions: ["thmx"]
      },
      "application/vnd.ms-opentype": {
        source: "apache",
        compressible: true
      },
      "application/vnd.ms-outlook": {
        compressible: false,
        extensions: ["msg"]
      },
      "application/vnd.ms-package.obfuscated-opentype": {
        source: "apache"
      },
      "application/vnd.ms-pki.seccat": {
        source: "apache",
        extensions: ["cat"]
      },
      "application/vnd.ms-pki.stl": {
        source: "apache",
        extensions: ["stl"]
      },
      "application/vnd.ms-playready.initiator+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ms-powerpoint": {
        source: "iana",
        compressible: false,
        extensions: ["ppt", "pps", "pot"]
      },
      "application/vnd.ms-powerpoint.addin.macroenabled.12": {
        source: "iana",
        extensions: ["ppam"]
      },
      "application/vnd.ms-powerpoint.presentation.macroenabled.12": {
        source: "iana",
        extensions: ["pptm"]
      },
      "application/vnd.ms-powerpoint.slide.macroenabled.12": {
        source: "iana",
        extensions: ["sldm"]
      },
      "application/vnd.ms-powerpoint.slideshow.macroenabled.12": {
        source: "iana",
        extensions: ["ppsm"]
      },
      "application/vnd.ms-powerpoint.template.macroenabled.12": {
        source: "iana",
        extensions: ["potm"]
      },
      "application/vnd.ms-printdevicecapabilities+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ms-printing.printticket+xml": {
        source: "apache",
        compressible: true
      },
      "application/vnd.ms-printschematicket+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ms-project": {
        source: "iana",
        extensions: ["mpp", "mpt"]
      },
      "application/vnd.ms-tnef": {
        source: "iana"
      },
      "application/vnd.ms-windows.devicepairing": {
        source: "iana"
      },
      "application/vnd.ms-windows.nwprinting.oob": {
        source: "iana"
      },
      "application/vnd.ms-windows.printerpairing": {
        source: "iana"
      },
      "application/vnd.ms-windows.wsd.oob": {
        source: "iana"
      },
      "application/vnd.ms-wmdrm.lic-chlg-req": {
        source: "iana"
      },
      "application/vnd.ms-wmdrm.lic-resp": {
        source: "iana"
      },
      "application/vnd.ms-wmdrm.meter-chlg-req": {
        source: "iana"
      },
      "application/vnd.ms-wmdrm.meter-resp": {
        source: "iana"
      },
      "application/vnd.ms-word.document.macroenabled.12": {
        source: "iana",
        extensions: ["docm"]
      },
      "application/vnd.ms-word.template.macroenabled.12": {
        source: "iana",
        extensions: ["dotm"]
      },
      "application/vnd.ms-works": {
        source: "iana",
        extensions: ["wps", "wks", "wcm", "wdb"]
      },
      "application/vnd.ms-wpl": {
        source: "iana",
        extensions: ["wpl"]
      },
      "application/vnd.ms-xpsdocument": {
        source: "iana",
        compressible: false,
        extensions: ["xps"]
      },
      "application/vnd.msa-disk-image": {
        source: "iana"
      },
      "application/vnd.mseq": {
        source: "iana",
        extensions: ["mseq"]
      },
      "application/vnd.msign": {
        source: "iana"
      },
      "application/vnd.multiad.creator": {
        source: "iana"
      },
      "application/vnd.multiad.creator.cif": {
        source: "iana"
      },
      "application/vnd.music-niff": {
        source: "iana"
      },
      "application/vnd.musician": {
        source: "iana",
        extensions: ["mus"]
      },
      "application/vnd.muvee.style": {
        source: "iana",
        extensions: ["msty"]
      },
      "application/vnd.mynfc": {
        source: "iana",
        extensions: ["taglet"]
      },
      "application/vnd.nacamar.ybrid+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ncd.control": {
        source: "iana"
      },
      "application/vnd.ncd.reference": {
        source: "iana"
      },
      "application/vnd.nearst.inv+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.nebumind.line": {
        source: "iana"
      },
      "application/vnd.nervana": {
        source: "iana"
      },
      "application/vnd.netfpx": {
        source: "iana"
      },
      "application/vnd.neurolanguage.nlu": {
        source: "iana",
        extensions: ["nlu"]
      },
      "application/vnd.nimn": {
        source: "iana"
      },
      "application/vnd.nintendo.nitro.rom": {
        source: "iana"
      },
      "application/vnd.nintendo.snes.rom": {
        source: "iana"
      },
      "application/vnd.nitf": {
        source: "iana",
        extensions: ["ntf", "nitf"]
      },
      "application/vnd.noblenet-directory": {
        source: "iana",
        extensions: ["nnd"]
      },
      "application/vnd.noblenet-sealer": {
        source: "iana",
        extensions: ["nns"]
      },
      "application/vnd.noblenet-web": {
        source: "iana",
        extensions: ["nnw"]
      },
      "application/vnd.nokia.catalogs": {
        source: "iana"
      },
      "application/vnd.nokia.conml+wbxml": {
        source: "iana"
      },
      "application/vnd.nokia.conml+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.nokia.iptv.config+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.nokia.isds-radio-presets": {
        source: "iana"
      },
      "application/vnd.nokia.landmark+wbxml": {
        source: "iana"
      },
      "application/vnd.nokia.landmark+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.nokia.landmarkcollection+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.nokia.n-gage.ac+xml": {
        source: "iana",
        compressible: true,
        extensions: ["ac"]
      },
      "application/vnd.nokia.n-gage.data": {
        source: "iana",
        extensions: ["ngdat"]
      },
      "application/vnd.nokia.n-gage.symbian.install": {
        source: "iana",
        extensions: ["n-gage"]
      },
      "application/vnd.nokia.ncd": {
        source: "iana"
      },
      "application/vnd.nokia.pcd+wbxml": {
        source: "iana"
      },
      "application/vnd.nokia.pcd+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.nokia.radio-preset": {
        source: "iana",
        extensions: ["rpst"]
      },
      "application/vnd.nokia.radio-presets": {
        source: "iana",
        extensions: ["rpss"]
      },
      "application/vnd.novadigm.edm": {
        source: "iana",
        extensions: ["edm"]
      },
      "application/vnd.novadigm.edx": {
        source: "iana",
        extensions: ["edx"]
      },
      "application/vnd.novadigm.ext": {
        source: "iana",
        extensions: ["ext"]
      },
      "application/vnd.ntt-local.content-share": {
        source: "iana"
      },
      "application/vnd.ntt-local.file-transfer": {
        source: "iana"
      },
      "application/vnd.ntt-local.ogw_remote-access": {
        source: "iana"
      },
      "application/vnd.ntt-local.sip-ta_remote": {
        source: "iana"
      },
      "application/vnd.ntt-local.sip-ta_tcp_stream": {
        source: "iana"
      },
      "application/vnd.oasis.opendocument.chart": {
        source: "iana",
        extensions: ["odc"]
      },
      "application/vnd.oasis.opendocument.chart-template": {
        source: "iana",
        extensions: ["otc"]
      },
      "application/vnd.oasis.opendocument.database": {
        source: "iana",
        extensions: ["odb"]
      },
      "application/vnd.oasis.opendocument.formula": {
        source: "iana",
        extensions: ["odf"]
      },
      "application/vnd.oasis.opendocument.formula-template": {
        source: "iana",
        extensions: ["odft"]
      },
      "application/vnd.oasis.opendocument.graphics": {
        source: "iana",
        compressible: false,
        extensions: ["odg"]
      },
      "application/vnd.oasis.opendocument.graphics-template": {
        source: "iana",
        extensions: ["otg"]
      },
      "application/vnd.oasis.opendocument.image": {
        source: "iana",
        extensions: ["odi"]
      },
      "application/vnd.oasis.opendocument.image-template": {
        source: "iana",
        extensions: ["oti"]
      },
      "application/vnd.oasis.opendocument.presentation": {
        source: "iana",
        compressible: false,
        extensions: ["odp"]
      },
      "application/vnd.oasis.opendocument.presentation-template": {
        source: "iana",
        extensions: ["otp"]
      },
      "application/vnd.oasis.opendocument.spreadsheet": {
        source: "iana",
        compressible: false,
        extensions: ["ods"]
      },
      "application/vnd.oasis.opendocument.spreadsheet-template": {
        source: "iana",
        extensions: ["ots"]
      },
      "application/vnd.oasis.opendocument.text": {
        source: "iana",
        compressible: false,
        extensions: ["odt"]
      },
      "application/vnd.oasis.opendocument.text-master": {
        source: "iana",
        extensions: ["odm"]
      },
      "application/vnd.oasis.opendocument.text-template": {
        source: "iana",
        extensions: ["ott"]
      },
      "application/vnd.oasis.opendocument.text-web": {
        source: "iana",
        extensions: ["oth"]
      },
      "application/vnd.obn": {
        source: "iana"
      },
      "application/vnd.ocf+cbor": {
        source: "iana"
      },
      "application/vnd.oci.image.manifest.v1+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oftn.l10n+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oipf.contentaccessdownload+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oipf.contentaccessstreaming+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oipf.cspg-hexbinary": {
        source: "iana"
      },
      "application/vnd.oipf.dae.svg+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oipf.dae.xhtml+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oipf.mippvcontrolmessage+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oipf.pae.gem": {
        source: "iana"
      },
      "application/vnd.oipf.spdiscovery+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oipf.spdlist+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oipf.ueprofile+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oipf.userprofile+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.olpc-sugar": {
        source: "iana",
        extensions: ["xo"]
      },
      "application/vnd.oma-scws-config": {
        source: "iana"
      },
      "application/vnd.oma-scws-http-request": {
        source: "iana"
      },
      "application/vnd.oma-scws-http-response": {
        source: "iana"
      },
      "application/vnd.oma.bcast.associated-procedure-parameter+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.bcast.drm-trigger+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.bcast.imd+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.bcast.ltkm": {
        source: "iana"
      },
      "application/vnd.oma.bcast.notification+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.bcast.provisioningtrigger": {
        source: "iana"
      },
      "application/vnd.oma.bcast.sgboot": {
        source: "iana"
      },
      "application/vnd.oma.bcast.sgdd+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.bcast.sgdu": {
        source: "iana"
      },
      "application/vnd.oma.bcast.simple-symbol-container": {
        source: "iana"
      },
      "application/vnd.oma.bcast.smartcard-trigger+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.bcast.sprov+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.bcast.stkm": {
        source: "iana"
      },
      "application/vnd.oma.cab-address-book+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.cab-feature-handler+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.cab-pcc+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.cab-subs-invite+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.cab-user-prefs+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.dcd": {
        source: "iana"
      },
      "application/vnd.oma.dcdc": {
        source: "iana"
      },
      "application/vnd.oma.dd2+xml": {
        source: "iana",
        compressible: true,
        extensions: ["dd2"]
      },
      "application/vnd.oma.drm.risd+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.group-usage-list+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.lwm2m+cbor": {
        source: "iana"
      },
      "application/vnd.oma.lwm2m+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.lwm2m+tlv": {
        source: "iana"
      },
      "application/vnd.oma.pal+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.poc.detailed-progress-report+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.poc.final-report+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.poc.groups+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.poc.invocation-descriptor+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.poc.optimized-progress-report+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.push": {
        source: "iana"
      },
      "application/vnd.oma.scidm.messages+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.xcap-directory+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.omads-email+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/vnd.omads-file+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/vnd.omads-folder+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/vnd.omaloc-supl-init": {
        source: "iana"
      },
      "application/vnd.onepager": {
        source: "iana"
      },
      "application/vnd.onepagertamp": {
        source: "iana"
      },
      "application/vnd.onepagertamx": {
        source: "iana"
      },
      "application/vnd.onepagertat": {
        source: "iana"
      },
      "application/vnd.onepagertatp": {
        source: "iana"
      },
      "application/vnd.onepagertatx": {
        source: "iana"
      },
      "application/vnd.openblox.game+xml": {
        source: "iana",
        compressible: true,
        extensions: ["obgx"]
      },
      "application/vnd.openblox.game-binary": {
        source: "iana"
      },
      "application/vnd.openeye.oeb": {
        source: "iana"
      },
      "application/vnd.openofficeorg.extension": {
        source: "apache",
        extensions: ["oxt"]
      },
      "application/vnd.openstreetmap.data+xml": {
        source: "iana",
        compressible: true,
        extensions: ["osm"]
      },
      "application/vnd.opentimestamps.ots": {
        source: "iana"
      },
      "application/vnd.openxmlformats-officedocument.custom-properties+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.customxmlproperties+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.drawing+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.drawingml.chart+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.drawingml.chartshapes+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.drawingml.diagramcolors+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.drawingml.diagramdata+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.drawingml.diagramlayout+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.drawingml.diagramstyle+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.extended-properties+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.commentauthors+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.comments+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.handoutmaster+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.notesmaster+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.notesslide+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.presentation": {
        source: "iana",
        compressible: false,
        extensions: ["pptx"]
      },
      "application/vnd.openxmlformats-officedocument.presentationml.presentation.main+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.presprops+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.slide": {
        source: "iana",
        extensions: ["sldx"]
      },
      "application/vnd.openxmlformats-officedocument.presentationml.slide+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.slidelayout+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.slidemaster+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.slideshow": {
        source: "iana",
        extensions: ["ppsx"]
      },
      "application/vnd.openxmlformats-officedocument.presentationml.slideshow.main+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.slideupdateinfo+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.tablestyles+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.tags+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.template": {
        source: "iana",
        extensions: ["potx"]
      },
      "application/vnd.openxmlformats-officedocument.presentationml.template.main+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.viewprops+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.calcchain+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.chartsheet+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.comments+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.connections+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.dialogsheet+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.externallink+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.pivotcachedefinition+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.pivotcacherecords+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.pivottable+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.querytable+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.revisionheaders+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.revisionlog+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sharedstrings+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {
        source: "iana",
        compressible: false,
        extensions: ["xlsx"]
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheetmetadata+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.table+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.tablesinglecells+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.template": {
        source: "iana",
        extensions: ["xltx"]
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.template.main+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.usernames+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.volatiledependencies+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.theme+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.themeoverride+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.vmldrawing": {
        source: "iana"
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.comments+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document": {
        source: "iana",
        compressible: false,
        extensions: ["docx"]
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document.glossary+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.endnotes+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.fonttable+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.footer+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.footnotes+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.numbering+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.settings+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.template": {
        source: "iana",
        extensions: ["dotx"]
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.template.main+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.websettings+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-package.core-properties+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-package.digital-signature-xmlsignature+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-package.relationships+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oracle.resource+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.orange.indata": {
        source: "iana"
      },
      "application/vnd.osa.netdeploy": {
        source: "iana"
      },
      "application/vnd.osgeo.mapguide.package": {
        source: "iana",
        extensions: ["mgp"]
      },
      "application/vnd.osgi.bundle": {
        source: "iana"
      },
      "application/vnd.osgi.dp": {
        source: "iana",
        extensions: ["dp"]
      },
      "application/vnd.osgi.subsystem": {
        source: "iana",
        extensions: ["esa"]
      },
      "application/vnd.otps.ct-kip+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oxli.countgraph": {
        source: "iana"
      },
      "application/vnd.pagerduty+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.palm": {
        source: "iana",
        extensions: ["pdb", "pqa", "oprc"]
      },
      "application/vnd.panoply": {
        source: "iana"
      },
      "application/vnd.paos.xml": {
        source: "iana"
      },
      "application/vnd.patentdive": {
        source: "iana"
      },
      "application/vnd.patientecommsdoc": {
        source: "iana"
      },
      "application/vnd.pawaafile": {
        source: "iana",
        extensions: ["paw"]
      },
      "application/vnd.pcos": {
        source: "iana"
      },
      "application/vnd.pg.format": {
        source: "iana",
        extensions: ["str"]
      },
      "application/vnd.pg.osasli": {
        source: "iana",
        extensions: ["ei6"]
      },
      "application/vnd.piaccess.application-licence": {
        source: "iana"
      },
      "application/vnd.picsel": {
        source: "iana",
        extensions: ["efif"]
      },
      "application/vnd.pmi.widget": {
        source: "iana",
        extensions: ["wg"]
      },
      "application/vnd.poc.group-advertisement+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.pocketlearn": {
        source: "iana",
        extensions: ["plf"]
      },
      "application/vnd.powerbuilder6": {
        source: "iana",
        extensions: ["pbd"]
      },
      "application/vnd.powerbuilder6-s": {
        source: "iana"
      },
      "application/vnd.powerbuilder7": {
        source: "iana"
      },
      "application/vnd.powerbuilder7-s": {
        source: "iana"
      },
      "application/vnd.powerbuilder75": {
        source: "iana"
      },
      "application/vnd.powerbuilder75-s": {
        source: "iana"
      },
      "application/vnd.preminet": {
        source: "iana"
      },
      "application/vnd.previewsystems.box": {
        source: "iana",
        extensions: ["box"]
      },
      "application/vnd.proteus.magazine": {
        source: "iana",
        extensions: ["mgz"]
      },
      "application/vnd.psfs": {
        source: "iana"
      },
      "application/vnd.publishare-delta-tree": {
        source: "iana",
        extensions: ["qps"]
      },
      "application/vnd.pvi.ptid1": {
        source: "iana",
        extensions: ["ptid"]
      },
      "application/vnd.pwg-multiplexed": {
        source: "iana"
      },
      "application/vnd.pwg-xhtml-print+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.qualcomm.brew-app-res": {
        source: "iana"
      },
      "application/vnd.quarantainenet": {
        source: "iana"
      },
      "application/vnd.quark.quarkxpress": {
        source: "iana",
        extensions: ["qxd", "qxt", "qwd", "qwt", "qxl", "qxb"]
      },
      "application/vnd.quobject-quoxdocument": {
        source: "iana"
      },
      "application/vnd.radisys.moml+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-audit+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-audit-conf+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-audit-conn+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-audit-dialog+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-audit-stream+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-conf+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-dialog+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-dialog-base+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-dialog-fax-detect+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-dialog-fax-sendrecv+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-dialog-group+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-dialog-speech+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-dialog-transform+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.rainstor.data": {
        source: "iana"
      },
      "application/vnd.rapid": {
        source: "iana"
      },
      "application/vnd.rar": {
        source: "iana",
        extensions: ["rar"]
      },
      "application/vnd.realvnc.bed": {
        source: "iana",
        extensions: ["bed"]
      },
      "application/vnd.recordare.musicxml": {
        source: "iana",
        extensions: ["mxl"]
      },
      "application/vnd.recordare.musicxml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["musicxml"]
      },
      "application/vnd.renlearn.rlprint": {
        source: "iana"
      },
      "application/vnd.resilient.logic": {
        source: "iana"
      },
      "application/vnd.restful+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.rig.cryptonote": {
        source: "iana",
        extensions: ["cryptonote"]
      },
      "application/vnd.rim.cod": {
        source: "apache",
        extensions: ["cod"]
      },
      "application/vnd.rn-realmedia": {
        source: "apache",
        extensions: ["rm"]
      },
      "application/vnd.rn-realmedia-vbr": {
        source: "apache",
        extensions: ["rmvb"]
      },
      "application/vnd.route66.link66+xml": {
        source: "iana",
        compressible: true,
        extensions: ["link66"]
      },
      "application/vnd.rs-274x": {
        source: "iana"
      },
      "application/vnd.ruckus.download": {
        source: "iana"
      },
      "application/vnd.s3sms": {
        source: "iana"
      },
      "application/vnd.sailingtracker.track": {
        source: "iana",
        extensions: ["st"]
      },
      "application/vnd.sar": {
        source: "iana"
      },
      "application/vnd.sbm.cid": {
        source: "iana"
      },
      "application/vnd.sbm.mid2": {
        source: "iana"
      },
      "application/vnd.scribus": {
        source: "iana"
      },
      "application/vnd.sealed.3df": {
        source: "iana"
      },
      "application/vnd.sealed.csf": {
        source: "iana"
      },
      "application/vnd.sealed.doc": {
        source: "iana"
      },
      "application/vnd.sealed.eml": {
        source: "iana"
      },
      "application/vnd.sealed.mht": {
        source: "iana"
      },
      "application/vnd.sealed.net": {
        source: "iana"
      },
      "application/vnd.sealed.ppt": {
        source: "iana"
      },
      "application/vnd.sealed.tiff": {
        source: "iana"
      },
      "application/vnd.sealed.xls": {
        source: "iana"
      },
      "application/vnd.sealedmedia.softseal.html": {
        source: "iana"
      },
      "application/vnd.sealedmedia.softseal.pdf": {
        source: "iana"
      },
      "application/vnd.seemail": {
        source: "iana",
        extensions: ["see"]
      },
      "application/vnd.seis+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.sema": {
        source: "iana",
        extensions: ["sema"]
      },
      "application/vnd.semd": {
        source: "iana",
        extensions: ["semd"]
      },
      "application/vnd.semf": {
        source: "iana",
        extensions: ["semf"]
      },
      "application/vnd.shade-save-file": {
        source: "iana"
      },
      "application/vnd.shana.informed.formdata": {
        source: "iana",
        extensions: ["ifm"]
      },
      "application/vnd.shana.informed.formtemplate": {
        source: "iana",
        extensions: ["itp"]
      },
      "application/vnd.shana.informed.interchange": {
        source: "iana",
        extensions: ["iif"]
      },
      "application/vnd.shana.informed.package": {
        source: "iana",
        extensions: ["ipk"]
      },
      "application/vnd.shootproof+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.shopkick+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.shp": {
        source: "iana"
      },
      "application/vnd.shx": {
        source: "iana"
      },
      "application/vnd.sigrok.session": {
        source: "iana"
      },
      "application/vnd.simtech-mindmapper": {
        source: "iana",
        extensions: ["twd", "twds"]
      },
      "application/vnd.siren+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.smaf": {
        source: "iana",
        extensions: ["mmf"]
      },
      "application/vnd.smart.notebook": {
        source: "iana"
      },
      "application/vnd.smart.teacher": {
        source: "iana",
        extensions: ["teacher"]
      },
      "application/vnd.snesdev-page-table": {
        source: "iana"
      },
      "application/vnd.software602.filler.form+xml": {
        source: "iana",
        compressible: true,
        extensions: ["fo"]
      },
      "application/vnd.software602.filler.form-xml-zip": {
        source: "iana"
      },
      "application/vnd.solent.sdkm+xml": {
        source: "iana",
        compressible: true,
        extensions: ["sdkm", "sdkd"]
      },
      "application/vnd.spotfire.dxp": {
        source: "iana",
        extensions: ["dxp"]
      },
      "application/vnd.spotfire.sfs": {
        source: "iana",
        extensions: ["sfs"]
      },
      "application/vnd.sqlite3": {
        source: "iana"
      },
      "application/vnd.sss-cod": {
        source: "iana"
      },
      "application/vnd.sss-dtf": {
        source: "iana"
      },
      "application/vnd.sss-ntf": {
        source: "iana"
      },
      "application/vnd.stardivision.calc": {
        source: "apache",
        extensions: ["sdc"]
      },
      "application/vnd.stardivision.draw": {
        source: "apache",
        extensions: ["sda"]
      },
      "application/vnd.stardivision.impress": {
        source: "apache",
        extensions: ["sdd"]
      },
      "application/vnd.stardivision.math": {
        source: "apache",
        extensions: ["smf"]
      },
      "application/vnd.stardivision.writer": {
        source: "apache",
        extensions: ["sdw", "vor"]
      },
      "application/vnd.stardivision.writer-global": {
        source: "apache",
        extensions: ["sgl"]
      },
      "application/vnd.stepmania.package": {
        source: "iana",
        extensions: ["smzip"]
      },
      "application/vnd.stepmania.stepchart": {
        source: "iana",
        extensions: ["sm"]
      },
      "application/vnd.street-stream": {
        source: "iana"
      },
      "application/vnd.sun.wadl+xml": {
        source: "iana",
        compressible: true,
        extensions: ["wadl"]
      },
      "application/vnd.sun.xml.calc": {
        source: "apache",
        extensions: ["sxc"]
      },
      "application/vnd.sun.xml.calc.template": {
        source: "apache",
        extensions: ["stc"]
      },
      "application/vnd.sun.xml.draw": {
        source: "apache",
        extensions: ["sxd"]
      },
      "application/vnd.sun.xml.draw.template": {
        source: "apache",
        extensions: ["std"]
      },
      "application/vnd.sun.xml.impress": {
        source: "apache",
        extensions: ["sxi"]
      },
      "application/vnd.sun.xml.impress.template": {
        source: "apache",
        extensions: ["sti"]
      },
      "application/vnd.sun.xml.math": {
        source: "apache",
        extensions: ["sxm"]
      },
      "application/vnd.sun.xml.writer": {
        source: "apache",
        extensions: ["sxw"]
      },
      "application/vnd.sun.xml.writer.global": {
        source: "apache",
        extensions: ["sxg"]
      },
      "application/vnd.sun.xml.writer.template": {
        source: "apache",
        extensions: ["stw"]
      },
      "application/vnd.sus-calendar": {
        source: "iana",
        extensions: ["sus", "susp"]
      },
      "application/vnd.svd": {
        source: "iana",
        extensions: ["svd"]
      },
      "application/vnd.swiftview-ics": {
        source: "iana"
      },
      "application/vnd.sycle+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.syft+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.symbian.install": {
        source: "apache",
        extensions: ["sis", "sisx"]
      },
      "application/vnd.syncml+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true,
        extensions: ["xsm"]
      },
      "application/vnd.syncml.dm+wbxml": {
        source: "iana",
        charset: "UTF-8",
        extensions: ["bdm"]
      },
      "application/vnd.syncml.dm+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true,
        extensions: ["xdm"]
      },
      "application/vnd.syncml.dm.notification": {
        source: "iana"
      },
      "application/vnd.syncml.dmddf+wbxml": {
        source: "iana"
      },
      "application/vnd.syncml.dmddf+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true,
        extensions: ["ddf"]
      },
      "application/vnd.syncml.dmtnds+wbxml": {
        source: "iana"
      },
      "application/vnd.syncml.dmtnds+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/vnd.syncml.ds.notification": {
        source: "iana"
      },
      "application/vnd.tableschema+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.tao.intent-module-archive": {
        source: "iana",
        extensions: ["tao"]
      },
      "application/vnd.tcpdump.pcap": {
        source: "iana",
        extensions: ["pcap", "cap", "dmp"]
      },
      "application/vnd.think-cell.ppttc+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.tmd.mediaflex.api+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.tml": {
        source: "iana"
      },
      "application/vnd.tmobile-livetv": {
        source: "iana",
        extensions: ["tmo"]
      },
      "application/vnd.tri.onesource": {
        source: "iana"
      },
      "application/vnd.trid.tpt": {
        source: "iana",
        extensions: ["tpt"]
      },
      "application/vnd.triscape.mxs": {
        source: "iana",
        extensions: ["mxs"]
      },
      "application/vnd.trueapp": {
        source: "iana",
        extensions: ["tra"]
      },
      "application/vnd.truedoc": {
        source: "iana"
      },
      "application/vnd.ubisoft.webplayer": {
        source: "iana"
      },
      "application/vnd.ufdl": {
        source: "iana",
        extensions: ["ufd", "ufdl"]
      },
      "application/vnd.uiq.theme": {
        source: "iana",
        extensions: ["utz"]
      },
      "application/vnd.umajin": {
        source: "iana",
        extensions: ["umj"]
      },
      "application/vnd.unity": {
        source: "iana",
        extensions: ["unityweb"]
      },
      "application/vnd.uoml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["uoml"]
      },
      "application/vnd.uplanet.alert": {
        source: "iana"
      },
      "application/vnd.uplanet.alert-wbxml": {
        source: "iana"
      },
      "application/vnd.uplanet.bearer-choice": {
        source: "iana"
      },
      "application/vnd.uplanet.bearer-choice-wbxml": {
        source: "iana"
      },
      "application/vnd.uplanet.cacheop": {
        source: "iana"
      },
      "application/vnd.uplanet.cacheop-wbxml": {
        source: "iana"
      },
      "application/vnd.uplanet.channel": {
        source: "iana"
      },
      "application/vnd.uplanet.channel-wbxml": {
        source: "iana"
      },
      "application/vnd.uplanet.list": {
        source: "iana"
      },
      "application/vnd.uplanet.list-wbxml": {
        source: "iana"
      },
      "application/vnd.uplanet.listcmd": {
        source: "iana"
      },
      "application/vnd.uplanet.listcmd-wbxml": {
        source: "iana"
      },
      "application/vnd.uplanet.signal": {
        source: "iana"
      },
      "application/vnd.uri-map": {
        source: "iana"
      },
      "application/vnd.valve.source.material": {
        source: "iana"
      },
      "application/vnd.vcx": {
        source: "iana",
        extensions: ["vcx"]
      },
      "application/vnd.vd-study": {
        source: "iana"
      },
      "application/vnd.vectorworks": {
        source: "iana"
      },
      "application/vnd.vel+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.verimatrix.vcas": {
        source: "iana"
      },
      "application/vnd.veritone.aion+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.veryant.thin": {
        source: "iana"
      },
      "application/vnd.ves.encrypted": {
        source: "iana"
      },
      "application/vnd.vidsoft.vidconference": {
        source: "iana"
      },
      "application/vnd.visio": {
        source: "iana",
        extensions: ["vsd", "vst", "vss", "vsw"]
      },
      "application/vnd.visionary": {
        source: "iana",
        extensions: ["vis"]
      },
      "application/vnd.vividence.scriptfile": {
        source: "iana"
      },
      "application/vnd.vsf": {
        source: "iana",
        extensions: ["vsf"]
      },
      "application/vnd.wap.sic": {
        source: "iana"
      },
      "application/vnd.wap.slc": {
        source: "iana"
      },
      "application/vnd.wap.wbxml": {
        source: "iana",
        charset: "UTF-8",
        extensions: ["wbxml"]
      },
      "application/vnd.wap.wmlc": {
        source: "iana",
        extensions: ["wmlc"]
      },
      "application/vnd.wap.wmlscriptc": {
        source: "iana",
        extensions: ["wmlsc"]
      },
      "application/vnd.webturbo": {
        source: "iana",
        extensions: ["wtb"]
      },
      "application/vnd.wfa.dpp": {
        source: "iana"
      },
      "application/vnd.wfa.p2p": {
        source: "iana"
      },
      "application/vnd.wfa.wsc": {
        source: "iana"
      },
      "application/vnd.windows.devicepairing": {
        source: "iana"
      },
      "application/vnd.wmc": {
        source: "iana"
      },
      "application/vnd.wmf.bootstrap": {
        source: "iana"
      },
      "application/vnd.wolfram.mathematica": {
        source: "iana"
      },
      "application/vnd.wolfram.mathematica.package": {
        source: "iana"
      },
      "application/vnd.wolfram.player": {
        source: "iana",
        extensions: ["nbp"]
      },
      "application/vnd.wordperfect": {
        source: "iana",
        extensions: ["wpd"]
      },
      "application/vnd.wqd": {
        source: "iana",
        extensions: ["wqd"]
      },
      "application/vnd.wrq-hp3000-labelled": {
        source: "iana"
      },
      "application/vnd.wt.stf": {
        source: "iana",
        extensions: ["stf"]
      },
      "application/vnd.wv.csp+wbxml": {
        source: "iana"
      },
      "application/vnd.wv.csp+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.wv.ssp+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.xacml+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.xara": {
        source: "iana",
        extensions: ["xar"]
      },
      "application/vnd.xfdl": {
        source: "iana",
        extensions: ["xfdl"]
      },
      "application/vnd.xfdl.webform": {
        source: "iana"
      },
      "application/vnd.xmi+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.xmpie.cpkg": {
        source: "iana"
      },
      "application/vnd.xmpie.dpkg": {
        source: "iana"
      },
      "application/vnd.xmpie.plan": {
        source: "iana"
      },
      "application/vnd.xmpie.ppkg": {
        source: "iana"
      },
      "application/vnd.xmpie.xlim": {
        source: "iana"
      },
      "application/vnd.yamaha.hv-dic": {
        source: "iana",
        extensions: ["hvd"]
      },
      "application/vnd.yamaha.hv-script": {
        source: "iana",
        extensions: ["hvs"]
      },
      "application/vnd.yamaha.hv-voice": {
        source: "iana",
        extensions: ["hvp"]
      },
      "application/vnd.yamaha.openscoreformat": {
        source: "iana",
        extensions: ["osf"]
      },
      "application/vnd.yamaha.openscoreformat.osfpvg+xml": {
        source: "iana",
        compressible: true,
        extensions: ["osfpvg"]
      },
      "application/vnd.yamaha.remote-setup": {
        source: "iana"
      },
      "application/vnd.yamaha.smaf-audio": {
        source: "iana",
        extensions: ["saf"]
      },
      "application/vnd.yamaha.smaf-phrase": {
        source: "iana",
        extensions: ["spf"]
      },
      "application/vnd.yamaha.through-ngn": {
        source: "iana"
      },
      "application/vnd.yamaha.tunnel-udpencap": {
        source: "iana"
      },
      "application/vnd.yaoweme": {
        source: "iana"
      },
      "application/vnd.yellowriver-custom-menu": {
        source: "iana",
        extensions: ["cmp"]
      },
      "application/vnd.youtube.yt": {
        source: "iana"
      },
      "application/vnd.zul": {
        source: "iana",
        extensions: ["zir", "zirz"]
      },
      "application/vnd.zzazz.deck+xml": {
        source: "iana",
        compressible: true,
        extensions: ["zaz"]
      },
      "application/voicexml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["vxml"]
      },
      "application/voucher-cms+json": {
        source: "iana",
        compressible: true
      },
      "application/vq-rtcpxr": {
        source: "iana"
      },
      "application/wasm": {
        source: "iana",
        compressible: true,
        extensions: ["wasm"]
      },
      "application/watcherinfo+xml": {
        source: "iana",
        compressible: true,
        extensions: ["wif"]
      },
      "application/webpush-options+json": {
        source: "iana",
        compressible: true
      },
      "application/whoispp-query": {
        source: "iana"
      },
      "application/whoispp-response": {
        source: "iana"
      },
      "application/widget": {
        source: "iana",
        extensions: ["wgt"]
      },
      "application/winhlp": {
        source: "apache",
        extensions: ["hlp"]
      },
      "application/wita": {
        source: "iana"
      },
      "application/wordperfect5.1": {
        source: "iana"
      },
      "application/wsdl+xml": {
        source: "iana",
        compressible: true,
        extensions: ["wsdl"]
      },
      "application/wspolicy+xml": {
        source: "iana",
        compressible: true,
        extensions: ["wspolicy"]
      },
      "application/x-7z-compressed": {
        source: "apache",
        compressible: false,
        extensions: ["7z"]
      },
      "application/x-abiword": {
        source: "apache",
        extensions: ["abw"]
      },
      "application/x-ace-compressed": {
        source: "apache",
        extensions: ["ace"]
      },
      "application/x-amf": {
        source: "apache"
      },
      "application/x-apple-diskimage": {
        source: "apache",
        extensions: ["dmg"]
      },
      "application/x-arj": {
        compressible: false,
        extensions: ["arj"]
      },
      "application/x-authorware-bin": {
        source: "apache",
        extensions: ["aab", "x32", "u32", "vox"]
      },
      "application/x-authorware-map": {
        source: "apache",
        extensions: ["aam"]
      },
      "application/x-authorware-seg": {
        source: "apache",
        extensions: ["aas"]
      },
      "application/x-bcpio": {
        source: "apache",
        extensions: ["bcpio"]
      },
      "application/x-bdoc": {
        compressible: false,
        extensions: ["bdoc"]
      },
      "application/x-bittorrent": {
        source: "apache",
        extensions: ["torrent"]
      },
      "application/x-blorb": {
        source: "apache",
        extensions: ["blb", "blorb"]
      },
      "application/x-bzip": {
        source: "apache",
        compressible: false,
        extensions: ["bz"]
      },
      "application/x-bzip2": {
        source: "apache",
        compressible: false,
        extensions: ["bz2", "boz"]
      },
      "application/x-cbr": {
        source: "apache",
        extensions: ["cbr", "cba", "cbt", "cbz", "cb7"]
      },
      "application/x-cdlink": {
        source: "apache",
        extensions: ["vcd"]
      },
      "application/x-cfs-compressed": {
        source: "apache",
        extensions: ["cfs"]
      },
      "application/x-chat": {
        source: "apache",
        extensions: ["chat"]
      },
      "application/x-chess-pgn": {
        source: "apache",
        extensions: ["pgn"]
      },
      "application/x-chrome-extension": {
        extensions: ["crx"]
      },
      "application/x-cocoa": {
        source: "nginx",
        extensions: ["cco"]
      },
      "application/x-compress": {
        source: "apache"
      },
      "application/x-conference": {
        source: "apache",
        extensions: ["nsc"]
      },
      "application/x-cpio": {
        source: "apache",
        extensions: ["cpio"]
      },
      "application/x-csh": {
        source: "apache",
        extensions: ["csh"]
      },
      "application/x-deb": {
        compressible: false
      },
      "application/x-debian-package": {
        source: "apache",
        extensions: ["deb", "udeb"]
      },
      "application/x-dgc-compressed": {
        source: "apache",
        extensions: ["dgc"]
      },
      "application/x-director": {
        source: "apache",
        extensions: ["dir", "dcr", "dxr", "cst", "cct", "cxt", "w3d", "fgd", "swa"]
      },
      "application/x-doom": {
        source: "apache",
        extensions: ["wad"]
      },
      "application/x-dtbncx+xml": {
        source: "apache",
        compressible: true,
        extensions: ["ncx"]
      },
      "application/x-dtbook+xml": {
        source: "apache",
        compressible: true,
        extensions: ["dtb"]
      },
      "application/x-dtbresource+xml": {
        source: "apache",
        compressible: true,
        extensions: ["res"]
      },
      "application/x-dvi": {
        source: "apache",
        compressible: false,
        extensions: ["dvi"]
      },
      "application/x-envoy": {
        source: "apache",
        extensions: ["evy"]
      },
      "application/x-eva": {
        source: "apache",
        extensions: ["eva"]
      },
      "application/x-font-bdf": {
        source: "apache",
        extensions: ["bdf"]
      },
      "application/x-font-dos": {
        source: "apache"
      },
      "application/x-font-framemaker": {
        source: "apache"
      },
      "application/x-font-ghostscript": {
        source: "apache",
        extensions: ["gsf"]
      },
      "application/x-font-libgrx": {
        source: "apache"
      },
      "application/x-font-linux-psf": {
        source: "apache",
        extensions: ["psf"]
      },
      "application/x-font-pcf": {
        source: "apache",
        extensions: ["pcf"]
      },
      "application/x-font-snf": {
        source: "apache",
        extensions: ["snf"]
      },
      "application/x-font-speedo": {
        source: "apache"
      },
      "application/x-font-sunos-news": {
        source: "apache"
      },
      "application/x-font-type1": {
        source: "apache",
        extensions: ["pfa", "pfb", "pfm", "afm"]
      },
      "application/x-font-vfont": {
        source: "apache"
      },
      "application/x-freearc": {
        source: "apache",
        extensions: ["arc"]
      },
      "application/x-futuresplash": {
        source: "apache",
        extensions: ["spl"]
      },
      "application/x-gca-compressed": {
        source: "apache",
        extensions: ["gca"]
      },
      "application/x-glulx": {
        source: "apache",
        extensions: ["ulx"]
      },
      "application/x-gnumeric": {
        source: "apache",
        extensions: ["gnumeric"]
      },
      "application/x-gramps-xml": {
        source: "apache",
        extensions: ["gramps"]
      },
      "application/x-gtar": {
        source: "apache",
        extensions: ["gtar"]
      },
      "application/x-gzip": {
        source: "apache"
      },
      "application/x-hdf": {
        source: "apache",
        extensions: ["hdf"]
      },
      "application/x-httpd-php": {
        compressible: true,
        extensions: ["php"]
      },
      "application/x-install-instructions": {
        source: "apache",
        extensions: ["install"]
      },
      "application/x-iso9660-image": {
        source: "apache",
        extensions: ["iso"]
      },
      "application/x-iwork-keynote-sffkey": {
        extensions: ["key"]
      },
      "application/x-iwork-numbers-sffnumbers": {
        extensions: ["numbers"]
      },
      "application/x-iwork-pages-sffpages": {
        extensions: ["pages"]
      },
      "application/x-java-archive-diff": {
        source: "nginx",
        extensions: ["jardiff"]
      },
      "application/x-java-jnlp-file": {
        source: "apache",
        compressible: false,
        extensions: ["jnlp"]
      },
      "application/x-javascript": {
        compressible: true
      },
      "application/x-keepass2": {
        extensions: ["kdbx"]
      },
      "application/x-latex": {
        source: "apache",
        compressible: false,
        extensions: ["latex"]
      },
      "application/x-lua-bytecode": {
        extensions: ["luac"]
      },
      "application/x-lzh-compressed": {
        source: "apache",
        extensions: ["lzh", "lha"]
      },
      "application/x-makeself": {
        source: "nginx",
        extensions: ["run"]
      },
      "application/x-mie": {
        source: "apache",
        extensions: ["mie"]
      },
      "application/x-mobipocket-ebook": {
        source: "apache",
        extensions: ["prc", "mobi"]
      },
      "application/x-mpegurl": {
        compressible: false
      },
      "application/x-ms-application": {
        source: "apache",
        extensions: ["application"]
      },
      "application/x-ms-shortcut": {
        source: "apache",
        extensions: ["lnk"]
      },
      "application/x-ms-wmd": {
        source: "apache",
        extensions: ["wmd"]
      },
      "application/x-ms-wmz": {
        source: "apache",
        extensions: ["wmz"]
      },
      "application/x-ms-xbap": {
        source: "apache",
        extensions: ["xbap"]
      },
      "application/x-msaccess": {
        source: "apache",
        extensions: ["mdb"]
      },
      "application/x-msbinder": {
        source: "apache",
        extensions: ["obd"]
      },
      "application/x-mscardfile": {
        source: "apache",
        extensions: ["crd"]
      },
      "application/x-msclip": {
        source: "apache",
        extensions: ["clp"]
      },
      "application/x-msdos-program": {
        extensions: ["exe"]
      },
      "application/x-msdownload": {
        source: "apache",
        extensions: ["exe", "dll", "com", "bat", "msi"]
      },
      "application/x-msmediaview": {
        source: "apache",
        extensions: ["mvb", "m13", "m14"]
      },
      "application/x-msmetafile": {
        source: "apache",
        extensions: ["wmf", "wmz", "emf", "emz"]
      },
      "application/x-msmoney": {
        source: "apache",
        extensions: ["mny"]
      },
      "application/x-mspublisher": {
        source: "apache",
        extensions: ["pub"]
      },
      "application/x-msschedule": {
        source: "apache",
        extensions: ["scd"]
      },
      "application/x-msterminal": {
        source: "apache",
        extensions: ["trm"]
      },
      "application/x-mswrite": {
        source: "apache",
        extensions: ["wri"]
      },
      "application/x-netcdf": {
        source: "apache",
        extensions: ["nc", "cdf"]
      },
      "application/x-ns-proxy-autoconfig": {
        compressible: true,
        extensions: ["pac"]
      },
      "application/x-nzb": {
        source: "apache",
        extensions: ["nzb"]
      },
      "application/x-perl": {
        source: "nginx",
        extensions: ["pl", "pm"]
      },
      "application/x-pilot": {
        source: "nginx",
        extensions: ["prc", "pdb"]
      },
      "application/x-pkcs12": {
        source: "apache",
        compressible: false,
        extensions: ["p12", "pfx"]
      },
      "application/x-pkcs7-certificates": {
        source: "apache",
        extensions: ["p7b", "spc"]
      },
      "application/x-pkcs7-certreqresp": {
        source: "apache",
        extensions: ["p7r"]
      },
      "application/x-pki-message": {
        source: "iana"
      },
      "application/x-rar-compressed": {
        source: "apache",
        compressible: false,
        extensions: ["rar"]
      },
      "application/x-redhat-package-manager": {
        source: "nginx",
        extensions: ["rpm"]
      },
      "application/x-research-info-systems": {
        source: "apache",
        extensions: ["ris"]
      },
      "application/x-sea": {
        source: "nginx",
        extensions: ["sea"]
      },
      "application/x-sh": {
        source: "apache",
        compressible: true,
        extensions: ["sh"]
      },
      "application/x-shar": {
        source: "apache",
        extensions: ["shar"]
      },
      "application/x-shockwave-flash": {
        source: "apache",
        compressible: false,
        extensions: ["swf"]
      },
      "application/x-silverlight-app": {
        source: "apache",
        extensions: ["xap"]
      },
      "application/x-sql": {
        source: "apache",
        extensions: ["sql"]
      },
      "application/x-stuffit": {
        source: "apache",
        compressible: false,
        extensions: ["sit"]
      },
      "application/x-stuffitx": {
        source: "apache",
        extensions: ["sitx"]
      },
      "application/x-subrip": {
        source: "apache",
        extensions: ["srt"]
      },
      "application/x-sv4cpio": {
        source: "apache",
        extensions: ["sv4cpio"]
      },
      "application/x-sv4crc": {
        source: "apache",
        extensions: ["sv4crc"]
      },
      "application/x-t3vm-image": {
        source: "apache",
        extensions: ["t3"]
      },
      "application/x-tads": {
        source: "apache",
        extensions: ["gam"]
      },
      "application/x-tar": {
        source: "apache",
        compressible: true,
        extensions: ["tar"]
      },
      "application/x-tcl": {
        source: "apache",
        extensions: ["tcl", "tk"]
      },
      "application/x-tex": {
        source: "apache",
        extensions: ["tex"]
      },
      "application/x-tex-tfm": {
        source: "apache",
        extensions: ["tfm"]
      },
      "application/x-texinfo": {
        source: "apache",
        extensions: ["texinfo", "texi"]
      },
      "application/x-tgif": {
        source: "apache",
        extensions: ["obj"]
      },
      "application/x-ustar": {
        source: "apache",
        extensions: ["ustar"]
      },
      "application/x-virtualbox-hdd": {
        compressible: true,
        extensions: ["hdd"]
      },
      "application/x-virtualbox-ova": {
        compressible: true,
        extensions: ["ova"]
      },
      "application/x-virtualbox-ovf": {
        compressible: true,
        extensions: ["ovf"]
      },
      "application/x-virtualbox-vbox": {
        compressible: true,
        extensions: ["vbox"]
      },
      "application/x-virtualbox-vbox-extpack": {
        compressible: false,
        extensions: ["vbox-extpack"]
      },
      "application/x-virtualbox-vdi": {
        compressible: true,
        extensions: ["vdi"]
      },
      "application/x-virtualbox-vhd": {
        compressible: true,
        extensions: ["vhd"]
      },
      "application/x-virtualbox-vmdk": {
        compressible: true,
        extensions: ["vmdk"]
      },
      "application/x-wais-source": {
        source: "apache",
        extensions: ["src"]
      },
      "application/x-web-app-manifest+json": {
        compressible: true,
        extensions: ["webapp"]
      },
      "application/x-www-form-urlencoded": {
        source: "iana",
        compressible: true
      },
      "application/x-x509-ca-cert": {
        source: "iana",
        extensions: ["der", "crt", "pem"]
      },
      "application/x-x509-ca-ra-cert": {
        source: "iana"
      },
      "application/x-x509-next-ca-cert": {
        source: "iana"
      },
      "application/x-xfig": {
        source: "apache",
        extensions: ["fig"]
      },
      "application/x-xliff+xml": {
        source: "apache",
        compressible: true,
        extensions: ["xlf"]
      },
      "application/x-xpinstall": {
        source: "apache",
        compressible: false,
        extensions: ["xpi"]
      },
      "application/x-xz": {
        source: "apache",
        extensions: ["xz"]
      },
      "application/x-zmachine": {
        source: "apache",
        extensions: ["z1", "z2", "z3", "z4", "z5", "z6", "z7", "z8"]
      },
      "application/x400-bp": {
        source: "iana"
      },
      "application/xacml+xml": {
        source: "iana",
        compressible: true
      },
      "application/xaml+xml": {
        source: "apache",
        compressible: true,
        extensions: ["xaml"]
      },
      "application/xcap-att+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xav"]
      },
      "application/xcap-caps+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xca"]
      },
      "application/xcap-diff+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xdf"]
      },
      "application/xcap-el+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xel"]
      },
      "application/xcap-error+xml": {
        source: "iana",
        compressible: true
      },
      "application/xcap-ns+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xns"]
      },
      "application/xcon-conference-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/xcon-conference-info-diff+xml": {
        source: "iana",
        compressible: true
      },
      "application/xenc+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xenc"]
      },
      "application/xhtml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xhtml", "xht"]
      },
      "application/xhtml-voice+xml": {
        source: "apache",
        compressible: true
      },
      "application/xliff+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xlf"]
      },
      "application/xml": {
        source: "iana",
        compressible: true,
        extensions: ["xml", "xsl", "xsd", "rng"]
      },
      "application/xml-dtd": {
        source: "iana",
        compressible: true,
        extensions: ["dtd"]
      },
      "application/xml-external-parsed-entity": {
        source: "iana"
      },
      "application/xml-patch+xml": {
        source: "iana",
        compressible: true
      },
      "application/xmpp+xml": {
        source: "iana",
        compressible: true
      },
      "application/xop+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xop"]
      },
      "application/xproc+xml": {
        source: "apache",
        compressible: true,
        extensions: ["xpl"]
      },
      "application/xslt+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xsl", "xslt"]
      },
      "application/xspf+xml": {
        source: "apache",
        compressible: true,
        extensions: ["xspf"]
      },
      "application/xv+xml": {
        source: "iana",
        compressible: true,
        extensions: ["mxml", "xhvml", "xvml", "xvm"]
      },
      "application/yang": {
        source: "iana",
        extensions: ["yang"]
      },
      "application/yang-data+json": {
        source: "iana",
        compressible: true
      },
      "application/yang-data+xml": {
        source: "iana",
        compressible: true
      },
      "application/yang-patch+json": {
        source: "iana",
        compressible: true
      },
      "application/yang-patch+xml": {
        source: "iana",
        compressible: true
      },
      "application/yin+xml": {
        source: "iana",
        compressible: true,
        extensions: ["yin"]
      },
      "application/zip": {
        source: "iana",
        compressible: false,
        extensions: ["zip"]
      },
      "application/zlib": {
        source: "iana"
      },
      "application/zstd": {
        source: "iana"
      },
      "audio/1d-interleaved-parityfec": {
        source: "iana"
      },
      "audio/32kadpcm": {
        source: "iana"
      },
      "audio/3gpp": {
        source: "iana",
        compressible: false,
        extensions: ["3gpp"]
      },
      "audio/3gpp2": {
        source: "iana"
      },
      "audio/aac": {
        source: "iana"
      },
      "audio/ac3": {
        source: "iana"
      },
      "audio/adpcm": {
        source: "apache",
        extensions: ["adp"]
      },
      "audio/amr": {
        source: "iana",
        extensions: ["amr"]
      },
      "audio/amr-wb": {
        source: "iana"
      },
      "audio/amr-wb+": {
        source: "iana"
      },
      "audio/aptx": {
        source: "iana"
      },
      "audio/asc": {
        source: "iana"
      },
      "audio/atrac-advanced-lossless": {
        source: "iana"
      },
      "audio/atrac-x": {
        source: "iana"
      },
      "audio/atrac3": {
        source: "iana"
      },
      "audio/basic": {
        source: "iana",
        compressible: false,
        extensions: ["au", "snd"]
      },
      "audio/bv16": {
        source: "iana"
      },
      "audio/bv32": {
        source: "iana"
      },
      "audio/clearmode": {
        source: "iana"
      },
      "audio/cn": {
        source: "iana"
      },
      "audio/dat12": {
        source: "iana"
      },
      "audio/dls": {
        source: "iana"
      },
      "audio/dsr-es201108": {
        source: "iana"
      },
      "audio/dsr-es202050": {
        source: "iana"
      },
      "audio/dsr-es202211": {
        source: "iana"
      },
      "audio/dsr-es202212": {
        source: "iana"
      },
      "audio/dv": {
        source: "iana"
      },
      "audio/dvi4": {
        source: "iana"
      },
      "audio/eac3": {
        source: "iana"
      },
      "audio/encaprtp": {
        source: "iana"
      },
      "audio/evrc": {
        source: "iana"
      },
      "audio/evrc-qcp": {
        source: "iana"
      },
      "audio/evrc0": {
        source: "iana"
      },
      "audio/evrc1": {
        source: "iana"
      },
      "audio/evrcb": {
        source: "iana"
      },
      "audio/evrcb0": {
        source: "iana"
      },
      "audio/evrcb1": {
        source: "iana"
      },
      "audio/evrcnw": {
        source: "iana"
      },
      "audio/evrcnw0": {
        source: "iana"
      },
      "audio/evrcnw1": {
        source: "iana"
      },
      "audio/evrcwb": {
        source: "iana"
      },
      "audio/evrcwb0": {
        source: "iana"
      },
      "audio/evrcwb1": {
        source: "iana"
      },
      "audio/evs": {
        source: "iana"
      },
      "audio/flexfec": {
        source: "iana"
      },
      "audio/fwdred": {
        source: "iana"
      },
      "audio/g711-0": {
        source: "iana"
      },
      "audio/g719": {
        source: "iana"
      },
      "audio/g722": {
        source: "iana"
      },
      "audio/g7221": {
        source: "iana"
      },
      "audio/g723": {
        source: "iana"
      },
      "audio/g726-16": {
        source: "iana"
      },
      "audio/g726-24": {
        source: "iana"
      },
      "audio/g726-32": {
        source: "iana"
      },
      "audio/g726-40": {
        source: "iana"
      },
      "audio/g728": {
        source: "iana"
      },
      "audio/g729": {
        source: "iana"
      },
      "audio/g7291": {
        source: "iana"
      },
      "audio/g729d": {
        source: "iana"
      },
      "audio/g729e": {
        source: "iana"
      },
      "audio/gsm": {
        source: "iana"
      },
      "audio/gsm-efr": {
        source: "iana"
      },
      "audio/gsm-hr-08": {
        source: "iana"
      },
      "audio/ilbc": {
        source: "iana"
      },
      "audio/ip-mr_v2.5": {
        source: "iana"
      },
      "audio/isac": {
        source: "apache"
      },
      "audio/l16": {
        source: "iana"
      },
      "audio/l20": {
        source: "iana"
      },
      "audio/l24": {
        source: "iana",
        compressible: false
      },
      "audio/l8": {
        source: "iana"
      },
      "audio/lpc": {
        source: "iana"
      },
      "audio/melp": {
        source: "iana"
      },
      "audio/melp1200": {
        source: "iana"
      },
      "audio/melp2400": {
        source: "iana"
      },
      "audio/melp600": {
        source: "iana"
      },
      "audio/mhas": {
        source: "iana"
      },
      "audio/midi": {
        source: "apache",
        extensions: ["mid", "midi", "kar", "rmi"]
      },
      "audio/mobile-xmf": {
        source: "iana",
        extensions: ["mxmf"]
      },
      "audio/mp3": {
        compressible: false,
        extensions: ["mp3"]
      },
      "audio/mp4": {
        source: "iana",
        compressible: false,
        extensions: ["m4a", "mp4a"]
      },
      "audio/mp4a-latm": {
        source: "iana"
      },
      "audio/mpa": {
        source: "iana"
      },
      "audio/mpa-robust": {
        source: "iana"
      },
      "audio/mpeg": {
        source: "iana",
        compressible: false,
        extensions: ["mpga", "mp2", "mp2a", "mp3", "m2a", "m3a"]
      },
      "audio/mpeg4-generic": {
        source: "iana"
      },
      "audio/musepack": {
        source: "apache"
      },
      "audio/ogg": {
        source: "iana",
        compressible: false,
        extensions: ["oga", "ogg", "spx", "opus"]
      },
      "audio/opus": {
        source: "iana"
      },
      "audio/parityfec": {
        source: "iana"
      },
      "audio/pcma": {
        source: "iana"
      },
      "audio/pcma-wb": {
        source: "iana"
      },
      "audio/pcmu": {
        source: "iana"
      },
      "audio/pcmu-wb": {
        source: "iana"
      },
      "audio/prs.sid": {
        source: "iana"
      },
      "audio/qcelp": {
        source: "iana"
      },
      "audio/raptorfec": {
        source: "iana"
      },
      "audio/red": {
        source: "iana"
      },
      "audio/rtp-enc-aescm128": {
        source: "iana"
      },
      "audio/rtp-midi": {
        source: "iana"
      },
      "audio/rtploopback": {
        source: "iana"
      },
      "audio/rtx": {
        source: "iana"
      },
      "audio/s3m": {
        source: "apache",
        extensions: ["s3m"]
      },
      "audio/scip": {
        source: "iana"
      },
      "audio/silk": {
        source: "apache",
        extensions: ["sil"]
      },
      "audio/smv": {
        source: "iana"
      },
      "audio/smv-qcp": {
        source: "iana"
      },
      "audio/smv0": {
        source: "iana"
      },
      "audio/sofa": {
        source: "iana"
      },
      "audio/sp-midi": {
        source: "iana"
      },
      "audio/speex": {
        source: "iana"
      },
      "audio/t140c": {
        source: "iana"
      },
      "audio/t38": {
        source: "iana"
      },
      "audio/telephone-event": {
        source: "iana"
      },
      "audio/tetra_acelp": {
        source: "iana"
      },
      "audio/tetra_acelp_bb": {
        source: "iana"
      },
      "audio/tone": {
        source: "iana"
      },
      "audio/tsvcis": {
        source: "iana"
      },
      "audio/uemclip": {
        source: "iana"
      },
      "audio/ulpfec": {
        source: "iana"
      },
      "audio/usac": {
        source: "iana"
      },
      "audio/vdvi": {
        source: "iana"
      },
      "audio/vmr-wb": {
        source: "iana"
      },
      "audio/vnd.3gpp.iufp": {
        source: "iana"
      },
      "audio/vnd.4sb": {
        source: "iana"
      },
      "audio/vnd.audiokoz": {
        source: "iana"
      },
      "audio/vnd.celp": {
        source: "iana"
      },
      "audio/vnd.cisco.nse": {
        source: "iana"
      },
      "audio/vnd.cmles.radio-events": {
        source: "iana"
      },
      "audio/vnd.cns.anp1": {
        source: "iana"
      },
      "audio/vnd.cns.inf1": {
        source: "iana"
      },
      "audio/vnd.dece.audio": {
        source: "iana",
        extensions: ["uva", "uvva"]
      },
      "audio/vnd.digital-winds": {
        source: "iana",
        extensions: ["eol"]
      },
      "audio/vnd.dlna.adts": {
        source: "iana"
      },
      "audio/vnd.dolby.heaac.1": {
        source: "iana"
      },
      "audio/vnd.dolby.heaac.2": {
        source: "iana"
      },
      "audio/vnd.dolby.mlp": {
        source: "iana"
      },
      "audio/vnd.dolby.mps": {
        source: "iana"
      },
      "audio/vnd.dolby.pl2": {
        source: "iana"
      },
      "audio/vnd.dolby.pl2x": {
        source: "iana"
      },
      "audio/vnd.dolby.pl2z": {
        source: "iana"
      },
      "audio/vnd.dolby.pulse.1": {
        source: "iana"
      },
      "audio/vnd.dra": {
        source: "iana",
        extensions: ["dra"]
      },
      "audio/vnd.dts": {
        source: "iana",
        extensions: ["dts"]
      },
      "audio/vnd.dts.hd": {
        source: "iana",
        extensions: ["dtshd"]
      },
      "audio/vnd.dts.uhd": {
        source: "iana"
      },
      "audio/vnd.dvb.file": {
        source: "iana"
      },
      "audio/vnd.everad.plj": {
        source: "iana"
      },
      "audio/vnd.hns.audio": {
        source: "iana"
      },
      "audio/vnd.lucent.voice": {
        source: "iana",
        extensions: ["lvp"]
      },
      "audio/vnd.ms-playready.media.pya": {
        source: "iana",
        extensions: ["pya"]
      },
      "audio/vnd.nokia.mobile-xmf": {
        source: "iana"
      },
      "audio/vnd.nortel.vbk": {
        source: "iana"
      },
      "audio/vnd.nuera.ecelp4800": {
        source: "iana",
        extensions: ["ecelp4800"]
      },
      "audio/vnd.nuera.ecelp7470": {
        source: "iana",
        extensions: ["ecelp7470"]
      },
      "audio/vnd.nuera.ecelp9600": {
        source: "iana",
        extensions: ["ecelp9600"]
      },
      "audio/vnd.octel.sbc": {
        source: "iana"
      },
      "audio/vnd.presonus.multitrack": {
        source: "iana"
      },
      "audio/vnd.qcelp": {
        source: "iana"
      },
      "audio/vnd.rhetorex.32kadpcm": {
        source: "iana"
      },
      "audio/vnd.rip": {
        source: "iana",
        extensions: ["rip"]
      },
      "audio/vnd.rn-realaudio": {
        compressible: false
      },
      "audio/vnd.sealedmedia.softseal.mpeg": {
        source: "iana"
      },
      "audio/vnd.vmx.cvsd": {
        source: "iana"
      },
      "audio/vnd.wave": {
        compressible: false
      },
      "audio/vorbis": {
        source: "iana",
        compressible: false
      },
      "audio/vorbis-config": {
        source: "iana"
      },
      "audio/wav": {
        compressible: false,
        extensions: ["wav"]
      },
      "audio/wave": {
        compressible: false,
        extensions: ["wav"]
      },
      "audio/webm": {
        source: "apache",
        compressible: false,
        extensions: ["weba"]
      },
      "audio/x-aac": {
        source: "apache",
        compressible: false,
        extensions: ["aac"]
      },
      "audio/x-aiff": {
        source: "apache",
        extensions: ["aif", "aiff", "aifc"]
      },
      "audio/x-caf": {
        source: "apache",
        compressible: false,
        extensions: ["caf"]
      },
      "audio/x-flac": {
        source: "apache",
        extensions: ["flac"]
      },
      "audio/x-m4a": {
        source: "nginx",
        extensions: ["m4a"]
      },
      "audio/x-matroska": {
        source: "apache",
        extensions: ["mka"]
      },
      "audio/x-mpegurl": {
        source: "apache",
        extensions: ["m3u"]
      },
      "audio/x-ms-wax": {
        source: "apache",
        extensions: ["wax"]
      },
      "audio/x-ms-wma": {
        source: "apache",
        extensions: ["wma"]
      },
      "audio/x-pn-realaudio": {
        source: "apache",
        extensions: ["ram", "ra"]
      },
      "audio/x-pn-realaudio-plugin": {
        source: "apache",
        extensions: ["rmp"]
      },
      "audio/x-realaudio": {
        source: "nginx",
        extensions: ["ra"]
      },
      "audio/x-tta": {
        source: "apache"
      },
      "audio/x-wav": {
        source: "apache",
        extensions: ["wav"]
      },
      "audio/xm": {
        source: "apache",
        extensions: ["xm"]
      },
      "chemical/x-cdx": {
        source: "apache",
        extensions: ["cdx"]
      },
      "chemical/x-cif": {
        source: "apache",
        extensions: ["cif"]
      },
      "chemical/x-cmdf": {
        source: "apache",
        extensions: ["cmdf"]
      },
      "chemical/x-cml": {
        source: "apache",
        extensions: ["cml"]
      },
      "chemical/x-csml": {
        source: "apache",
        extensions: ["csml"]
      },
      "chemical/x-pdb": {
        source: "apache"
      },
      "chemical/x-xyz": {
        source: "apache",
        extensions: ["xyz"]
      },
      "font/collection": {
        source: "iana",
        extensions: ["ttc"]
      },
      "font/otf": {
        source: "iana",
        compressible: true,
        extensions: ["otf"]
      },
      "font/sfnt": {
        source: "iana"
      },
      "font/ttf": {
        source: "iana",
        compressible: true,
        extensions: ["ttf"]
      },
      "font/woff": {
        source: "iana",
        extensions: ["woff"]
      },
      "font/woff2": {
        source: "iana",
        extensions: ["woff2"]
      },
      "image/aces": {
        source: "iana",
        extensions: ["exr"]
      },
      "image/apng": {
        compressible: false,
        extensions: ["apng"]
      },
      "image/avci": {
        source: "iana",
        extensions: ["avci"]
      },
      "image/avcs": {
        source: "iana",
        extensions: ["avcs"]
      },
      "image/avif": {
        source: "iana",
        compressible: false,
        extensions: ["avif"]
      },
      "image/bmp": {
        source: "iana",
        compressible: true,
        extensions: ["bmp"]
      },
      "image/cgm": {
        source: "iana",
        extensions: ["cgm"]
      },
      "image/dicom-rle": {
        source: "iana",
        extensions: ["drle"]
      },
      "image/emf": {
        source: "iana",
        extensions: ["emf"]
      },
      "image/fits": {
        source: "iana",
        extensions: ["fits"]
      },
      "image/g3fax": {
        source: "iana",
        extensions: ["g3"]
      },
      "image/gif": {
        source: "iana",
        compressible: false,
        extensions: ["gif"]
      },
      "image/heic": {
        source: "iana",
        extensions: ["heic"]
      },
      "image/heic-sequence": {
        source: "iana",
        extensions: ["heics"]
      },
      "image/heif": {
        source: "iana",
        extensions: ["heif"]
      },
      "image/heif-sequence": {
        source: "iana",
        extensions: ["heifs"]
      },
      "image/hej2k": {
        source: "iana",
        extensions: ["hej2"]
      },
      "image/hsj2": {
        source: "iana",
        extensions: ["hsj2"]
      },
      "image/ief": {
        source: "iana",
        extensions: ["ief"]
      },
      "image/jls": {
        source: "iana",
        extensions: ["jls"]
      },
      "image/jp2": {
        source: "iana",
        compressible: false,
        extensions: ["jp2", "jpg2"]
      },
      "image/jpeg": {
        source: "iana",
        compressible: false,
        extensions: ["jpeg", "jpg", "jpe"]
      },
      "image/jph": {
        source: "iana",
        extensions: ["jph"]
      },
      "image/jphc": {
        source: "iana",
        extensions: ["jhc"]
      },
      "image/jpm": {
        source: "iana",
        compressible: false,
        extensions: ["jpm"]
      },
      "image/jpx": {
        source: "iana",
        compressible: false,
        extensions: ["jpx", "jpf"]
      },
      "image/jxr": {
        source: "iana",
        extensions: ["jxr"]
      },
      "image/jxra": {
        source: "iana",
        extensions: ["jxra"]
      },
      "image/jxrs": {
        source: "iana",
        extensions: ["jxrs"]
      },
      "image/jxs": {
        source: "iana",
        extensions: ["jxs"]
      },
      "image/jxsc": {
        source: "iana",
        extensions: ["jxsc"]
      },
      "image/jxsi": {
        source: "iana",
        extensions: ["jxsi"]
      },
      "image/jxss": {
        source: "iana",
        extensions: ["jxss"]
      },
      "image/ktx": {
        source: "iana",
        extensions: ["ktx"]
      },
      "image/ktx2": {
        source: "iana",
        extensions: ["ktx2"]
      },
      "image/naplps": {
        source: "iana"
      },
      "image/pjpeg": {
        compressible: false
      },
      "image/png": {
        source: "iana",
        compressible: false,
        extensions: ["png"]
      },
      "image/prs.btif": {
        source: "iana",
        extensions: ["btif"]
      },
      "image/prs.pti": {
        source: "iana",
        extensions: ["pti"]
      },
      "image/pwg-raster": {
        source: "iana"
      },
      "image/sgi": {
        source: "apache",
        extensions: ["sgi"]
      },
      "image/svg+xml": {
        source: "iana",
        compressible: true,
        extensions: ["svg", "svgz"]
      },
      "image/t38": {
        source: "iana",
        extensions: ["t38"]
      },
      "image/tiff": {
        source: "iana",
        compressible: false,
        extensions: ["tif", "tiff"]
      },
      "image/tiff-fx": {
        source: "iana",
        extensions: ["tfx"]
      },
      "image/vnd.adobe.photoshop": {
        source: "iana",
        compressible: true,
        extensions: ["psd"]
      },
      "image/vnd.airzip.accelerator.azv": {
        source: "iana",
        extensions: ["azv"]
      },
      "image/vnd.cns.inf2": {
        source: "iana"
      },
      "image/vnd.dece.graphic": {
        source: "iana",
        extensions: ["uvi", "uvvi", "uvg", "uvvg"]
      },
      "image/vnd.djvu": {
        source: "iana",
        extensions: ["djvu", "djv"]
      },
      "image/vnd.dvb.subtitle": {
        source: "iana",
        extensions: ["sub"]
      },
      "image/vnd.dwg": {
        source: "iana",
        extensions: ["dwg"]
      },
      "image/vnd.dxf": {
        source: "iana",
        extensions: ["dxf"]
      },
      "image/vnd.fastbidsheet": {
        source: "iana",
        extensions: ["fbs"]
      },
      "image/vnd.fpx": {
        source: "iana",
        extensions: ["fpx"]
      },
      "image/vnd.fst": {
        source: "iana",
        extensions: ["fst"]
      },
      "image/vnd.fujixerox.edmics-mmr": {
        source: "iana",
        extensions: ["mmr"]
      },
      "image/vnd.fujixerox.edmics-rlc": {
        source: "iana",
        extensions: ["rlc"]
      },
      "image/vnd.globalgraphics.pgb": {
        source: "iana"
      },
      "image/vnd.microsoft.icon": {
        source: "iana",
        compressible: true,
        extensions: ["ico"]
      },
      "image/vnd.mix": {
        source: "iana"
      },
      "image/vnd.mozilla.apng": {
        source: "iana"
      },
      "image/vnd.ms-dds": {
        compressible: true,
        extensions: ["dds"]
      },
      "image/vnd.ms-modi": {
        source: "iana",
        extensions: ["mdi"]
      },
      "image/vnd.ms-photo": {
        source: "apache",
        extensions: ["wdp"]
      },
      "image/vnd.net-fpx": {
        source: "iana",
        extensions: ["npx"]
      },
      "image/vnd.pco.b16": {
        source: "iana",
        extensions: ["b16"]
      },
      "image/vnd.radiance": {
        source: "iana"
      },
      "image/vnd.sealed.png": {
        source: "iana"
      },
      "image/vnd.sealedmedia.softseal.gif": {
        source: "iana"
      },
      "image/vnd.sealedmedia.softseal.jpg": {
        source: "iana"
      },
      "image/vnd.svf": {
        source: "iana"
      },
      "image/vnd.tencent.tap": {
        source: "iana",
        extensions: ["tap"]
      },
      "image/vnd.valve.source.texture": {
        source: "iana",
        extensions: ["vtf"]
      },
      "image/vnd.wap.wbmp": {
        source: "iana",
        extensions: ["wbmp"]
      },
      "image/vnd.xiff": {
        source: "iana",
        extensions: ["xif"]
      },
      "image/vnd.zbrush.pcx": {
        source: "iana",
        extensions: ["pcx"]
      },
      "image/webp": {
        source: "apache",
        extensions: ["webp"]
      },
      "image/wmf": {
        source: "iana",
        extensions: ["wmf"]
      },
      "image/x-3ds": {
        source: "apache",
        extensions: ["3ds"]
      },
      "image/x-cmu-raster": {
        source: "apache",
        extensions: ["ras"]
      },
      "image/x-cmx": {
        source: "apache",
        extensions: ["cmx"]
      },
      "image/x-freehand": {
        source: "apache",
        extensions: ["fh", "fhc", "fh4", "fh5", "fh7"]
      },
      "image/x-icon": {
        source: "apache",
        compressible: true,
        extensions: ["ico"]
      },
      "image/x-jng": {
        source: "nginx",
        extensions: ["jng"]
      },
      "image/x-mrsid-image": {
        source: "apache",
        extensions: ["sid"]
      },
      "image/x-ms-bmp": {
        source: "nginx",
        compressible: true,
        extensions: ["bmp"]
      },
      "image/x-pcx": {
        source: "apache",
        extensions: ["pcx"]
      },
      "image/x-pict": {
        source: "apache",
        extensions: ["pic", "pct"]
      },
      "image/x-portable-anymap": {
        source: "apache",
        extensions: ["pnm"]
      },
      "image/x-portable-bitmap": {
        source: "apache",
        extensions: ["pbm"]
      },
      "image/x-portable-graymap": {
        source: "apache",
        extensions: ["pgm"]
      },
      "image/x-portable-pixmap": {
        source: "apache",
        extensions: ["ppm"]
      },
      "image/x-rgb": {
        source: "apache",
        extensions: ["rgb"]
      },
      "image/x-tga": {
        source: "apache",
        extensions: ["tga"]
      },
      "image/x-xbitmap": {
        source: "apache",
        extensions: ["xbm"]
      },
      "image/x-xcf": {
        compressible: false
      },
      "image/x-xpixmap": {
        source: "apache",
        extensions: ["xpm"]
      },
      "image/x-xwindowdump": {
        source: "apache",
        extensions: ["xwd"]
      },
      "message/cpim": {
        source: "iana"
      },
      "message/delivery-status": {
        source: "iana"
      },
      "message/disposition-notification": {
        source: "iana",
        extensions: [
          "disposition-notification"
        ]
      },
      "message/external-body": {
        source: "iana"
      },
      "message/feedback-report": {
        source: "iana"
      },
      "message/global": {
        source: "iana",
        extensions: ["u8msg"]
      },
      "message/global-delivery-status": {
        source: "iana",
        extensions: ["u8dsn"]
      },
      "message/global-disposition-notification": {
        source: "iana",
        extensions: ["u8mdn"]
      },
      "message/global-headers": {
        source: "iana",
        extensions: ["u8hdr"]
      },
      "message/http": {
        source: "iana",
        compressible: false
      },
      "message/imdn+xml": {
        source: "iana",
        compressible: true
      },
      "message/news": {
        source: "iana"
      },
      "message/partial": {
        source: "iana",
        compressible: false
      },
      "message/rfc822": {
        source: "iana",
        compressible: true,
        extensions: ["eml", "mime"]
      },
      "message/s-http": {
        source: "iana"
      },
      "message/sip": {
        source: "iana"
      },
      "message/sipfrag": {
        source: "iana"
      },
      "message/tracking-status": {
        source: "iana"
      },
      "message/vnd.si.simp": {
        source: "iana"
      },
      "message/vnd.wfa.wsc": {
        source: "iana",
        extensions: ["wsc"]
      },
      "model/3mf": {
        source: "iana",
        extensions: ["3mf"]
      },
      "model/e57": {
        source: "iana"
      },
      "model/gltf+json": {
        source: "iana",
        compressible: true,
        extensions: ["gltf"]
      },
      "model/gltf-binary": {
        source: "iana",
        compressible: true,
        extensions: ["glb"]
      },
      "model/iges": {
        source: "iana",
        compressible: false,
        extensions: ["igs", "iges"]
      },
      "model/mesh": {
        source: "iana",
        compressible: false,
        extensions: ["msh", "mesh", "silo"]
      },
      "model/mtl": {
        source: "iana",
        extensions: ["mtl"]
      },
      "model/obj": {
        source: "iana",
        extensions: ["obj"]
      },
      "model/step": {
        source: "iana"
      },
      "model/step+xml": {
        source: "iana",
        compressible: true,
        extensions: ["stpx"]
      },
      "model/step+zip": {
        source: "iana",
        compressible: false,
        extensions: ["stpz"]
      },
      "model/step-xml+zip": {
        source: "iana",
        compressible: false,
        extensions: ["stpxz"]
      },
      "model/stl": {
        source: "iana",
        extensions: ["stl"]
      },
      "model/vnd.collada+xml": {
        source: "iana",
        compressible: true,
        extensions: ["dae"]
      },
      "model/vnd.dwf": {
        source: "iana",
        extensions: ["dwf"]
      },
      "model/vnd.flatland.3dml": {
        source: "iana"
      },
      "model/vnd.gdl": {
        source: "iana",
        extensions: ["gdl"]
      },
      "model/vnd.gs-gdl": {
        source: "apache"
      },
      "model/vnd.gs.gdl": {
        source: "iana"
      },
      "model/vnd.gtw": {
        source: "iana",
        extensions: ["gtw"]
      },
      "model/vnd.moml+xml": {
        source: "iana",
        compressible: true
      },
      "model/vnd.mts": {
        source: "iana",
        extensions: ["mts"]
      },
      "model/vnd.opengex": {
        source: "iana",
        extensions: ["ogex"]
      },
      "model/vnd.parasolid.transmit.binary": {
        source: "iana",
        extensions: ["x_b"]
      },
      "model/vnd.parasolid.transmit.text": {
        source: "iana",
        extensions: ["x_t"]
      },
      "model/vnd.pytha.pyox": {
        source: "iana"
      },
      "model/vnd.rosette.annotated-data-model": {
        source: "iana"
      },
      "model/vnd.sap.vds": {
        source: "iana",
        extensions: ["vds"]
      },
      "model/vnd.usdz+zip": {
        source: "iana",
        compressible: false,
        extensions: ["usdz"]
      },
      "model/vnd.valve.source.compiled-map": {
        source: "iana",
        extensions: ["bsp"]
      },
      "model/vnd.vtu": {
        source: "iana",
        extensions: ["vtu"]
      },
      "model/vrml": {
        source: "iana",
        compressible: false,
        extensions: ["wrl", "vrml"]
      },
      "model/x3d+binary": {
        source: "apache",
        compressible: false,
        extensions: ["x3db", "x3dbz"]
      },
      "model/x3d+fastinfoset": {
        source: "iana",
        extensions: ["x3db"]
      },
      "model/x3d+vrml": {
        source: "apache",
        compressible: false,
        extensions: ["x3dv", "x3dvz"]
      },
      "model/x3d+xml": {
        source: "iana",
        compressible: true,
        extensions: ["x3d", "x3dz"]
      },
      "model/x3d-vrml": {
        source: "iana",
        extensions: ["x3dv"]
      },
      "multipart/alternative": {
        source: "iana",
        compressible: false
      },
      "multipart/appledouble": {
        source: "iana"
      },
      "multipart/byteranges": {
        source: "iana"
      },
      "multipart/digest": {
        source: "iana"
      },
      "multipart/encrypted": {
        source: "iana",
        compressible: false
      },
      "multipart/form-data": {
        source: "iana",
        compressible: false
      },
      "multipart/header-set": {
        source: "iana"
      },
      "multipart/mixed": {
        source: "iana"
      },
      "multipart/multilingual": {
        source: "iana"
      },
      "multipart/parallel": {
        source: "iana"
      },
      "multipart/related": {
        source: "iana",
        compressible: false
      },
      "multipart/report": {
        source: "iana"
      },
      "multipart/signed": {
        source: "iana",
        compressible: false
      },
      "multipart/vnd.bint.med-plus": {
        source: "iana"
      },
      "multipart/voice-message": {
        source: "iana"
      },
      "multipart/x-mixed-replace": {
        source: "iana"
      },
      "text/1d-interleaved-parityfec": {
        source: "iana"
      },
      "text/cache-manifest": {
        source: "iana",
        compressible: true,
        extensions: ["appcache", "manifest"]
      },
      "text/calendar": {
        source: "iana",
        extensions: ["ics", "ifb"]
      },
      "text/calender": {
        compressible: true
      },
      "text/cmd": {
        compressible: true
      },
      "text/coffeescript": {
        extensions: ["coffee", "litcoffee"]
      },
      "text/cql": {
        source: "iana"
      },
      "text/cql-expression": {
        source: "iana"
      },
      "text/cql-identifier": {
        source: "iana"
      },
      "text/css": {
        source: "iana",
        charset: "UTF-8",
        compressible: true,
        extensions: ["css"]
      },
      "text/csv": {
        source: "iana",
        compressible: true,
        extensions: ["csv"]
      },
      "text/csv-schema": {
        source: "iana"
      },
      "text/directory": {
        source: "iana"
      },
      "text/dns": {
        source: "iana"
      },
      "text/ecmascript": {
        source: "iana"
      },
      "text/encaprtp": {
        source: "iana"
      },
      "text/enriched": {
        source: "iana"
      },
      "text/fhirpath": {
        source: "iana"
      },
      "text/flexfec": {
        source: "iana"
      },
      "text/fwdred": {
        source: "iana"
      },
      "text/gff3": {
        source: "iana"
      },
      "text/grammar-ref-list": {
        source: "iana"
      },
      "text/html": {
        source: "iana",
        compressible: true,
        extensions: ["html", "htm", "shtml"]
      },
      "text/jade": {
        extensions: ["jade"]
      },
      "text/javascript": {
        source: "iana",
        compressible: true
      },
      "text/jcr-cnd": {
        source: "iana"
      },
      "text/jsx": {
        compressible: true,
        extensions: ["jsx"]
      },
      "text/less": {
        compressible: true,
        extensions: ["less"]
      },
      "text/markdown": {
        source: "iana",
        compressible: true,
        extensions: ["markdown", "md"]
      },
      "text/mathml": {
        source: "nginx",
        extensions: ["mml"]
      },
      "text/mdx": {
        compressible: true,
        extensions: ["mdx"]
      },
      "text/mizar": {
        source: "iana"
      },
      "text/n3": {
        source: "iana",
        charset: "UTF-8",
        compressible: true,
        extensions: ["n3"]
      },
      "text/parameters": {
        source: "iana",
        charset: "UTF-8"
      },
      "text/parityfec": {
        source: "iana"
      },
      "text/plain": {
        source: "iana",
        compressible: true,
        extensions: ["txt", "text", "conf", "def", "list", "log", "in", "ini"]
      },
      "text/provenance-notation": {
        source: "iana",
        charset: "UTF-8"
      },
      "text/prs.fallenstein.rst": {
        source: "iana"
      },
      "text/prs.lines.tag": {
        source: "iana",
        extensions: ["dsc"]
      },
      "text/prs.prop.logic": {
        source: "iana"
      },
      "text/raptorfec": {
        source: "iana"
      },
      "text/red": {
        source: "iana"
      },
      "text/rfc822-headers": {
        source: "iana"
      },
      "text/richtext": {
        source: "iana",
        compressible: true,
        extensions: ["rtx"]
      },
      "text/rtf": {
        source: "iana",
        compressible: true,
        extensions: ["rtf"]
      },
      "text/rtp-enc-aescm128": {
        source: "iana"
      },
      "text/rtploopback": {
        source: "iana"
      },
      "text/rtx": {
        source: "iana"
      },
      "text/sgml": {
        source: "iana",
        extensions: ["sgml", "sgm"]
      },
      "text/shaclc": {
        source: "iana"
      },
      "text/shex": {
        source: "iana",
        extensions: ["shex"]
      },
      "text/slim": {
        extensions: ["slim", "slm"]
      },
      "text/spdx": {
        source: "iana",
        extensions: ["spdx"]
      },
      "text/strings": {
        source: "iana"
      },
      "text/stylus": {
        extensions: ["stylus", "styl"]
      },
      "text/t140": {
        source: "iana"
      },
      "text/tab-separated-values": {
        source: "iana",
        compressible: true,
        extensions: ["tsv"]
      },
      "text/troff": {
        source: "iana",
        extensions: ["t", "tr", "roff", "man", "me", "ms"]
      },
      "text/turtle": {
        source: "iana",
        charset: "UTF-8",
        extensions: ["ttl"]
      },
      "text/ulpfec": {
        source: "iana"
      },
      "text/uri-list": {
        source: "iana",
        compressible: true,
        extensions: ["uri", "uris", "urls"]
      },
      "text/vcard": {
        source: "iana",
        compressible: true,
        extensions: ["vcard"]
      },
      "text/vnd.a": {
        source: "iana"
      },
      "text/vnd.abc": {
        source: "iana"
      },
      "text/vnd.ascii-art": {
        source: "iana"
      },
      "text/vnd.curl": {
        source: "iana",
        extensions: ["curl"]
      },
      "text/vnd.curl.dcurl": {
        source: "apache",
        extensions: ["dcurl"]
      },
      "text/vnd.curl.mcurl": {
        source: "apache",
        extensions: ["mcurl"]
      },
      "text/vnd.curl.scurl": {
        source: "apache",
        extensions: ["scurl"]
      },
      "text/vnd.debian.copyright": {
        source: "iana",
        charset: "UTF-8"
      },
      "text/vnd.dmclientscript": {
        source: "iana"
      },
      "text/vnd.dvb.subtitle": {
        source: "iana",
        extensions: ["sub"]
      },
      "text/vnd.esmertec.theme-descriptor": {
        source: "iana",
        charset: "UTF-8"
      },
      "text/vnd.familysearch.gedcom": {
        source: "iana",
        extensions: ["ged"]
      },
      "text/vnd.ficlab.flt": {
        source: "iana"
      },
      "text/vnd.fly": {
        source: "iana",
        extensions: ["fly"]
      },
      "text/vnd.fmi.flexstor": {
        source: "iana",
        extensions: ["flx"]
      },
      "text/vnd.gml": {
        source: "iana"
      },
      "text/vnd.graphviz": {
        source: "iana",
        extensions: ["gv"]
      },
      "text/vnd.hans": {
        source: "iana"
      },
      "text/vnd.hgl": {
        source: "iana"
      },
      "text/vnd.in3d.3dml": {
        source: "iana",
        extensions: ["3dml"]
      },
      "text/vnd.in3d.spot": {
        source: "iana",
        extensions: ["spot"]
      },
      "text/vnd.iptc.newsml": {
        source: "iana"
      },
      "text/vnd.iptc.nitf": {
        source: "iana"
      },
      "text/vnd.latex-z": {
        source: "iana"
      },
      "text/vnd.motorola.reflex": {
        source: "iana"
      },
      "text/vnd.ms-mediapackage": {
        source: "iana"
      },
      "text/vnd.net2phone.commcenter.command": {
        source: "iana"
      },
      "text/vnd.radisys.msml-basic-layout": {
        source: "iana"
      },
      "text/vnd.senx.warpscript": {
        source: "iana"
      },
      "text/vnd.si.uricatalogue": {
        source: "iana"
      },
      "text/vnd.sosi": {
        source: "iana"
      },
      "text/vnd.sun.j2me.app-descriptor": {
        source: "iana",
        charset: "UTF-8",
        extensions: ["jad"]
      },
      "text/vnd.trolltech.linguist": {
        source: "iana",
        charset: "UTF-8"
      },
      "text/vnd.wap.si": {
        source: "iana"
      },
      "text/vnd.wap.sl": {
        source: "iana"
      },
      "text/vnd.wap.wml": {
        source: "iana",
        extensions: ["wml"]
      },
      "text/vnd.wap.wmlscript": {
        source: "iana",
        extensions: ["wmls"]
      },
      "text/vtt": {
        source: "iana",
        charset: "UTF-8",
        compressible: true,
        extensions: ["vtt"]
      },
      "text/x-asm": {
        source: "apache",
        extensions: ["s", "asm"]
      },
      "text/x-c": {
        source: "apache",
        extensions: ["c", "cc", "cxx", "cpp", "h", "hh", "dic"]
      },
      "text/x-component": {
        source: "nginx",
        extensions: ["htc"]
      },
      "text/x-fortran": {
        source: "apache",
        extensions: ["f", "for", "f77", "f90"]
      },
      "text/x-gwt-rpc": {
        compressible: true
      },
      "text/x-handlebars-template": {
        extensions: ["hbs"]
      },
      "text/x-java-source": {
        source: "apache",
        extensions: ["java"]
      },
      "text/x-jquery-tmpl": {
        compressible: true
      },
      "text/x-lua": {
        extensions: ["lua"]
      },
      "text/x-markdown": {
        compressible: true,
        extensions: ["mkd"]
      },
      "text/x-nfo": {
        source: "apache",
        extensions: ["nfo"]
      },
      "text/x-opml": {
        source: "apache",
        extensions: ["opml"]
      },
      "text/x-org": {
        compressible: true,
        extensions: ["org"]
      },
      "text/x-pascal": {
        source: "apache",
        extensions: ["p", "pas"]
      },
      "text/x-processing": {
        compressible: true,
        extensions: ["pde"]
      },
      "text/x-sass": {
        extensions: ["sass"]
      },
      "text/x-scss": {
        extensions: ["scss"]
      },
      "text/x-setext": {
        source: "apache",
        extensions: ["etx"]
      },
      "text/x-sfv": {
        source: "apache",
        extensions: ["sfv"]
      },
      "text/x-suse-ymp": {
        compressible: true,
        extensions: ["ymp"]
      },
      "text/x-uuencode": {
        source: "apache",
        extensions: ["uu"]
      },
      "text/x-vcalendar": {
        source: "apache",
        extensions: ["vcs"]
      },
      "text/x-vcard": {
        source: "apache",
        extensions: ["vcf"]
      },
      "text/xml": {
        source: "iana",
        compressible: true,
        extensions: ["xml"]
      },
      "text/xml-external-parsed-entity": {
        source: "iana"
      },
      "text/yaml": {
        compressible: true,
        extensions: ["yaml", "yml"]
      },
      "video/1d-interleaved-parityfec": {
        source: "iana"
      },
      "video/3gpp": {
        source: "iana",
        extensions: ["3gp", "3gpp"]
      },
      "video/3gpp-tt": {
        source: "iana"
      },
      "video/3gpp2": {
        source: "iana",
        extensions: ["3g2"]
      },
      "video/av1": {
        source: "iana"
      },
      "video/bmpeg": {
        source: "iana"
      },
      "video/bt656": {
        source: "iana"
      },
      "video/celb": {
        source: "iana"
      },
      "video/dv": {
        source: "iana"
      },
      "video/encaprtp": {
        source: "iana"
      },
      "video/ffv1": {
        source: "iana"
      },
      "video/flexfec": {
        source: "iana"
      },
      "video/h261": {
        source: "iana",
        extensions: ["h261"]
      },
      "video/h263": {
        source: "iana",
        extensions: ["h263"]
      },
      "video/h263-1998": {
        source: "iana"
      },
      "video/h263-2000": {
        source: "iana"
      },
      "video/h264": {
        source: "iana",
        extensions: ["h264"]
      },
      "video/h264-rcdo": {
        source: "iana"
      },
      "video/h264-svc": {
        source: "iana"
      },
      "video/h265": {
        source: "iana"
      },
      "video/iso.segment": {
        source: "iana",
        extensions: ["m4s"]
      },
      "video/jpeg": {
        source: "iana",
        extensions: ["jpgv"]
      },
      "video/jpeg2000": {
        source: "iana"
      },
      "video/jpm": {
        source: "apache",
        extensions: ["jpm", "jpgm"]
      },
      "video/jxsv": {
        source: "iana"
      },
      "video/mj2": {
        source: "iana",
        extensions: ["mj2", "mjp2"]
      },
      "video/mp1s": {
        source: "iana"
      },
      "video/mp2p": {
        source: "iana"
      },
      "video/mp2t": {
        source: "iana",
        extensions: ["ts"]
      },
      "video/mp4": {
        source: "iana",
        compressible: false,
        extensions: ["mp4", "mp4v", "mpg4"]
      },
      "video/mp4v-es": {
        source: "iana"
      },
      "video/mpeg": {
        source: "iana",
        compressible: false,
        extensions: ["mpeg", "mpg", "mpe", "m1v", "m2v"]
      },
      "video/mpeg4-generic": {
        source: "iana"
      },
      "video/mpv": {
        source: "iana"
      },
      "video/nv": {
        source: "iana"
      },
      "video/ogg": {
        source: "iana",
        compressible: false,
        extensions: ["ogv"]
      },
      "video/parityfec": {
        source: "iana"
      },
      "video/pointer": {
        source: "iana"
      },
      "video/quicktime": {
        source: "iana",
        compressible: false,
        extensions: ["qt", "mov"]
      },
      "video/raptorfec": {
        source: "iana"
      },
      "video/raw": {
        source: "iana"
      },
      "video/rtp-enc-aescm128": {
        source: "iana"
      },
      "video/rtploopback": {
        source: "iana"
      },
      "video/rtx": {
        source: "iana"
      },
      "video/scip": {
        source: "iana"
      },
      "video/smpte291": {
        source: "iana"
      },
      "video/smpte292m": {
        source: "iana"
      },
      "video/ulpfec": {
        source: "iana"
      },
      "video/vc1": {
        source: "iana"
      },
      "video/vc2": {
        source: "iana"
      },
      "video/vnd.cctv": {
        source: "iana"
      },
      "video/vnd.dece.hd": {
        source: "iana",
        extensions: ["uvh", "uvvh"]
      },
      "video/vnd.dece.mobile": {
        source: "iana",
        extensions: ["uvm", "uvvm"]
      },
      "video/vnd.dece.mp4": {
        source: "iana"
      },
      "video/vnd.dece.pd": {
        source: "iana",
        extensions: ["uvp", "uvvp"]
      },
      "video/vnd.dece.sd": {
        source: "iana",
        extensions: ["uvs", "uvvs"]
      },
      "video/vnd.dece.video": {
        source: "iana",
        extensions: ["uvv", "uvvv"]
      },
      "video/vnd.directv.mpeg": {
        source: "iana"
      },
      "video/vnd.directv.mpeg-tts": {
        source: "iana"
      },
      "video/vnd.dlna.mpeg-tts": {
        source: "iana"
      },
      "video/vnd.dvb.file": {
        source: "iana",
        extensions: ["dvb"]
      },
      "video/vnd.fvt": {
        source: "iana",
        extensions: ["fvt"]
      },
      "video/vnd.hns.video": {
        source: "iana"
      },
      "video/vnd.iptvforum.1dparityfec-1010": {
        source: "iana"
      },
      "video/vnd.iptvforum.1dparityfec-2005": {
        source: "iana"
      },
      "video/vnd.iptvforum.2dparityfec-1010": {
        source: "iana"
      },
      "video/vnd.iptvforum.2dparityfec-2005": {
        source: "iana"
      },
      "video/vnd.iptvforum.ttsavc": {
        source: "iana"
      },
      "video/vnd.iptvforum.ttsmpeg2": {
        source: "iana"
      },
      "video/vnd.motorola.video": {
        source: "iana"
      },
      "video/vnd.motorola.videop": {
        source: "iana"
      },
      "video/vnd.mpegurl": {
        source: "iana",
        extensions: ["mxu", "m4u"]
      },
      "video/vnd.ms-playready.media.pyv": {
        source: "iana",
        extensions: ["pyv"]
      },
      "video/vnd.nokia.interleaved-multimedia": {
        source: "iana"
      },
      "video/vnd.nokia.mp4vr": {
        source: "iana"
      },
      "video/vnd.nokia.videovoip": {
        source: "iana"
      },
      "video/vnd.objectvideo": {
        source: "iana"
      },
      "video/vnd.radgamettools.bink": {
        source: "iana"
      },
      "video/vnd.radgamettools.smacker": {
        source: "iana"
      },
      "video/vnd.sealed.mpeg1": {
        source: "iana"
      },
      "video/vnd.sealed.mpeg4": {
        source: "iana"
      },
      "video/vnd.sealed.swf": {
        source: "iana"
      },
      "video/vnd.sealedmedia.softseal.mov": {
        source: "iana"
      },
      "video/vnd.uvvu.mp4": {
        source: "iana",
        extensions: ["uvu", "uvvu"]
      },
      "video/vnd.vivo": {
        source: "iana",
        extensions: ["viv"]
      },
      "video/vnd.youtube.yt": {
        source: "iana"
      },
      "video/vp8": {
        source: "iana"
      },
      "video/vp9": {
        source: "iana"
      },
      "video/webm": {
        source: "apache",
        compressible: false,
        extensions: ["webm"]
      },
      "video/x-f4v": {
        source: "apache",
        extensions: ["f4v"]
      },
      "video/x-fli": {
        source: "apache",
        extensions: ["fli"]
      },
      "video/x-flv": {
        source: "apache",
        compressible: false,
        extensions: ["flv"]
      },
      "video/x-m4v": {
        source: "apache",
        extensions: ["m4v"]
      },
      "video/x-matroska": {
        source: "apache",
        compressible: false,
        extensions: ["mkv", "mk3d", "mks"]
      },
      "video/x-mng": {
        source: "apache",
        extensions: ["mng"]
      },
      "video/x-ms-asf": {
        source: "apache",
        extensions: ["asf", "asx"]
      },
      "video/x-ms-vob": {
        source: "apache",
        extensions: ["vob"]
      },
      "video/x-ms-wm": {
        source: "apache",
        extensions: ["wm"]
      },
      "video/x-ms-wmv": {
        source: "apache",
        compressible: false,
        extensions: ["wmv"]
      },
      "video/x-ms-wmx": {
        source: "apache",
        extensions: ["wmx"]
      },
      "video/x-ms-wvx": {
        source: "apache",
        extensions: ["wvx"]
      },
      "video/x-msvideo": {
        source: "apache",
        extensions: ["avi"]
      },
      "video/x-sgi-movie": {
        source: "apache",
        extensions: ["movie"]
      },
      "video/x-smv": {
        source: "apache",
        extensions: ["smv"]
      },
      "x-conference/x-cooltalk": {
        source: "apache",
        extensions: ["ice"]
      },
      "x-shader/x-fragment": {
        compressible: true
      },
      "x-shader/x-vertex": {
        compressible: true
      }
    };
  }
});

// node_modules/mime-db/index.js
var require_mime_db = __commonJS({
  "node_modules/mime-db/index.js"(exports, module) {
    module.exports = require_db();
  }
});

// node_modules/mime-types/index.js
var require_mime_types = __commonJS({
  "node_modules/mime-types/index.js"(exports) {
    "use strict";
    var db = require_mime_db();
    var extname = __require("path").extname;
    var EXTRACT_TYPE_REGEXP = /^\s*([^;\s]*)(?:;|\s|$)/;
    var TEXT_TYPE_REGEXP = /^text\//i;
    exports.charset = charset;
    exports.charsets = { lookup: charset };
    exports.contentType = contentType;
    exports.extension = extension2;
    exports.extensions = /* @__PURE__ */ Object.create(null);
    exports.lookup = lookup2;
    exports.types = /* @__PURE__ */ Object.create(null);
    populateMaps(exports.extensions, exports.types);
    function charset(type) {
      if (!type || typeof type !== "string") {
        return false;
      }
      var match = EXTRACT_TYPE_REGEXP.exec(type);
      var mime = match && db[match[1].toLowerCase()];
      if (mime && mime.charset) {
        return mime.charset;
      }
      if (match && TEXT_TYPE_REGEXP.test(match[1])) {
        return "UTF-8";
      }
      return false;
    }
    function contentType(str) {
      if (!str || typeof str !== "string") {
        return false;
      }
      var mime = str.indexOf("/") === -1 ? exports.lookup(str) : str;
      if (!mime) {
        return false;
      }
      if (mime.indexOf("charset") === -1) {
        var charset2 = exports.charset(mime);
        if (charset2) mime += "; charset=" + charset2.toLowerCase();
      }
      return mime;
    }
    function extension2(type) {
      if (!type || typeof type !== "string") {
        return false;
      }
      var match = EXTRACT_TYPE_REGEXP.exec(type);
      var exts = match && exports.extensions[match[1].toLowerCase()];
      if (!exts || !exts.length) {
        return false;
      }
      return exts[0];
    }
    function lookup2(path) {
      if (!path || typeof path !== "string") {
        return false;
      }
      var extension3 = extname("x." + path).toLowerCase().substr(1);
      if (!extension3) {
        return false;
      }
      return exports.types[extension3] || false;
    }
    function populateMaps(extensions, types) {
      var preference = ["nginx", "apache", void 0, "iana"];
      Object.keys(db).forEach(function forEachMimeType(type) {
        var mime = db[type];
        var exts = mime.extensions;
        if (!exts || !exts.length) {
          return;
        }
        extensions[type] = exts;
        for (var i = 0; i < exts.length; i++) {
          var extension3 = exts[i];
          if (types[extension3]) {
            var from = preference.indexOf(db[types[extension3]].source);
            var to = preference.indexOf(mime.source);
            if (types[extension3] !== "application/octet-stream" && (from > to || from === to && types[extension3].substr(0, 12) === "application/")) {
              continue;
            }
          }
          types[extension3] = type;
        }
      });
    }
  }
});

// node_modules/asynckit/lib/defer.js
var require_defer = __commonJS({
  "node_modules/asynckit/lib/defer.js"(exports, module) {
    module.exports = defer;
    function defer(fn) {
      var nextTick = typeof setImmediate == "function" ? setImmediate : typeof process == "object" && typeof process.nextTick == "function" ? process.nextTick : null;
      if (nextTick) {
        nextTick(fn);
      } else {
        setTimeout(fn, 0);
      }
    }
  }
});

// node_modules/asynckit/lib/async.js
var require_async = __commonJS({
  "node_modules/asynckit/lib/async.js"(exports, module) {
    var defer = require_defer();
    module.exports = async;
    function async(callback) {
      var isAsync = false;
      defer(function() {
        isAsync = true;
      });
      return function async_callback(err, result) {
        if (isAsync) {
          callback(err, result);
        } else {
          defer(function nextTick_callback() {
            callback(err, result);
          });
        }
      };
    }
  }
});

// node_modules/asynckit/lib/abort.js
var require_abort = __commonJS({
  "node_modules/asynckit/lib/abort.js"(exports, module) {
    module.exports = abort;
    function abort(state) {
      Object.keys(state.jobs).forEach(clean.bind(state));
      state.jobs = {};
    }
    function clean(key) {
      if (typeof this.jobs[key] == "function") {
        this.jobs[key]();
      }
    }
  }
});

// node_modules/asynckit/lib/iterate.js
var require_iterate = __commonJS({
  "node_modules/asynckit/lib/iterate.js"(exports, module) {
    var async = require_async();
    var abort = require_abort();
    module.exports = iterate;
    function iterate(list, iterator2, state, callback) {
      var key = state["keyedList"] ? state["keyedList"][state.index] : state.index;
      state.jobs[key] = runJob(iterator2, key, list[key], function(error, output) {
        if (!(key in state.jobs)) {
          return;
        }
        delete state.jobs[key];
        if (error) {
          abort(state);
        } else {
          state.results[key] = output;
        }
        callback(error, state.results);
      });
    }
    function runJob(iterator2, key, item, callback) {
      var aborter;
      if (iterator2.length == 2) {
        aborter = iterator2(item, async(callback));
      } else {
        aborter = iterator2(item, key, async(callback));
      }
      return aborter;
    }
  }
});

// node_modules/asynckit/lib/state.js
var require_state = __commonJS({
  "node_modules/asynckit/lib/state.js"(exports, module) {
    module.exports = state;
    function state(list, sortMethod) {
      var isNamedList = !Array.isArray(list), initState = {
        index: 0,
        keyedList: isNamedList || sortMethod ? Object.keys(list) : null,
        jobs: {},
        results: isNamedList ? {} : [],
        size: isNamedList ? Object.keys(list).length : list.length
      };
      if (sortMethod) {
        initState.keyedList.sort(isNamedList ? sortMethod : function(a, b) {
          return sortMethod(list[a], list[b]);
        });
      }
      return initState;
    }
  }
});

// node_modules/asynckit/lib/terminator.js
var require_terminator = __commonJS({
  "node_modules/asynckit/lib/terminator.js"(exports, module) {
    var abort = require_abort();
    var async = require_async();
    module.exports = terminator;
    function terminator(callback) {
      if (!Object.keys(this.jobs).length) {
        return;
      }
      this.index = this.size;
      abort(this);
      async(callback)(null, this.results);
    }
  }
});

// node_modules/asynckit/parallel.js
var require_parallel = __commonJS({
  "node_modules/asynckit/parallel.js"(exports, module) {
    var iterate = require_iterate();
    var initState = require_state();
    var terminator = require_terminator();
    module.exports = parallel;
    function parallel(list, iterator2, callback) {
      var state = initState(list);
      while (state.index < (state["keyedList"] || list).length) {
        iterate(list, iterator2, state, function(error, result) {
          if (error) {
            callback(error, result);
            return;
          }
          if (Object.keys(state.jobs).length === 0) {
            callback(null, state.results);
            return;
          }
        });
        state.index++;
      }
      return terminator.bind(state, callback);
    }
  }
});

// node_modules/asynckit/serialOrdered.js
var require_serialOrdered = __commonJS({
  "node_modules/asynckit/serialOrdered.js"(exports, module) {
    var iterate = require_iterate();
    var initState = require_state();
    var terminator = require_terminator();
    module.exports = serialOrdered;
    module.exports.ascending = ascending;
    module.exports.descending = descending;
    function serialOrdered(list, iterator2, sortMethod, callback) {
      var state = initState(list, sortMethod);
      iterate(list, iterator2, state, function iteratorHandler(error, result) {
        if (error) {
          callback(error, result);
          return;
        }
        state.index++;
        if (state.index < (state["keyedList"] || list).length) {
          iterate(list, iterator2, state, iteratorHandler);
          return;
        }
        callback(null, state.results);
      });
      return terminator.bind(state, callback);
    }
    function ascending(a, b) {
      return a < b ? -1 : a > b ? 1 : 0;
    }
    function descending(a, b) {
      return -1 * ascending(a, b);
    }
  }
});

// node_modules/asynckit/serial.js
var require_serial = __commonJS({
  "node_modules/asynckit/serial.js"(exports, module) {
    var serialOrdered = require_serialOrdered();
    module.exports = serial;
    function serial(list, iterator2, callback) {
      return serialOrdered(list, iterator2, null, callback);
    }
  }
});

// node_modules/asynckit/index.js
var require_asynckit = __commonJS({
  "node_modules/asynckit/index.js"(exports, module) {
    module.exports = {
      parallel: require_parallel(),
      serial: require_serial(),
      serialOrdered: require_serialOrdered()
    };
  }
});

// node_modules/es-object-atoms/index.js
var require_es_object_atoms = __commonJS({
  "node_modules/es-object-atoms/index.js"(exports, module) {
    "use strict";
    module.exports = Object;
  }
});

// node_modules/es-errors/index.js
var require_es_errors = __commonJS({
  "node_modules/es-errors/index.js"(exports, module) {
    "use strict";
    module.exports = Error;
  }
});

// node_modules/es-errors/eval.js
var require_eval = __commonJS({
  "node_modules/es-errors/eval.js"(exports, module) {
    "use strict";
    module.exports = EvalError;
  }
});

// node_modules/es-errors/range.js
var require_range = __commonJS({
  "node_modules/es-errors/range.js"(exports, module) {
    "use strict";
    module.exports = RangeError;
  }
});

// node_modules/es-errors/ref.js
var require_ref = __commonJS({
  "node_modules/es-errors/ref.js"(exports, module) {
    "use strict";
    module.exports = ReferenceError;
  }
});

// node_modules/es-errors/syntax.js
var require_syntax = __commonJS({
  "node_modules/es-errors/syntax.js"(exports, module) {
    "use strict";
    module.exports = SyntaxError;
  }
});

// node_modules/es-errors/type.js
var require_type = __commonJS({
  "node_modules/es-errors/type.js"(exports, module) {
    "use strict";
    module.exports = TypeError;
  }
});

// node_modules/es-errors/uri.js
var require_uri = __commonJS({
  "node_modules/es-errors/uri.js"(exports, module) {
    "use strict";
    module.exports = URIError;
  }
});

// node_modules/math-intrinsics/abs.js
var require_abs = __commonJS({
  "node_modules/math-intrinsics/abs.js"(exports, module) {
    "use strict";
    module.exports = Math.abs;
  }
});

// node_modules/math-intrinsics/floor.js
var require_floor = __commonJS({
  "node_modules/math-intrinsics/floor.js"(exports, module) {
    "use strict";
    module.exports = Math.floor;
  }
});

// node_modules/math-intrinsics/max.js
var require_max = __commonJS({
  "node_modules/math-intrinsics/max.js"(exports, module) {
    "use strict";
    module.exports = Math.max;
  }
});

// node_modules/math-intrinsics/min.js
var require_min = __commonJS({
  "node_modules/math-intrinsics/min.js"(exports, module) {
    "use strict";
    module.exports = Math.min;
  }
});

// node_modules/math-intrinsics/pow.js
var require_pow = __commonJS({
  "node_modules/math-intrinsics/pow.js"(exports, module) {
    "use strict";
    module.exports = Math.pow;
  }
});

// node_modules/math-intrinsics/round.js
var require_round = __commonJS({
  "node_modules/math-intrinsics/round.js"(exports, module) {
    "use strict";
    module.exports = Math.round;
  }
});

// node_modules/math-intrinsics/isNaN.js
var require_isNaN = __commonJS({
  "node_modules/math-intrinsics/isNaN.js"(exports, module) {
    "use strict";
    module.exports = Number.isNaN || function isNaN2(a) {
      return a !== a;
    };
  }
});

// node_modules/math-intrinsics/sign.js
var require_sign = __commonJS({
  "node_modules/math-intrinsics/sign.js"(exports, module) {
    "use strict";
    var $isNaN = require_isNaN();
    module.exports = function sign(number) {
      if ($isNaN(number) || number === 0) {
        return number;
      }
      return number < 0 ? -1 : 1;
    };
  }
});

// node_modules/gopd/gOPD.js
var require_gOPD = __commonJS({
  "node_modules/gopd/gOPD.js"(exports, module) {
    "use strict";
    module.exports = Object.getOwnPropertyDescriptor;
  }
});

// node_modules/gopd/index.js
var require_gopd = __commonJS({
  "node_modules/gopd/index.js"(exports, module) {
    "use strict";
    var $gOPD = require_gOPD();
    if ($gOPD) {
      try {
        $gOPD([], "length");
      } catch (e) {
        $gOPD = null;
      }
    }
    module.exports = $gOPD;
  }
});

// node_modules/es-define-property/index.js
var require_es_define_property = __commonJS({
  "node_modules/es-define-property/index.js"(exports, module) {
    "use strict";
    var $defineProperty = Object.defineProperty || false;
    if ($defineProperty) {
      try {
        $defineProperty({}, "a", { value: 1 });
      } catch (e) {
        $defineProperty = false;
      }
    }
    module.exports = $defineProperty;
  }
});

// node_modules/has-symbols/shams.js
var require_shams = __commonJS({
  "node_modules/has-symbols/shams.js"(exports, module) {
    "use strict";
    module.exports = function hasSymbols() {
      if (typeof Symbol !== "function" || typeof Object.getOwnPropertySymbols !== "function") {
        return false;
      }
      if (typeof Symbol.iterator === "symbol") {
        return true;
      }
      var obj = {};
      var sym = /* @__PURE__ */ Symbol("test");
      var symObj = Object(sym);
      if (typeof sym === "string") {
        return false;
      }
      if (Object.prototype.toString.call(sym) !== "[object Symbol]") {
        return false;
      }
      if (Object.prototype.toString.call(symObj) !== "[object Symbol]") {
        return false;
      }
      var symVal = 42;
      obj[sym] = symVal;
      for (var _ in obj) {
        return false;
      }
      if (typeof Object.keys === "function" && Object.keys(obj).length !== 0) {
        return false;
      }
      if (typeof Object.getOwnPropertyNames === "function" && Object.getOwnPropertyNames(obj).length !== 0) {
        return false;
      }
      var syms = Object.getOwnPropertySymbols(obj);
      if (syms.length !== 1 || syms[0] !== sym) {
        return false;
      }
      if (!Object.prototype.propertyIsEnumerable.call(obj, sym)) {
        return false;
      }
      if (typeof Object.getOwnPropertyDescriptor === "function") {
        var descriptor = (
          /** @type {PropertyDescriptor} */
          Object.getOwnPropertyDescriptor(obj, sym)
        );
        if (descriptor.value !== symVal || descriptor.enumerable !== true) {
          return false;
        }
      }
      return true;
    };
  }
});

// node_modules/has-symbols/index.js
var require_has_symbols = __commonJS({
  "node_modules/has-symbols/index.js"(exports, module) {
    "use strict";
    var origSymbol = typeof Symbol !== "undefined" && Symbol;
    var hasSymbolSham = require_shams();
    module.exports = function hasNativeSymbols() {
      if (typeof origSymbol !== "function") {
        return false;
      }
      if (typeof Symbol !== "function") {
        return false;
      }
      if (typeof origSymbol("foo") !== "symbol") {
        return false;
      }
      if (typeof /* @__PURE__ */ Symbol("bar") !== "symbol") {
        return false;
      }
      return hasSymbolSham();
    };
  }
});

// node_modules/get-proto/Reflect.getPrototypeOf.js
var require_Reflect_getPrototypeOf = __commonJS({
  "node_modules/get-proto/Reflect.getPrototypeOf.js"(exports, module) {
    "use strict";
    module.exports = typeof Reflect !== "undefined" && Reflect.getPrototypeOf || null;
  }
});

// node_modules/get-proto/Object.getPrototypeOf.js
var require_Object_getPrototypeOf = __commonJS({
  "node_modules/get-proto/Object.getPrototypeOf.js"(exports, module) {
    "use strict";
    var $Object = require_es_object_atoms();
    module.exports = $Object.getPrototypeOf || null;
  }
});

// node_modules/function-bind/implementation.js
var require_implementation = __commonJS({
  "node_modules/function-bind/implementation.js"(exports, module) {
    "use strict";
    var ERROR_MESSAGE = "Function.prototype.bind called on incompatible ";
    var toStr = Object.prototype.toString;
    var max = Math.max;
    var funcType = "[object Function]";
    var concatty = function concatty2(a, b) {
      var arr = [];
      for (var i = 0; i < a.length; i += 1) {
        arr[i] = a[i];
      }
      for (var j = 0; j < b.length; j += 1) {
        arr[j + a.length] = b[j];
      }
      return arr;
    };
    var slicy = function slicy2(arrLike, offset) {
      var arr = [];
      for (var i = offset || 0, j = 0; i < arrLike.length; i += 1, j += 1) {
        arr[j] = arrLike[i];
      }
      return arr;
    };
    var joiny = function(arr, joiner) {
      var str = "";
      for (var i = 0; i < arr.length; i += 1) {
        str += arr[i];
        if (i + 1 < arr.length) {
          str += joiner;
        }
      }
      return str;
    };
    module.exports = function bind2(that) {
      var target = this;
      if (typeof target !== "function" || toStr.apply(target) !== funcType) {
        throw new TypeError(ERROR_MESSAGE + target);
      }
      var args = slicy(arguments, 1);
      var bound;
      var binder = function() {
        if (this instanceof bound) {
          var result = target.apply(
            this,
            concatty(args, arguments)
          );
          if (Object(result) === result) {
            return result;
          }
          return this;
        }
        return target.apply(
          that,
          concatty(args, arguments)
        );
      };
      var boundLength = max(0, target.length - args.length);
      var boundArgs = [];
      for (var i = 0; i < boundLength; i++) {
        boundArgs[i] = "$" + i;
      }
      bound = Function("binder", "return function (" + joiny(boundArgs, ",") + "){ return binder.apply(this,arguments); }")(binder);
      if (target.prototype) {
        var Empty = function Empty2() {
        };
        Empty.prototype = target.prototype;
        bound.prototype = new Empty();
        Empty.prototype = null;
      }
      return bound;
    };
  }
});

// node_modules/function-bind/index.js
var require_function_bind = __commonJS({
  "node_modules/function-bind/index.js"(exports, module) {
    "use strict";
    var implementation = require_implementation();
    module.exports = Function.prototype.bind || implementation;
  }
});

// node_modules/call-bind-apply-helpers/functionCall.js
var require_functionCall = __commonJS({
  "node_modules/call-bind-apply-helpers/functionCall.js"(exports, module) {
    "use strict";
    module.exports = Function.prototype.call;
  }
});

// node_modules/call-bind-apply-helpers/functionApply.js
var require_functionApply = __commonJS({
  "node_modules/call-bind-apply-helpers/functionApply.js"(exports, module) {
    "use strict";
    module.exports = Function.prototype.apply;
  }
});

// node_modules/call-bind-apply-helpers/reflectApply.js
var require_reflectApply = __commonJS({
  "node_modules/call-bind-apply-helpers/reflectApply.js"(exports, module) {
    "use strict";
    module.exports = typeof Reflect !== "undefined" && Reflect && Reflect.apply;
  }
});

// node_modules/call-bind-apply-helpers/actualApply.js
var require_actualApply = __commonJS({
  "node_modules/call-bind-apply-helpers/actualApply.js"(exports, module) {
    "use strict";
    var bind2 = require_function_bind();
    var $apply = require_functionApply();
    var $call = require_functionCall();
    var $reflectApply = require_reflectApply();
    module.exports = $reflectApply || bind2.call($call, $apply);
  }
});

// node_modules/call-bind-apply-helpers/index.js
var require_call_bind_apply_helpers = __commonJS({
  "node_modules/call-bind-apply-helpers/index.js"(exports, module) {
    "use strict";
    var bind2 = require_function_bind();
    var $TypeError = require_type();
    var $call = require_functionCall();
    var $actualApply = require_actualApply();
    module.exports = function callBindBasic(args) {
      if (args.length < 1 || typeof args[0] !== "function") {
        throw new $TypeError("a function is required");
      }
      return $actualApply(bind2, $call, args);
    };
  }
});

// node_modules/dunder-proto/get.js
var require_get = __commonJS({
  "node_modules/dunder-proto/get.js"(exports, module) {
    "use strict";
    var callBind = require_call_bind_apply_helpers();
    var gOPD = require_gopd();
    var hasProtoAccessor;
    try {
      hasProtoAccessor = /** @type {{ __proto__?: typeof Array.prototype }} */
      [].__proto__ === Array.prototype;
    } catch (e) {
      if (!e || typeof e !== "object" || !("code" in e) || e.code !== "ERR_PROTO_ACCESS") {
        throw e;
      }
    }
    var desc = !!hasProtoAccessor && gOPD && gOPD(
      Object.prototype,
      /** @type {keyof typeof Object.prototype} */
      "__proto__"
    );
    var $Object = Object;
    var $getPrototypeOf = $Object.getPrototypeOf;
    module.exports = desc && typeof desc.get === "function" ? callBind([desc.get]) : typeof $getPrototypeOf === "function" ? (
      /** @type {import('./get')} */
      function getDunder(value) {
        return $getPrototypeOf(value == null ? value : $Object(value));
      }
    ) : false;
  }
});

// node_modules/get-proto/index.js
var require_get_proto = __commonJS({
  "node_modules/get-proto/index.js"(exports, module) {
    "use strict";
    var reflectGetProto = require_Reflect_getPrototypeOf();
    var originalGetProto = require_Object_getPrototypeOf();
    var getDunderProto = require_get();
    module.exports = reflectGetProto ? function getProto(O) {
      return reflectGetProto(O);
    } : originalGetProto ? function getProto(O) {
      if (!O || typeof O !== "object" && typeof O !== "function") {
        throw new TypeError("getProto: not an object");
      }
      return originalGetProto(O);
    } : getDunderProto ? function getProto(O) {
      return getDunderProto(O);
    } : null;
  }
});

// node_modules/hasown/index.js
var require_hasown = __commonJS({
  "node_modules/hasown/index.js"(exports, module) {
    "use strict";
    var call = Function.prototype.call;
    var $hasOwn = Object.prototype.hasOwnProperty;
    var bind2 = require_function_bind();
    module.exports = bind2.call(call, $hasOwn);
  }
});

// node_modules/get-intrinsic/index.js
var require_get_intrinsic = __commonJS({
  "node_modules/get-intrinsic/index.js"(exports, module) {
    "use strict";
    var undefined2;
    var $Object = require_es_object_atoms();
    var $Error = require_es_errors();
    var $EvalError = require_eval();
    var $RangeError = require_range();
    var $ReferenceError = require_ref();
    var $SyntaxError = require_syntax();
    var $TypeError = require_type();
    var $URIError = require_uri();
    var abs = require_abs();
    var floor = require_floor();
    var max = require_max();
    var min = require_min();
    var pow = require_pow();
    var round = require_round();
    var sign = require_sign();
    var $Function = Function;
    var getEvalledConstructor = function(expressionSyntax) {
      try {
        return $Function('"use strict"; return (' + expressionSyntax + ").constructor;")();
      } catch (e) {
      }
    };
    var $gOPD = require_gopd();
    var $defineProperty = require_es_define_property();
    var throwTypeError = function() {
      throw new $TypeError();
    };
    var ThrowTypeError = $gOPD ? (function() {
      try {
        arguments.callee;
        return throwTypeError;
      } catch (calleeThrows) {
        try {
          return $gOPD(arguments, "callee").get;
        } catch (gOPDthrows) {
          return throwTypeError;
        }
      }
    })() : throwTypeError;
    var hasSymbols = require_has_symbols()();
    var getProto = require_get_proto();
    var $ObjectGPO = require_Object_getPrototypeOf();
    var $ReflectGPO = require_Reflect_getPrototypeOf();
    var $apply = require_functionApply();
    var $call = require_functionCall();
    var needsEval = {};
    var TypedArray = typeof Uint8Array === "undefined" || !getProto ? undefined2 : getProto(Uint8Array);
    var INTRINSICS = {
      __proto__: null,
      "%AggregateError%": typeof AggregateError === "undefined" ? undefined2 : AggregateError,
      "%Array%": Array,
      "%ArrayBuffer%": typeof ArrayBuffer === "undefined" ? undefined2 : ArrayBuffer,
      "%ArrayIteratorPrototype%": hasSymbols && getProto ? getProto([][Symbol.iterator]()) : undefined2,
      "%AsyncFromSyncIteratorPrototype%": undefined2,
      "%AsyncFunction%": needsEval,
      "%AsyncGenerator%": needsEval,
      "%AsyncGeneratorFunction%": needsEval,
      "%AsyncIteratorPrototype%": needsEval,
      "%Atomics%": typeof Atomics === "undefined" ? undefined2 : Atomics,
      "%BigInt%": typeof BigInt === "undefined" ? undefined2 : BigInt,
      "%BigInt64Array%": typeof BigInt64Array === "undefined" ? undefined2 : BigInt64Array,
      "%BigUint64Array%": typeof BigUint64Array === "undefined" ? undefined2 : BigUint64Array,
      "%Boolean%": Boolean,
      "%DataView%": typeof DataView === "undefined" ? undefined2 : DataView,
      "%Date%": Date,
      "%decodeURI%": decodeURI,
      "%decodeURIComponent%": decodeURIComponent,
      "%encodeURI%": encodeURI,
      "%encodeURIComponent%": encodeURIComponent,
      "%Error%": $Error,
      "%eval%": eval,
      // eslint-disable-line no-eval
      "%EvalError%": $EvalError,
      "%Float16Array%": typeof Float16Array === "undefined" ? undefined2 : Float16Array,
      "%Float32Array%": typeof Float32Array === "undefined" ? undefined2 : Float32Array,
      "%Float64Array%": typeof Float64Array === "undefined" ? undefined2 : Float64Array,
      "%FinalizationRegistry%": typeof FinalizationRegistry === "undefined" ? undefined2 : FinalizationRegistry,
      "%Function%": $Function,
      "%GeneratorFunction%": needsEval,
      "%Int8Array%": typeof Int8Array === "undefined" ? undefined2 : Int8Array,
      "%Int16Array%": typeof Int16Array === "undefined" ? undefined2 : Int16Array,
      "%Int32Array%": typeof Int32Array === "undefined" ? undefined2 : Int32Array,
      "%isFinite%": isFinite,
      "%isNaN%": isNaN,
      "%IteratorPrototype%": hasSymbols && getProto ? getProto(getProto([][Symbol.iterator]())) : undefined2,
      "%JSON%": typeof JSON === "object" ? JSON : undefined2,
      "%Map%": typeof Map === "undefined" ? undefined2 : Map,
      "%MapIteratorPrototype%": typeof Map === "undefined" || !hasSymbols || !getProto ? undefined2 : getProto((/* @__PURE__ */ new Map())[Symbol.iterator]()),
      "%Math%": Math,
      "%Number%": Number,
      "%Object%": $Object,
      "%Object.getOwnPropertyDescriptor%": $gOPD,
      "%parseFloat%": parseFloat,
      "%parseInt%": parseInt,
      "%Promise%": typeof Promise === "undefined" ? undefined2 : Promise,
      "%Proxy%": typeof Proxy === "undefined" ? undefined2 : Proxy,
      "%RangeError%": $RangeError,
      "%ReferenceError%": $ReferenceError,
      "%Reflect%": typeof Reflect === "undefined" ? undefined2 : Reflect,
      "%RegExp%": RegExp,
      "%Set%": typeof Set === "undefined" ? undefined2 : Set,
      "%SetIteratorPrototype%": typeof Set === "undefined" || !hasSymbols || !getProto ? undefined2 : getProto((/* @__PURE__ */ new Set())[Symbol.iterator]()),
      "%SharedArrayBuffer%": typeof SharedArrayBuffer === "undefined" ? undefined2 : SharedArrayBuffer,
      "%String%": String,
      "%StringIteratorPrototype%": hasSymbols && getProto ? getProto(""[Symbol.iterator]()) : undefined2,
      "%Symbol%": hasSymbols ? Symbol : undefined2,
      "%SyntaxError%": $SyntaxError,
      "%ThrowTypeError%": ThrowTypeError,
      "%TypedArray%": TypedArray,
      "%TypeError%": $TypeError,
      "%Uint8Array%": typeof Uint8Array === "undefined" ? undefined2 : Uint8Array,
      "%Uint8ClampedArray%": typeof Uint8ClampedArray === "undefined" ? undefined2 : Uint8ClampedArray,
      "%Uint16Array%": typeof Uint16Array === "undefined" ? undefined2 : Uint16Array,
      "%Uint32Array%": typeof Uint32Array === "undefined" ? undefined2 : Uint32Array,
      "%URIError%": $URIError,
      "%WeakMap%": typeof WeakMap === "undefined" ? undefined2 : WeakMap,
      "%WeakRef%": typeof WeakRef === "undefined" ? undefined2 : WeakRef,
      "%WeakSet%": typeof WeakSet === "undefined" ? undefined2 : WeakSet,
      "%Function.prototype.call%": $call,
      "%Function.prototype.apply%": $apply,
      "%Object.defineProperty%": $defineProperty,
      "%Object.getPrototypeOf%": $ObjectGPO,
      "%Math.abs%": abs,
      "%Math.floor%": floor,
      "%Math.max%": max,
      "%Math.min%": min,
      "%Math.pow%": pow,
      "%Math.round%": round,
      "%Math.sign%": sign,
      "%Reflect.getPrototypeOf%": $ReflectGPO
    };
    if (getProto) {
      try {
        null.error;
      } catch (e) {
        errorProto = getProto(getProto(e));
        INTRINSICS["%Error.prototype%"] = errorProto;
      }
    }
    var errorProto;
    var doEval = function doEval2(name) {
      var value;
      if (name === "%AsyncFunction%") {
        value = getEvalledConstructor("async function () {}");
      } else if (name === "%GeneratorFunction%") {
        value = getEvalledConstructor("function* () {}");
      } else if (name === "%AsyncGeneratorFunction%") {
        value = getEvalledConstructor("async function* () {}");
      } else if (name === "%AsyncGenerator%") {
        var fn = doEval2("%AsyncGeneratorFunction%");
        if (fn) {
          value = fn.prototype;
        }
      } else if (name === "%AsyncIteratorPrototype%") {
        var gen = doEval2("%AsyncGenerator%");
        if (gen && getProto) {
          value = getProto(gen.prototype);
        }
      }
      INTRINSICS[name] = value;
      return value;
    };
    var LEGACY_ALIASES = {
      __proto__: null,
      "%ArrayBufferPrototype%": ["ArrayBuffer", "prototype"],
      "%ArrayPrototype%": ["Array", "prototype"],
      "%ArrayProto_entries%": ["Array", "prototype", "entries"],
      "%ArrayProto_forEach%": ["Array", "prototype", "forEach"],
      "%ArrayProto_keys%": ["Array", "prototype", "keys"],
      "%ArrayProto_values%": ["Array", "prototype", "values"],
      "%AsyncFunctionPrototype%": ["AsyncFunction", "prototype"],
      "%AsyncGenerator%": ["AsyncGeneratorFunction", "prototype"],
      "%AsyncGeneratorPrototype%": ["AsyncGeneratorFunction", "prototype", "prototype"],
      "%BooleanPrototype%": ["Boolean", "prototype"],
      "%DataViewPrototype%": ["DataView", "prototype"],
      "%DatePrototype%": ["Date", "prototype"],
      "%ErrorPrototype%": ["Error", "prototype"],
      "%EvalErrorPrototype%": ["EvalError", "prototype"],
      "%Float32ArrayPrototype%": ["Float32Array", "prototype"],
      "%Float64ArrayPrototype%": ["Float64Array", "prototype"],
      "%FunctionPrototype%": ["Function", "prototype"],
      "%Generator%": ["GeneratorFunction", "prototype"],
      "%GeneratorPrototype%": ["GeneratorFunction", "prototype", "prototype"],
      "%Int8ArrayPrototype%": ["Int8Array", "prototype"],
      "%Int16ArrayPrototype%": ["Int16Array", "prototype"],
      "%Int32ArrayPrototype%": ["Int32Array", "prototype"],
      "%JSONParse%": ["JSON", "parse"],
      "%JSONStringify%": ["JSON", "stringify"],
      "%MapPrototype%": ["Map", "prototype"],
      "%NumberPrototype%": ["Number", "prototype"],
      "%ObjectPrototype%": ["Object", "prototype"],
      "%ObjProto_toString%": ["Object", "prototype", "toString"],
      "%ObjProto_valueOf%": ["Object", "prototype", "valueOf"],
      "%PromisePrototype%": ["Promise", "prototype"],
      "%PromiseProto_then%": ["Promise", "prototype", "then"],
      "%Promise_all%": ["Promise", "all"],
      "%Promise_reject%": ["Promise", "reject"],
      "%Promise_resolve%": ["Promise", "resolve"],
      "%RangeErrorPrototype%": ["RangeError", "prototype"],
      "%ReferenceErrorPrototype%": ["ReferenceError", "prototype"],
      "%RegExpPrototype%": ["RegExp", "prototype"],
      "%SetPrototype%": ["Set", "prototype"],
      "%SharedArrayBufferPrototype%": ["SharedArrayBuffer", "prototype"],
      "%StringPrototype%": ["String", "prototype"],
      "%SymbolPrototype%": ["Symbol", "prototype"],
      "%SyntaxErrorPrototype%": ["SyntaxError", "prototype"],
      "%TypedArrayPrototype%": ["TypedArray", "prototype"],
      "%TypeErrorPrototype%": ["TypeError", "prototype"],
      "%Uint8ArrayPrototype%": ["Uint8Array", "prototype"],
      "%Uint8ClampedArrayPrototype%": ["Uint8ClampedArray", "prototype"],
      "%Uint16ArrayPrototype%": ["Uint16Array", "prototype"],
      "%Uint32ArrayPrototype%": ["Uint32Array", "prototype"],
      "%URIErrorPrototype%": ["URIError", "prototype"],
      "%WeakMapPrototype%": ["WeakMap", "prototype"],
      "%WeakSetPrototype%": ["WeakSet", "prototype"]
    };
    var bind2 = require_function_bind();
    var hasOwn = require_hasown();
    var $concat = bind2.call($call, Array.prototype.concat);
    var $spliceApply = bind2.call($apply, Array.prototype.splice);
    var $replace = bind2.call($call, String.prototype.replace);
    var $strSlice = bind2.call($call, String.prototype.slice);
    var $exec = bind2.call($call, RegExp.prototype.exec);
    var rePropName = /[^%.[\]]+|\[(?:(-?\d+(?:\.\d+)?)|(["'])((?:(?!\2)[^\\]|\\.)*?)\2)\]|(?=(?:\.|\[\])(?:\.|\[\]|%$))/g;
    var reEscapeChar = /\\(\\)?/g;
    var stringToPath = function stringToPath2(string) {
      var first = $strSlice(string, 0, 1);
      var last = $strSlice(string, -1);
      if (first === "%" && last !== "%") {
        throw new $SyntaxError("invalid intrinsic syntax, expected closing `%`");
      } else if (last === "%" && first !== "%") {
        throw new $SyntaxError("invalid intrinsic syntax, expected opening `%`");
      }
      var result = [];
      $replace(string, rePropName, function(match, number, quote, subString) {
        result[result.length] = quote ? $replace(subString, reEscapeChar, "$1") : number || match;
      });
      return result;
    };
    var getBaseIntrinsic = function getBaseIntrinsic2(name, allowMissing) {
      var intrinsicName = name;
      var alias;
      if (hasOwn(LEGACY_ALIASES, intrinsicName)) {
        alias = LEGACY_ALIASES[intrinsicName];
        intrinsicName = "%" + alias[0] + "%";
      }
      if (hasOwn(INTRINSICS, intrinsicName)) {
        var value = INTRINSICS[intrinsicName];
        if (value === needsEval) {
          value = doEval(intrinsicName);
        }
        if (typeof value === "undefined" && !allowMissing) {
          throw new $TypeError("intrinsic " + name + " exists, but is not available. Please file an issue!");
        }
        return {
          alias,
          name: intrinsicName,
          value
        };
      }
      throw new $SyntaxError("intrinsic " + name + " does not exist!");
    };
    module.exports = function GetIntrinsic(name, allowMissing) {
      if (typeof name !== "string" || name.length === 0) {
        throw new $TypeError("intrinsic name must be a non-empty string");
      }
      if (arguments.length > 1 && typeof allowMissing !== "boolean") {
        throw new $TypeError('"allowMissing" argument must be a boolean');
      }
      if ($exec(/^%?[^%]*%?$/, name) === null) {
        throw new $SyntaxError("`%` may not be present anywhere but at the beginning and end of the intrinsic name");
      }
      var parts = stringToPath(name);
      var intrinsicBaseName = parts.length > 0 ? parts[0] : "";
      var intrinsic = getBaseIntrinsic("%" + intrinsicBaseName + "%", allowMissing);
      var intrinsicRealName = intrinsic.name;
      var value = intrinsic.value;
      var skipFurtherCaching = false;
      var alias = intrinsic.alias;
      if (alias) {
        intrinsicBaseName = alias[0];
        $spliceApply(parts, $concat([0, 1], alias));
      }
      for (var i = 1, isOwn = true; i < parts.length; i += 1) {
        var part = parts[i];
        var first = $strSlice(part, 0, 1);
        var last = $strSlice(part, -1);
        if ((first === '"' || first === "'" || first === "`" || (last === '"' || last === "'" || last === "`")) && first !== last) {
          throw new $SyntaxError("property names with quotes must have matching quotes");
        }
        if (part === "constructor" || !isOwn) {
          skipFurtherCaching = true;
        }
        intrinsicBaseName += "." + part;
        intrinsicRealName = "%" + intrinsicBaseName + "%";
        if (hasOwn(INTRINSICS, intrinsicRealName)) {
          value = INTRINSICS[intrinsicRealName];
        } else if (value != null) {
          if (!(part in value)) {
            if (!allowMissing) {
              throw new $TypeError("base intrinsic for " + name + " exists, but the property is not available.");
            }
            return void undefined2;
          }
          if ($gOPD && i + 1 >= parts.length) {
            var desc = $gOPD(value, part);
            isOwn = !!desc;
            if (isOwn && "get" in desc && !("originalValue" in desc.get)) {
              value = desc.get;
            } else {
              value = value[part];
            }
          } else {
            isOwn = hasOwn(value, part);
            value = value[part];
          }
          if (isOwn && !skipFurtherCaching) {
            INTRINSICS[intrinsicRealName] = value;
          }
        }
      }
      return value;
    };
  }
});

// node_modules/has-tostringtag/shams.js
var require_shams2 = __commonJS({
  "node_modules/has-tostringtag/shams.js"(exports, module) {
    "use strict";
    var hasSymbols = require_shams();
    module.exports = function hasToStringTagShams() {
      return hasSymbols() && !!Symbol.toStringTag;
    };
  }
});

// node_modules/es-set-tostringtag/index.js
var require_es_set_tostringtag = __commonJS({
  "node_modules/es-set-tostringtag/index.js"(exports, module) {
    "use strict";
    var GetIntrinsic = require_get_intrinsic();
    var $defineProperty = GetIntrinsic("%Object.defineProperty%", true);
    var hasToStringTag = require_shams2()();
    var hasOwn = require_hasown();
    var $TypeError = require_type();
    var toStringTag2 = hasToStringTag ? Symbol.toStringTag : null;
    module.exports = function setToStringTag(object, value) {
      var overrideIfSet = arguments.length > 2 && !!arguments[2] && arguments[2].force;
      var nonConfigurable = arguments.length > 2 && !!arguments[2] && arguments[2].nonConfigurable;
      if (typeof overrideIfSet !== "undefined" && typeof overrideIfSet !== "boolean" || typeof nonConfigurable !== "undefined" && typeof nonConfigurable !== "boolean") {
        throw new $TypeError("if provided, the `overrideIfSet` and `nonConfigurable` options must be booleans");
      }
      if (toStringTag2 && (overrideIfSet || !hasOwn(object, toStringTag2))) {
        if ($defineProperty) {
          $defineProperty(object, toStringTag2, {
            configurable: !nonConfigurable,
            enumerable: false,
            value,
            writable: false
          });
        } else {
          object[toStringTag2] = value;
        }
      }
    };
  }
});

// node_modules/form-data/lib/populate.js
var require_populate = __commonJS({
  "node_modules/form-data/lib/populate.js"(exports, module) {
    "use strict";
    module.exports = function(dst, src) {
      Object.keys(src).forEach(function(prop) {
        dst[prop] = dst[prop] || src[prop];
      });
      return dst;
    };
  }
});

// node_modules/form-data/lib/form_data.js
var require_form_data = __commonJS({
  "node_modules/form-data/lib/form_data.js"(exports, module) {
    "use strict";
    var CombinedStream = require_combined_stream();
    var util4 = __require("util");
    var path = __require("path");
    var http3 = __require("http");
    var https2 = __require("https");
    var parseUrl2 = __require("url").parse;
    var fs = __require("fs");
    var Stream = __require("stream").Stream;
    var crypto2 = __require("crypto");
    var mime = require_mime_types();
    var asynckit = require_asynckit();
    var setToStringTag = require_es_set_tostringtag();
    var hasOwn = require_hasown();
    var populate = require_populate();
    function escapeHeaderParam(str) {
      return String(str).replace(/\r/g, "%0D").replace(/\n/g, "%0A").replace(/"/g, "%22");
    }
    function FormData3(options) {
      if (!(this instanceof FormData3)) {
        return new FormData3(options);
      }
      this._overheadLength = 0;
      this._valueLength = 0;
      this._valuesToMeasure = [];
      CombinedStream.call(this);
      options = options || {};
      for (var option in options) {
        this[option] = options[option];
      }
    }
    util4.inherits(FormData3, CombinedStream);
    FormData3.LINE_BREAK = "\r\n";
    FormData3.DEFAULT_CONTENT_TYPE = "application/octet-stream";
    FormData3.prototype.append = function(field, value, options) {
      options = options || {};
      if (typeof options === "string") {
        options = { filename: options };
      }
      var append2 = CombinedStream.prototype.append.bind(this);
      if (typeof value === "number" || value == null) {
        value = String(value);
      }
      if (Array.isArray(value)) {
        this._error(new Error("Arrays are not supported."));
        return;
      }
      var header = this._multiPartHeader(field, value, options);
      var footer = this._multiPartFooter();
      append2(header);
      append2(value);
      append2(footer);
      this._trackLength(header, value, options);
    };
    FormData3.prototype._trackLength = function(header, value, options) {
      var valueLength = 0;
      if (options.knownLength != null) {
        valueLength += Number(options.knownLength);
      } else if (Buffer.isBuffer(value)) {
        valueLength = value.length;
      } else if (typeof value === "string") {
        valueLength = Buffer.byteLength(value);
      }
      this._valueLength += valueLength;
      this._overheadLength += Buffer.byteLength(header) + FormData3.LINE_BREAK.length;
      if (!value || !value.path && !(value.readable && hasOwn(value, "httpVersion")) && !(value instanceof Stream)) {
        return;
      }
      if (!options.knownLength) {
        this._valuesToMeasure.push(value);
      }
    };
    FormData3.prototype._lengthRetriever = function(value, callback) {
      if (hasOwn(value, "fd")) {
        if (value.end != void 0 && value.end != Infinity && value.start != void 0) {
          callback(null, value.end + 1 - (value.start ? value.start : 0));
        } else {
          fs.stat(value.path, function(err, stat) {
            if (err) {
              callback(err);
              return;
            }
            var fileSize = stat.size - (value.start ? value.start : 0);
            callback(null, fileSize);
          });
        }
      } else if (hasOwn(value, "httpVersion")) {
        callback(null, Number(value.headers["content-length"]));
      } else if (hasOwn(value, "httpModule")) {
        value.on("response", function(response) {
          value.pause();
          callback(null, Number(response.headers["content-length"]));
        });
        value.resume();
      } else {
        callback("Unknown stream");
      }
    };
    FormData3.prototype._multiPartHeader = function(field, value, options) {
      if (typeof options.header === "string") {
        return options.header;
      }
      var contentDisposition = this._getContentDisposition(value, options);
      var contentType = this._getContentType(value, options);
      var contents = "";
      var headers = {
        // add custom disposition as third element or keep it two elements if not
        "Content-Disposition": ["form-data", 'name="' + escapeHeaderParam(field) + '"'].concat(contentDisposition || []),
        // if no content type. allow it to be empty array
        "Content-Type": [].concat(contentType || [])
      };
      if (typeof options.header === "object") {
        populate(headers, options.header);
      }
      var header;
      for (var prop in headers) {
        if (hasOwn(headers, prop)) {
          header = headers[prop];
          if (header == null) {
            continue;
          }
          if (!Array.isArray(header)) {
            header = [header];
          }
          if (header.length) {
            contents += prop + ": " + header.join("; ") + FormData3.LINE_BREAK;
          }
        }
      }
      return "--" + this.getBoundary() + FormData3.LINE_BREAK + contents + FormData3.LINE_BREAK;
    };
    FormData3.prototype._getContentDisposition = function(value, options) {
      var filename;
      if (typeof options.filepath === "string") {
        filename = path.normalize(options.filepath).replace(/\\/g, "/");
      } else if (options.filename || value && (value.name || value.path)) {
        filename = path.basename(options.filename || value && (value.name || value.path));
      } else if (value && value.readable && hasOwn(value, "httpVersion")) {
        filename = path.basename(value.client._httpMessage.path || "");
      }
      if (filename) {
        return 'filename="' + escapeHeaderParam(filename) + '"';
      }
    };
    FormData3.prototype._getContentType = function(value, options) {
      var contentType = options.contentType;
      if (!contentType && value && value.name) {
        contentType = mime.lookup(value.name);
      }
      if (!contentType && value && value.path) {
        contentType = mime.lookup(value.path);
      }
      if (!contentType && value && value.readable && hasOwn(value, "httpVersion")) {
        contentType = value.headers["content-type"];
      }
      if (!contentType && (options.filepath || options.filename)) {
        contentType = mime.lookup(options.filepath || options.filename);
      }
      if (!contentType && value && typeof value === "object") {
        contentType = FormData3.DEFAULT_CONTENT_TYPE;
      }
      return contentType;
    };
    FormData3.prototype._multiPartFooter = function() {
      return function(next) {
        var footer = FormData3.LINE_BREAK;
        var lastPart = this._streams.length === 0;
        if (lastPart) {
          footer += this._lastBoundary();
        }
        next(footer);
      }.bind(this);
    };
    FormData3.prototype._lastBoundary = function() {
      return "--" + this.getBoundary() + "--" + FormData3.LINE_BREAK;
    };
    FormData3.prototype.getHeaders = function(userHeaders) {
      var header;
      var formHeaders = {
        "content-type": "multipart/form-data; boundary=" + this.getBoundary()
      };
      for (header in userHeaders) {
        if (hasOwn(userHeaders, header)) {
          formHeaders[header.toLowerCase()] = userHeaders[header];
        }
      }
      return formHeaders;
    };
    FormData3.prototype.setBoundary = function(boundary) {
      if (typeof boundary !== "string") {
        throw new TypeError("FormData boundary must be a string");
      }
      this._boundary = boundary;
    };
    FormData3.prototype.getBoundary = function() {
      if (!this._boundary) {
        this._generateBoundary();
      }
      return this._boundary;
    };
    FormData3.prototype.getBuffer = function() {
      var dataBuffer = new Buffer.alloc(0);
      var boundary = this.getBoundary();
      for (var i = 0, len = this._streams.length; i < len; i++) {
        if (typeof this._streams[i] !== "function") {
          if (Buffer.isBuffer(this._streams[i])) {
            dataBuffer = Buffer.concat([dataBuffer, this._streams[i]]);
          } else {
            dataBuffer = Buffer.concat([dataBuffer, Buffer.from(this._streams[i])]);
          }
          if (typeof this._streams[i] !== "string" || this._streams[i].substring(2, boundary.length + 2) !== boundary) {
            dataBuffer = Buffer.concat([dataBuffer, Buffer.from(FormData3.LINE_BREAK)]);
          }
        }
      }
      return Buffer.concat([dataBuffer, Buffer.from(this._lastBoundary())]);
    };
    FormData3.prototype._generateBoundary = function() {
      this._boundary = "--------------------------" + crypto2.randomBytes(12).toString("hex");
    };
    FormData3.prototype.getLengthSync = function() {
      var knownLength = this._overheadLength + this._valueLength;
      if (this._streams.length) {
        knownLength += this._lastBoundary().length;
      }
      if (!this.hasKnownLength()) {
        this._error(new Error("Cannot calculate proper length in synchronous way."));
      }
      return knownLength;
    };
    FormData3.prototype.hasKnownLength = function() {
      var hasKnownLength = true;
      if (this._valuesToMeasure.length) {
        hasKnownLength = false;
      }
      return hasKnownLength;
    };
    FormData3.prototype.getLength = function(cb) {
      var knownLength = this._overheadLength + this._valueLength;
      if (this._streams.length) {
        knownLength += this._lastBoundary().length;
      }
      if (!this._valuesToMeasure.length) {
        process.nextTick(cb.bind(this, null, knownLength));
        return;
      }
      asynckit.parallel(this._valuesToMeasure, this._lengthRetriever, function(err, values) {
        if (err) {
          cb(err);
          return;
        }
        values.forEach(function(length) {
          knownLength += length;
        });
        cb(null, knownLength);
      });
    };
    FormData3.prototype.submit = function(params, cb) {
      var request;
      var options;
      var defaults2 = { method: "post" };
      if (typeof params === "string") {
        params = parseUrl2(params);
        options = populate({
          port: params.port,
          path: params.pathname,
          host: params.hostname,
          protocol: params.protocol
        }, defaults2);
      } else {
        options = populate(params, defaults2);
        if (!options.port) {
          options.port = options.protocol === "https:" ? 443 : 80;
        }
      }
      options.headers = this.getHeaders(params.headers);
      if (options.protocol === "https:") {
        request = https2.request(options);
      } else {
        request = http3.request(options);
      }
      this.getLength(function(err, length) {
        if (err && err !== "Unknown stream") {
          this._error(err);
          return;
        }
        if (length) {
          request.setHeader("Content-Length", length);
        }
        this.pipe(request);
        if (cb) {
          var onResponse;
          var callback = function(error, responce) {
            request.removeListener("error", callback);
            request.removeListener("response", onResponse);
            return cb.call(this, error, responce);
          };
          onResponse = callback.bind(this, null);
          request.on("error", callback);
          request.on("response", onResponse);
        }
      }.bind(this));
      return request;
    };
    FormData3.prototype._error = function(err) {
      if (!this.error) {
        this.error = err;
        this.pause();
        this.emit("error", err);
      }
    };
    FormData3.prototype.toString = function() {
      return "[object FormData]";
    };
    setToStringTag(FormData3.prototype, "FormData");
    module.exports = FormData3;
  }
});

// node_modules/axios/lib/platform/node/classes/FormData.js
var import_form_data, FormData_default;
var init_FormData = __esm({
  "node_modules/axios/lib/platform/node/classes/FormData.js"() {
    import_form_data = __toESM(require_form_data(), 1);
    FormData_default = import_form_data.default;
  }
});

// node_modules/axios/lib/platform/node/classes/Buffer.js
var Buffer_default;
var init_Buffer = __esm({
  "node_modules/axios/lib/platform/node/classes/Buffer.js"() {
    "use strict";
    Buffer_default = {
      isBufferAvailable() {
        return typeof Buffer !== "undefined";
      },
      from(value) {
        return Buffer.from(value);
      }
    };
  }
});

// node_modules/axios/lib/helpers/toFormData.js
function isVisitable(thing) {
  return utils_default.isPlainObject(thing) || utils_default.isArray(thing);
}
function removeBrackets(key) {
  return utils_default.endsWith(key, "[]") ? key.slice(0, -2) : key;
}
function renderKey(path, key, dots) {
  if (!path) return key;
  return path.concat(key).map(function each(token, i) {
    token = removeBrackets(token);
    return !dots && i ? "[" + token + "]" : token;
  }).join(dots ? "." : "");
}
function isFlatArray(arr) {
  return utils_default.isArray(arr) && !arr.some(isVisitable);
}
function toFormData(obj, formData, options) {
  if (!utils_default.isObject(obj)) {
    throw new TypeError("target must be an object");
  }
  formData = formData || new (FormData_default || FormData)();
  const option = (name, fallback) => {
    const value = utils_default.getSafeProp(options, name);
    return utils_default.isUndefined(value) ? fallback : value;
  };
  const metaTokens = option("metaTokens", true);
  const visitor = option("visitor") || defaultVisitor;
  const dots = option("dots", false);
  const indexes = option("indexes", false);
  const _Blob = option("Blob") || typeof Blob !== "undefined" && Blob;
  const maxDepth = option("maxDepth", DEFAULT_FORM_DATA_MAX_DEPTH);
  const useBlob = _Blob && utils_default.isSpecCompliantForm(formData);
  const stack = [];
  if (!utils_default.isFunction(visitor)) {
    throw new TypeError("visitor must be a function");
  }
  function convertValue(value) {
    if (value === null) return "";
    if (utils_default.isDate(value)) {
      return value.toISOString();
    }
    if (utils_default.isBoolean(value)) {
      return value.toString();
    }
    if (!useBlob && utils_default.isBlob(value)) {
      throw new AxiosError_default("Blob is not supported. Use a Buffer instead.");
    }
    if (utils_default.isArrayBuffer(value) || utils_default.isTypedArray(value)) {
      if (useBlob && typeof _Blob === "function") {
        return new _Blob([value]);
      }
      if (Buffer_default && Buffer_default.isBufferAvailable()) {
        return Buffer_default.from(value);
      }
      throw new AxiosError_default(
        "Blob is not supported. Use a Buffer instead.",
        AxiosError_default.ERR_NOT_SUPPORT
      );
    }
    return value;
  }
  function throwIfMaxDepthExceeded(depth) {
    if (depth > maxDepth) {
      throw new AxiosError_default(
        "Object is too deeply nested (" + depth + " levels). Max depth: " + maxDepth,
        AxiosError_default.ERR_FORM_DATA_DEPTH_EXCEEDED
      );
    }
  }
  function stringifyWithDepthLimit(value, depth) {
    if (maxDepth === Infinity) {
      return JSON.stringify(value);
    }
    const ancestors = [];
    return JSON.stringify(value, function limitDepth(_key, currentValue) {
      if (!utils_default.isObject(currentValue)) {
        return currentValue;
      }
      while (ancestors.length && ancestors[ancestors.length - 1] !== this) {
        ancestors.pop();
      }
      ancestors.push(currentValue);
      throwIfMaxDepthExceeded(depth + ancestors.length - 1);
      return currentValue;
    });
  }
  function defaultVisitor(value, key, path) {
    let arr = value;
    if (utils_default.isReactNative(formData) && utils_default.isReactNativeBlob(value)) {
      formData.append(renderKey(path, key, dots), convertValue(value));
      return false;
    }
    if (value && !path && typeof value === "object") {
      if (utils_default.endsWith(key, "{}")) {
        key = metaTokens ? key : key.slice(0, -2);
        value = stringifyWithDepthLimit(value, 1);
      } else if (utils_default.isArray(value) && isFlatArray(value) || (utils_default.isFileList(value) || utils_default.endsWith(key, "[]")) && (arr = utils_default.toArray(value))) {
        key = removeBrackets(key);
        arr.forEach(function each(el, index) {
          !(utils_default.isUndefined(el) || el === null) && formData.append(
            // eslint-disable-next-line no-nested-ternary
            indexes === true ? renderKey([key], index, dots) : indexes === null ? key : key + "[]",
            convertValue(el)
          );
        });
        return false;
      }
    }
    if (isVisitable(value)) {
      return true;
    }
    formData.append(renderKey(path, key, dots), convertValue(value));
    return false;
  }
  const exposedHelpers = Object.assign(predicates, {
    defaultVisitor,
    convertValue,
    isVisitable
  });
  function build(value, path, depth = 0) {
    if (utils_default.isUndefined(value)) return;
    throwIfMaxDepthExceeded(depth);
    if (stack.indexOf(value) !== -1) {
      throw new Error("Circular reference detected in " + path.join("."));
    }
    stack.push(value);
    utils_default.forEach(value, function each(el, key) {
      const result = !(utils_default.isUndefined(el) || el === null) && visitor.call(formData, el, utils_default.isString(key) ? key.trim() : key, path, exposedHelpers);
      if (result === true) {
        build(el, path ? path.concat(key) : [key], depth + 1);
      }
    });
    stack.pop();
  }
  if (!utils_default.isObject(obj)) {
    throw new TypeError("data must be an object");
  }
  build(obj);
  return formData;
}
var DEFAULT_FORM_DATA_MAX_DEPTH, predicates, toFormData_default;
var init_toFormData = __esm({
  "node_modules/axios/lib/helpers/toFormData.js"() {
    "use strict";
    init_utils();
    init_AxiosError();
    init_FormData();
    init_Buffer();
    DEFAULT_FORM_DATA_MAX_DEPTH = 100;
    predicates = utils_default.toFlatObject(utils_default, {}, null, function filter(prop) {
      return /^is[A-Z]/.test(prop);
    });
    toFormData_default = toFormData;
  }
});

// node_modules/axios/lib/helpers/AxiosURLSearchParams.js
function encode(str) {
  const charMap = {
    "!": "%21",
    "'": "%27",
    "(": "%28",
    ")": "%29",
    "~": "%7E",
    "%20": "+"
  };
  return encodeURIComponent(str).replace(/[!'()~]|%20/g, function replacer(match) {
    return charMap[match];
  });
}
function AxiosURLSearchParams(params, options) {
  this._pairs = [];
  params && toFormData_default(params, this, options);
}
var prototype, AxiosURLSearchParams_default;
var init_AxiosURLSearchParams = __esm({
  "node_modules/axios/lib/helpers/AxiosURLSearchParams.js"() {
    "use strict";
    init_toFormData();
    prototype = AxiosURLSearchParams.prototype;
    prototype.append = function append(name, value) {
      this._pairs.push([name, value]);
    };
    prototype.toString = function toString2(encoder) {
      const _encode = encoder ? (value) => encoder.call(this, value, encode) : encode;
      return this._pairs.map(function each(pair) {
        return _encode(pair[0]) + "=" + _encode(pair[1]);
      }, "").join("&");
    };
    AxiosURLSearchParams_default = AxiosURLSearchParams;
  }
});

// node_modules/axios/lib/helpers/buildURL.js
function encode2(val) {
  return encodeURIComponent(val).replace(/%3A/gi, ":").replace(/%24/g, "$").replace(/%2C/gi, ",").replace(/%20/g, "+");
}
function buildURL(url2, params, options) {
  if (!params) {
    return url2;
  }
  url2 = url2 || "";
  const _options = utils_default.isFunction(options) ? {
    serialize: options
  } : options;
  const _encode = utils_default.getSafeProp(_options, "encode") || encode2;
  const serializeFn = utils_default.getSafeProp(_options, "serialize");
  let serializedParams;
  if (serializeFn) {
    serializedParams = serializeFn(params, _options);
  } else {
    serializedParams = utils_default.isURLSearchParams(params) ? params.toString() : new AxiosURLSearchParams_default(params, _options).toString(_encode);
  }
  if (serializedParams) {
    const hashmarkIndex = url2.indexOf("#");
    if (hashmarkIndex !== -1) {
      url2 = url2.slice(0, hashmarkIndex);
    }
    url2 += (url2.indexOf("?") === -1 ? "?" : "&") + serializedParams;
  }
  return url2;
}
var init_buildURL = __esm({
  "node_modules/axios/lib/helpers/buildURL.js"() {
    "use strict";
    init_utils();
    init_AxiosURLSearchParams();
  }
});

// node_modules/axios/lib/core/InterceptorManager.js
function countHandlers(handlers) {
  return handlers ? handlers.length : 0;
}
function trimHandlers(handlers) {
  if (!handlers) {
    return;
  }
  while (handlers.length && handlers[handlers.length - 1] === null) {
    handlers.pop();
  }
}
function normalizeHandlerId(id) {
  if (typeof id === "string" && id !== "" && String(Number(id)) === id) {
    return Number(id);
  }
  return id;
}
function syncHandlerEntries(manager, internals) {
  const handlers = manager.handlers;
  const length = countHandlers(handlers);
  if (handlers !== internals.handlersRef) {
    internals.handlersRef = handlers;
    internals.handlerEntries.clear();
  } else if (length !== internals.handlersLength) {
    if (!length) {
      internals.handlerEntries.clear();
    } else {
      internals.handlerEntries.forEach(function removeStaleEntry(entry, id) {
        if (handlers[entry.index] !== entry.handler) {
          internals.handlerEntries.delete(id);
        }
      });
    }
  }
  internals.handlersLength = length;
}
var $internals2, InterceptorManager, InterceptorManager_default;
var init_InterceptorManager = __esm({
  "node_modules/axios/lib/core/InterceptorManager.js"() {
    "use strict";
    init_utils();
    $internals2 = /* @__PURE__ */ Symbol("internals");
    InterceptorManager = class {
      constructor() {
        this.handlers = [];
        this[$internals2] = {
          handlersRef: this.handlers,
          handlersLength: this.handlers.length,
          handlerEntries: /* @__PURE__ */ new Map(),
          iterationDepth: 0,
          nextId: 0
        };
      }
      /**
       * Add a new interceptor to the stack
       *
       * @param {Function} fulfilled The function to handle `then` for a `Promise`
       * @param {Function} rejected The function to handle `reject` for a `Promise`
       * @param {Object} options The options for the interceptor, synchronous and runWhen
       *
       * @return {Number} An ID used to remove interceptor later
       */
      use(fulfilled, rejected, options) {
        const handler = {
          fulfilled,
          rejected,
          synchronous: options ? options.synchronous : false,
          runWhen: options ? options.runWhen : null
        };
        const internals = this[$internals2];
        if (this.handlers == null) {
          this.handlers = [];
        }
        syncHandlerEntries(this, internals);
        const id = internals.nextId++;
        this.handlers.push(handler);
        internals.handlerEntries.set(id, {
          handler,
          index: this.handlers.length - 1
        });
        internals.handlersLength = this.handlers.length;
        return id;
      }
      /**
       * Remove an interceptor from the stack
       *
       * @param {Number} id The ID that was returned by `use`
       *
       * @returns {void}
       */
      eject(id) {
        const internals = this[$internals2];
        syncHandlerEntries(this, internals);
        const handlerId = normalizeHandlerId(id);
        const entry = internals.handlerEntries.get(handlerId);
        if (entry) {
          internals.handlerEntries.delete(handlerId);
          if (this.handlers[entry.index] !== entry.handler) {
            return;
          }
          this.handlers[entry.index] = null;
          if (!internals.iterationDepth) {
            trimHandlers(this.handlers);
            internals.handlersLength = this.handlers.length;
          }
        }
      }
      /**
       * Clear all interceptors from the stack
       *
       * @returns {void}
       */
      clear() {
        if (this.handlers) {
          this.handlers = [];
          syncHandlerEntries(this, this[$internals2]);
        }
      }
      /**
       * Iterate over all the registered interceptors
       *
       * This method is particularly useful for skipping over any
       * interceptors that may have become `null` calling `eject`.
       *
       * @param {Function} fn The function to call for each interceptor
       *
       * @returns {void}
       */
      forEach(fn) {
        const internals = this[$internals2];
        syncHandlerEntries(this, internals);
        internals.iterationDepth++;
        try {
          utils_default.forEach(this.handlers, function forEachHandler(h) {
            if (h !== null) {
              fn(h);
            }
          });
        } finally {
          if (!--internals.iterationDepth) {
            syncHandlerEntries(this, internals);
            trimHandlers(this.handlers);
            internals.handlersLength = countHandlers(this.handlers);
          }
        }
      }
    };
    InterceptorManager_default = InterceptorManager;
  }
});

// node_modules/axios/lib/defaults/transitional.js
var transitional_default;
var init_transitional = __esm({
  "node_modules/axios/lib/defaults/transitional.js"() {
    "use strict";
    transitional_default = {
      silentJSONParsing: true,
      forcedJSONParsing: true,
      clarifyTimeoutError: false,
      legacyInterceptorReqResOrdering: true,
      advertiseZstdAcceptEncoding: false,
      validateStatusUndefinedResolves: true,
      encodeBasicAuthUsername: false
    };
  }
});

// node_modules/axios/lib/platform/node/classes/URLSearchParams.js
import url from "url";
var URLSearchParams_default;
var init_URLSearchParams = __esm({
  "node_modules/axios/lib/platform/node/classes/URLSearchParams.js"() {
    "use strict";
    URLSearchParams_default = url.URLSearchParams;
  }
});

// node_modules/axios/lib/platform/node/index.js
import crypto from "crypto";
var ALPHA, DIGIT, ALPHABET, generateString, node_default;
var init_node = __esm({
  "node_modules/axios/lib/platform/node/index.js"() {
    init_URLSearchParams();
    init_FormData();
    ALPHA = "abcdefghijklmnopqrstuvwxyz";
    DIGIT = "0123456789";
    ALPHABET = {
      DIGIT,
      ALPHA,
      ALPHA_DIGIT: ALPHA + ALPHA.toUpperCase() + DIGIT
    };
    generateString = (size = 16, alphabet = ALPHABET.ALPHA_DIGIT) => {
      let str = "";
      const { length } = alphabet;
      const randomValues = new Uint32Array(size);
      crypto.randomFillSync(randomValues);
      for (let i = 0; i < size; i++) {
        str += alphabet[randomValues[i] % length];
      }
      return str;
    };
    node_default = {
      isNode: true,
      classes: {
        URLSearchParams: URLSearchParams_default,
        FormData: FormData_default,
        Blob: typeof Blob !== "undefined" && Blob || null
      },
      ALPHABET,
      generateString,
      protocols: ["http", "https", "file", "data"]
    };
  }
});

// node_modules/axios/lib/platform/common/utils.js
var utils_exports = {};
__export(utils_exports, {
  hasBrowserEnv: () => hasBrowserEnv,
  hasStandardBrowserEnv: () => hasStandardBrowserEnv,
  hasStandardBrowserWebWorkerEnv: () => hasStandardBrowserWebWorkerEnv,
  navigator: () => _navigator,
  origin: () => origin
});
var hasBrowserEnv, _navigator, hasStandardBrowserEnv, hasStandardBrowserWebWorkerEnv, origin;
var init_utils2 = __esm({
  "node_modules/axios/lib/platform/common/utils.js"() {
    hasBrowserEnv = typeof window !== "undefined" && typeof document !== "undefined";
    _navigator = typeof navigator === "object" && navigator || void 0;
    hasStandardBrowserEnv = hasBrowserEnv && (!_navigator || ["ReactNative", "NativeScript", "NS"].indexOf(_navigator.product) < 0);
    hasStandardBrowserWebWorkerEnv = (() => {
      return typeof WorkerGlobalScope !== "undefined" && // eslint-disable-next-line no-undef
      self instanceof WorkerGlobalScope && typeof self.importScripts === "function";
    })();
    origin = hasBrowserEnv && window.location.href || "http://localhost";
  }
});

// node_modules/axios/lib/platform/index.js
var platform_default;
var init_platform = __esm({
  "node_modules/axios/lib/platform/index.js"() {
    init_node();
    init_utils2();
    platform_default = {
      ...utils_exports,
      ...node_default
    };
  }
});

// node_modules/axios/lib/helpers/toURLEncodedForm.js
function toURLEncodedForm(data, options) {
  return toFormData_default(data, new platform_default.classes.URLSearchParams(), {
    visitor: function(value, key, path, helpers) {
      if (platform_default.isNode && utils_default.isBuffer(value)) {
        this.append(key, value.toString("base64"));
        return false;
      }
      return helpers.defaultVisitor.apply(this, arguments);
    },
    ...options
  });
}
var init_toURLEncodedForm = __esm({
  "node_modules/axios/lib/helpers/toURLEncodedForm.js"() {
    "use strict";
    init_utils();
    init_toFormData();
    init_platform();
  }
});

// node_modules/axios/lib/helpers/formDataToJSON.js
function throwIfDepthExceeded(index) {
  if (index > MAX_DEPTH) {
    throw new AxiosError_default(
      "FormData field is too deeply nested (" + index + " levels). Max depth: " + MAX_DEPTH,
      AxiosError_default.ERR_FORM_DATA_DEPTH_EXCEEDED
    );
  }
}
function parsePropPath(name) {
  const path = [];
  const pattern = /[^.[\]]+|\[([^.[\]]*)]/g;
  let match;
  while ((match = pattern.exec(name)) !== null) {
    throwIfDepthExceeded(path.length);
    path.push(match[0] === "[]" ? "" : match[1] || match[0]);
  }
  return path;
}
function arrayToObject(arr) {
  const obj = {};
  const keys = Object.keys(arr);
  let i;
  const len = keys.length;
  let key;
  for (i = 0; i < len; i++) {
    key = keys[i];
    obj[key] = arr[key];
  }
  return obj;
}
function normalizeArray(value) {
  if (!utils_default.isArray(value)) return value;
  const keys = Object.keys(value);
  return keys.length !== value.length || keys.some((key, index) => key !== String(index)) ? arrayToObject(value) : value;
}
function normalizeArrays(obj) {
  const pending = [obj];
  while (pending.length) {
    const target = pending.pop();
    Object.keys(target).forEach((key) => {
      const value = target[key] = normalizeArray(target[key]);
      if (utils_default.isArray(value) || utils_default.isPlainObject(value)) {
        pending.push(value);
      }
    });
  }
  return obj;
}
function formDataToJSON(formData) {
  function buildPath(path, value, target, index) {
    throwIfDepthExceeded(index);
    let name = path[index++];
    if (name === "__proto__") return true;
    const isNumericKey = Number.isFinite(+name);
    const isLast = index >= path.length;
    name = !name && utils_default.isArray(target) ? target.length : name;
    if (isLast) {
      if (utils_default.hasOwnProp(target, name)) {
        target[name] = normalizeArray(target[name]);
        target[name] = utils_default.isArray(target[name]) ? target[name].concat(value) : [target[name], value];
      } else {
        target[name] = value;
      }
      return !isNumericKey;
    }
    if (!utils_default.hasOwnProp(target, name) || !utils_default.isObject(target[name])) {
      target[name] = [];
    }
    const result = buildPath(path, value, target[name], index);
    if (result && utils_default.isArray(target[name])) {
      target[name] = arrayToObject(target[name]);
    }
    return !isNumericKey;
  }
  if (utils_default.isFormData(formData) && utils_default.isFunction(formData.entries)) {
    const obj = {};
    utils_default.forEachEntry(formData, (name, value) => {
      buildPath(parsePropPath(name), value, obj, 0);
    });
    return normalizeArrays(obj);
  }
  return null;
}
var MAX_DEPTH, formDataToJSON_default;
var init_formDataToJSON = __esm({
  "node_modules/axios/lib/helpers/formDataToJSON.js"() {
    "use strict";
    init_utils();
    init_AxiosError();
    init_toFormData();
    MAX_DEPTH = DEFAULT_FORM_DATA_MAX_DEPTH;
    formDataToJSON_default = formDataToJSON;
  }
});

// node_modules/axios/lib/core/methodList.js
var methodList, methodList_default;
var init_methodList = __esm({
  "node_modules/axios/lib/core/methodList.js"() {
    "use strict";
    methodList = Object.freeze([
      "get",
      "delete",
      "head",
      "options",
      "post",
      "put",
      "patch",
      "purge",
      "link",
      "unlink",
      "query"
    ]);
    methodList_default = methodList;
  }
});

// node_modules/axios/lib/defaults/index.js
function stringifySafely2(rawValue, parser, encoder) {
  if (utils_default.isString(rawValue)) {
    try {
      (parser || JSON.parse)(rawValue);
      return utils_default.trim(rawValue);
    } catch (e) {
      if (e.name !== "SyntaxError") {
        throw e;
      }
    }
  }
  return (encoder || JSON.stringify)(rawValue);
}
var own, defaults, defaults_default;
var init_defaults = __esm({
  "node_modules/axios/lib/defaults/index.js"() {
    "use strict";
    init_utils();
    init_AxiosError();
    init_transitional();
    init_toFormData();
    init_toURLEncodedForm();
    init_platform();
    init_formDataToJSON();
    init_methodList();
    own = (obj, key) => obj != null && utils_default.hasOwnProp(obj, key) ? obj[key] : void 0;
    defaults = {
      transitional: transitional_default,
      adapter: ["xhr", "http", "fetch"],
      transformRequest: [
        function transformRequest(data, headers) {
          const contentType = headers.getContentType() || "";
          const hasJSONContentType = contentType.indexOf("application/json") > -1;
          const isObjectPayload = utils_default.isObject(data);
          if (isObjectPayload && utils_default.isHTMLForm(data)) {
            data = new FormData(data);
          }
          const isFormData2 = utils_default.isFormData(data);
          if (isFormData2) {
            return hasJSONContentType ? JSON.stringify(formDataToJSON_default(data)) : data;
          }
          if (utils_default.isArrayBuffer(data) || utils_default.isBuffer(data) || utils_default.isStream(data) || utils_default.isFile(data) || utils_default.isBlob(data) || utils_default.isReadableStream(data)) {
            return data;
          }
          if (utils_default.isArrayBufferView(data)) {
            return data.buffer;
          }
          if (utils_default.isURLSearchParams(data)) {
            headers.setContentType("application/x-www-form-urlencoded;charset=utf-8", false);
            return data.toString();
          }
          let isFileList2;
          if (isObjectPayload) {
            const formSerializer = own(this, "formSerializer");
            if (contentType.indexOf("application/x-www-form-urlencoded") > -1) {
              return toURLEncodedForm(data, formSerializer).toString();
            }
            if ((isFileList2 = utils_default.isFileList(data)) || contentType.indexOf("multipart/form-data") > -1) {
              const env = own(this, "env");
              const _FormData = env && env.FormData;
              return toFormData_default(
                isFileList2 ? { "files[]": data } : data,
                _FormData && new _FormData(),
                formSerializer
              );
            }
          }
          if (isObjectPayload || hasJSONContentType) {
            headers.setContentType("application/json", false);
            return stringifySafely2(data);
          }
          return data;
        }
      ],
      transformResponse: [
        function transformResponse(data) {
          const transitional2 = own(this, "transitional") || defaults.transitional;
          const forcedJSONParsing = transitional2 && transitional2.forcedJSONParsing;
          const responseType = own(this, "responseType");
          const JSONRequested = responseType === "json";
          if (utils_default.isResponse(data) || utils_default.isReadableStream(data)) {
            return data;
          }
          if (data && utils_default.isString(data) && (forcedJSONParsing && !responseType || JSONRequested)) {
            const silentJSONParsing = transitional2 && transitional2.silentJSONParsing;
            const strictJSONParsing = !silentJSONParsing && JSONRequested;
            try {
              return JSON.parse(data, own(this, "parseReviver"));
            } catch (e) {
              if (strictJSONParsing) {
                if (e.name === "SyntaxError") {
                  throw AxiosError_default.from(e, AxiosError_default.ERR_BAD_RESPONSE, this, null, own(this, "response"));
                }
                throw e;
              }
            }
          }
          return data;
        }
      ],
      /**
       * A timeout in milliseconds to abort a request. If set to 0 (default) a
       * timeout is not created.
       */
      timeout: 0,
      xsrfCookieName: "XSRF-TOKEN",
      xsrfHeaderName: "X-XSRF-TOKEN",
      maxContentLength: -1,
      maxBodyLength: -1,
      env: {
        FormData: platform_default.classes.FormData,
        Blob: platform_default.classes.Blob
      },
      validateStatus: function validateStatus(status) {
        return status >= 200 && status < 300;
      },
      headers: {
        common: {
          Accept: "application/json, text/plain, */*",
          "Content-Type": void 0
        }
      }
    };
    utils_default.forEach(methodList_default, (method) => {
      defaults.headers[method] = {};
    });
    defaults_default = defaults;
  }
});

// node_modules/axios/lib/core/transformData.js
function transformData(fns, response) {
  const config = this || defaults_default;
  const context = response || config;
  const headers = AxiosHeaders_default.from(context.headers);
  let data = context.data;
  utils_default.forEach(fns, function transform(fn) {
    data = fn.call(config, data, headers.normalize(), response ? response.status : void 0);
  });
  headers.normalize();
  return data;
}
var init_transformData = __esm({
  "node_modules/axios/lib/core/transformData.js"() {
    "use strict";
    init_utils();
    init_defaults();
    init_AxiosHeaders();
  }
});

// node_modules/axios/lib/cancel/isCancel.js
function isCancel(value) {
  return !!(value && value.__CANCEL__);
}
var init_isCancel = __esm({
  "node_modules/axios/lib/cancel/isCancel.js"() {
    "use strict";
  }
});

// node_modules/axios/lib/cancel/CanceledError.js
var CanceledError, CanceledError_default;
var init_CanceledError = __esm({
  "node_modules/axios/lib/cancel/CanceledError.js"() {
    "use strict";
    init_AxiosError();
    CanceledError = class extends AxiosError_default {
      /**
       * A `CanceledError` is an object that is thrown when an operation is canceled.
       *
       * @param {string=} message The message.
       * @param {Object=} config The config.
       * @param {Object=} request The request.
       *
       * @returns {CanceledError} The created error.
       */
      constructor(message, config, request) {
        super(message == null ? "canceled" : message, AxiosError_default.ERR_CANCELED, config, request);
        this.name = "CanceledError";
        this.__CANCEL__ = true;
      }
    };
    CanceledError_default = CanceledError;
  }
});

// node_modules/axios/lib/core/settle.js
function settle(resolve, reject, response) {
  const validateStatus2 = response.config.validateStatus;
  if (!response.status || !validateStatus2 || validateStatus2(response.status)) {
    resolve(response);
  } else {
    reject(new AxiosError_default(
      "Request failed with status code " + response.status,
      response.status >= 400 && response.status < 500 ? AxiosError_default.ERR_BAD_REQUEST : AxiosError_default.ERR_BAD_RESPONSE,
      response.config,
      response.request,
      response
    ));
  }
}
var init_settle = __esm({
  "node_modules/axios/lib/core/settle.js"() {
    "use strict";
    init_AxiosError();
  }
});

// node_modules/axios/lib/helpers/isAbsoluteURL.js
function isAbsoluteURL(url2) {
  if (typeof url2 !== "string") {
    return false;
  }
  return /^([a-z][a-z\d+\-.]*:)?\/\//i.test(url2);
}
var init_isAbsoluteURL = __esm({
  "node_modules/axios/lib/helpers/isAbsoluteURL.js"() {
    "use strict";
  }
});

// node_modules/axios/lib/helpers/combineURLs.js
function combineURLs(baseURL, relativeURL) {
  if (!relativeURL) {
    return baseURL;
  }
  let end = baseURL.length;
  while (end > 0 && baseURL.charCodeAt(end - 1) === 47) {
    end--;
  }
  return baseURL.slice(0, end) + "/" + relativeURL.replace(/^\/+/, "");
}
var init_combineURLs = __esm({
  "node_modules/axios/lib/helpers/combineURLs.js"() {
    "use strict";
  }
});

// node_modules/axios/lib/helpers/normalizeURLForProtocolCheck.js
function normalizeURLForProtocolCheck(url2) {
  if (typeof url2 !== "string") {
    return url2;
  }
  let start = 0;
  while (start < url2.length && url2.charCodeAt(start) <= 32) {
    start++;
  }
  return url2.slice(start).replace(urlParserControlCharacters, "");
}
var urlParserControlCharacters;
var init_normalizeURLForProtocolCheck = __esm({
  "node_modules/axios/lib/helpers/normalizeURLForProtocolCheck.js"() {
    "use strict";
    urlParserControlCharacters = /[\t\n\r]/g;
  }
});

// node_modules/axios/lib/core/buildFullPath.js
function redactFragment(fragment) {
  if (!fragment) {
    return fragment;
  }
  return fragment.replace(/(^|&)([^=&]*=)?[^&]+/g, (match, separator, parameterName = "") => {
    return `${separator}${parameterName}${REDACTED}`;
  });
}
function redactSensitiveURLParts(url2) {
  const redactedURL = url2.replace(/^(https?:\/{0,2})[^/?#]*@/i, `$1${REDACTED}@`);
  const fragmentIndex = redactedURL.indexOf("#");
  const urlWithoutFragment = fragmentIndex === -1 ? redactedURL : redactedURL.slice(0, fragmentIndex);
  const redactedURLWithoutFragment = urlWithoutFragment.replace(
    /([?&][^=&#]*=)[^&#]*/g,
    `$1${REDACTED}`
  );
  if (fragmentIndex === -1) {
    return redactedURLWithoutFragment;
  }
  return `${redactedURLWithoutFragment}#${redactFragment(redactedURL.slice(fragmentIndex + 1))}`;
}
function assertValidHttpProtocolURL(url2, config) {
  if (typeof url2 === "string") {
    const normalizedURL = normalizeURLForProtocolCheck(url2);
    if (malformedHttpProtocol.test(normalizedURL)) {
      throw new AxiosError_default(
        `Invalid URL ${JSON.stringify(redactSensitiveURLParts(normalizedURL))}: missing "//" after protocol`,
        AxiosError_default.ERR_INVALID_URL,
        config
      );
    }
  }
}
function buildFullPath(baseURL, requestedURL, allowAbsoluteUrls, config) {
  assertValidHttpProtocolURL(requestedURL, config);
  let isRelativeUrl = !isAbsoluteURL(requestedURL);
  if (baseURL && (isRelativeUrl || allowAbsoluteUrls === false)) {
    assertValidHttpProtocolURL(baseURL, config);
    return combineURLs(baseURL, requestedURL);
  }
  return requestedURL;
}
var malformedHttpProtocol;
var init_buildFullPath = __esm({
  "node_modules/axios/lib/core/buildFullPath.js"() {
    "use strict";
    init_AxiosError();
    init_isAbsoluteURL();
    init_combineURLs();
    init_normalizeURLForProtocolCheck();
    malformedHttpProtocol = /^https?:(?!\/\/)/i;
  }
});

// node_modules/proxy-from-env/index.js
function parseUrl(urlString) {
  try {
    return new URL(urlString);
  } catch {
    return null;
  }
}
function getProxyForUrl(url2) {
  var parsedUrl = (typeof url2 === "string" ? parseUrl(url2) : url2) || {};
  var proto = parsedUrl.protocol;
  var hostname = parsedUrl.host;
  var port = parsedUrl.port;
  if (typeof hostname !== "string" || !hostname || typeof proto !== "string") {
    return "";
  }
  proto = proto.split(":", 1)[0];
  hostname = hostname.replace(/:\d*$/, "");
  port = parseInt(port) || DEFAULT_PORTS[proto] || 0;
  if (!shouldProxy(hostname, port)) {
    return "";
  }
  var proxy = getEnv(proto + "_proxy") || getEnv("all_proxy");
  if (proxy && proxy.indexOf("://") === -1) {
    proxy = proto + "://" + proxy;
  }
  return proxy;
}
function shouldProxy(hostname, port) {
  var NO_PROXY = getEnv("no_proxy").toLowerCase();
  if (!NO_PROXY) {
    return true;
  }
  if (NO_PROXY === "*") {
    return false;
  }
  return NO_PROXY.split(/[,\s]/).every(function(proxy) {
    if (!proxy) {
      return true;
    }
    var parsedProxy = proxy.match(/^(.+):(\d+)$/);
    var parsedProxyHostname = parsedProxy ? parsedProxy[1] : proxy;
    var parsedProxyPort = parsedProxy ? parseInt(parsedProxy[2]) : 0;
    if (parsedProxyPort && parsedProxyPort !== port) {
      return true;
    }
    if (!/^[.*]/.test(parsedProxyHostname)) {
      return hostname !== parsedProxyHostname;
    }
    if (parsedProxyHostname.charAt(0) === "*") {
      parsedProxyHostname = parsedProxyHostname.slice(1);
    }
    return !hostname.endsWith(parsedProxyHostname);
  });
}
function getEnv(key) {
  return process.env[key.toLowerCase()] || process.env[key.toUpperCase()] || "";
}
var DEFAULT_PORTS;
var init_proxy_from_env = __esm({
  "node_modules/proxy-from-env/index.js"() {
    "use strict";
    DEFAULT_PORTS = {
      ftp: 21,
      gopher: 70,
      http: 80,
      https: 443,
      ws: 80,
      wss: 443
    };
  }
});

// node_modules/ms/index.js
var require_ms = __commonJS({
  "node_modules/ms/index.js"(exports, module) {
    var s = 1e3;
    var m = s * 60;
    var h = m * 60;
    var d = h * 24;
    var w = d * 7;
    var y = d * 365.25;
    module.exports = function(val, options) {
      options = options || {};
      var type = typeof val;
      if (type === "string" && val.length > 0) {
        return parse(val);
      } else if (type === "number" && isFinite(val)) {
        return options.long ? fmtLong(val) : fmtShort(val);
      }
      throw new Error(
        "val is not a non-empty string or a valid number. val=" + JSON.stringify(val)
      );
    };
    function parse(str) {
      str = String(str);
      if (str.length > 100) {
        return;
      }
      var match = /^(-?(?:\d+)?\.?\d+) *(milliseconds?|msecs?|ms|seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h|days?|d|weeks?|w|years?|yrs?|y)?$/i.exec(
        str
      );
      if (!match) {
        return;
      }
      var n = parseFloat(match[1]);
      var type = (match[2] || "ms").toLowerCase();
      switch (type) {
        case "years":
        case "year":
        case "yrs":
        case "yr":
        case "y":
          return n * y;
        case "weeks":
        case "week":
        case "w":
          return n * w;
        case "days":
        case "day":
        case "d":
          return n * d;
        case "hours":
        case "hour":
        case "hrs":
        case "hr":
        case "h":
          return n * h;
        case "minutes":
        case "minute":
        case "mins":
        case "min":
        case "m":
          return n * m;
        case "seconds":
        case "second":
        case "secs":
        case "sec":
        case "s":
          return n * s;
        case "milliseconds":
        case "millisecond":
        case "msecs":
        case "msec":
        case "ms":
          return n;
        default:
          return void 0;
      }
    }
    function fmtShort(ms) {
      var msAbs = Math.abs(ms);
      if (msAbs >= d) {
        return Math.round(ms / d) + "d";
      }
      if (msAbs >= h) {
        return Math.round(ms / h) + "h";
      }
      if (msAbs >= m) {
        return Math.round(ms / m) + "m";
      }
      if (msAbs >= s) {
        return Math.round(ms / s) + "s";
      }
      return ms + "ms";
    }
    function fmtLong(ms) {
      var msAbs = Math.abs(ms);
      if (msAbs >= d) {
        return plural(ms, msAbs, d, "day");
      }
      if (msAbs >= h) {
        return plural(ms, msAbs, h, "hour");
      }
      if (msAbs >= m) {
        return plural(ms, msAbs, m, "minute");
      }
      if (msAbs >= s) {
        return plural(ms, msAbs, s, "second");
      }
      return ms + " ms";
    }
    function plural(ms, msAbs, n, name) {
      var isPlural = msAbs >= n * 1.5;
      return Math.round(ms / n) + " " + name + (isPlural ? "s" : "");
    }
  }
});

// node_modules/debug/src/common.js
var require_common = __commonJS({
  "node_modules/debug/src/common.js"(exports, module) {
    function setup(env) {
      createDebug.debug = createDebug;
      createDebug.default = createDebug;
      createDebug.coerce = coerce;
      createDebug.disable = disable;
      createDebug.enable = enable;
      createDebug.enabled = enabled;
      createDebug.humanize = require_ms();
      createDebug.destroy = destroy;
      Object.keys(env).forEach((key) => {
        createDebug[key] = env[key];
      });
      createDebug.names = [];
      createDebug.skips = [];
      createDebug.formatters = {};
      function selectColor(namespace) {
        let hash = 0;
        for (let i = 0; i < namespace.length; i++) {
          hash = (hash << 5) - hash + namespace.charCodeAt(i);
          hash |= 0;
        }
        return createDebug.colors[Math.abs(hash) % createDebug.colors.length];
      }
      createDebug.selectColor = selectColor;
      function createDebug(namespace) {
        let prevTime;
        let enableOverride = null;
        let namespacesCache;
        let enabledCache;
        function debug(...args) {
          if (!debug.enabled) {
            return;
          }
          const self2 = debug;
          const curr = Number(/* @__PURE__ */ new Date());
          const ms = curr - (prevTime || curr);
          self2.diff = ms;
          self2.prev = prevTime;
          self2.curr = curr;
          prevTime = curr;
          args[0] = createDebug.coerce(args[0]);
          if (typeof args[0] !== "string") {
            args.unshift("%O");
          }
          let index = 0;
          args[0] = args[0].replace(/%([a-zA-Z%])/g, (match, format) => {
            if (match === "%%") {
              return "%";
            }
            index++;
            const formatter = createDebug.formatters[format];
            if (typeof formatter === "function") {
              const val = args[index];
              match = formatter.call(self2, val);
              args.splice(index, 1);
              index--;
            }
            return match;
          });
          createDebug.formatArgs.call(self2, args);
          const logFn = self2.log || createDebug.log;
          logFn.apply(self2, args);
        }
        debug.namespace = namespace;
        debug.useColors = createDebug.useColors();
        debug.color = createDebug.selectColor(namespace);
        debug.extend = extend2;
        debug.destroy = createDebug.destroy;
        Object.defineProperty(debug, "enabled", {
          enumerable: true,
          configurable: false,
          get: () => {
            if (enableOverride !== null) {
              return enableOverride;
            }
            if (namespacesCache !== createDebug.namespaces) {
              namespacesCache = createDebug.namespaces;
              enabledCache = createDebug.enabled(namespace);
            }
            return enabledCache;
          },
          set: (v) => {
            enableOverride = v;
          }
        });
        if (typeof createDebug.init === "function") {
          createDebug.init(debug);
        }
        return debug;
      }
      function extend2(namespace, delimiter) {
        const newDebug = createDebug(this.namespace + (typeof delimiter === "undefined" ? ":" : delimiter) + namespace);
        newDebug.log = this.log;
        return newDebug;
      }
      function enable(namespaces) {
        createDebug.save(namespaces);
        createDebug.namespaces = namespaces;
        createDebug.names = [];
        createDebug.skips = [];
        const split = (typeof namespaces === "string" ? namespaces : "").trim().replace(/\s+/g, ",").split(",").filter(Boolean);
        for (const ns of split) {
          if (ns[0] === "-") {
            createDebug.skips.push(ns.slice(1));
          } else {
            createDebug.names.push(ns);
          }
        }
      }
      function matchesTemplate(search, template) {
        let searchIndex = 0;
        let templateIndex = 0;
        let starIndex = -1;
        let matchIndex = 0;
        while (searchIndex < search.length) {
          if (templateIndex < template.length && (template[templateIndex] === search[searchIndex] || template[templateIndex] === "*")) {
            if (template[templateIndex] === "*") {
              starIndex = templateIndex;
              matchIndex = searchIndex;
              templateIndex++;
            } else {
              searchIndex++;
              templateIndex++;
            }
          } else if (starIndex !== -1) {
            templateIndex = starIndex + 1;
            matchIndex++;
            searchIndex = matchIndex;
          } else {
            return false;
          }
        }
        while (templateIndex < template.length && template[templateIndex] === "*") {
          templateIndex++;
        }
        return templateIndex === template.length;
      }
      function disable() {
        const namespaces = [
          ...createDebug.names,
          ...createDebug.skips.map((namespace) => "-" + namespace)
        ].join(",");
        createDebug.enable("");
        return namespaces;
      }
      function enabled(name) {
        for (const skip of createDebug.skips) {
          if (matchesTemplate(name, skip)) {
            return false;
          }
        }
        for (const ns of createDebug.names) {
          if (matchesTemplate(name, ns)) {
            return true;
          }
        }
        return false;
      }
      function coerce(val) {
        if (val instanceof Error) {
          return val.stack || val.message;
        }
        return val;
      }
      function destroy() {
        console.warn("Instance method `debug.destroy()` is deprecated and no longer does anything. It will be removed in the next major version of `debug`.");
      }
      createDebug.enable(createDebug.load());
      return createDebug;
    }
    module.exports = setup;
  }
});

// node_modules/debug/src/browser.js
var require_browser = __commonJS({
  "node_modules/debug/src/browser.js"(exports, module) {
    exports.formatArgs = formatArgs;
    exports.save = save;
    exports.load = load;
    exports.useColors = useColors;
    exports.storage = localstorage();
    exports.destroy = /* @__PURE__ */ (() => {
      let warned = false;
      return () => {
        if (!warned) {
          warned = true;
          console.warn("Instance method `debug.destroy()` is deprecated and no longer does anything. It will be removed in the next major version of `debug`.");
        }
      };
    })();
    exports.colors = [
      "#0000CC",
      "#0000FF",
      "#0033CC",
      "#0033FF",
      "#0066CC",
      "#0066FF",
      "#0099CC",
      "#0099FF",
      "#00CC00",
      "#00CC33",
      "#00CC66",
      "#00CC99",
      "#00CCCC",
      "#00CCFF",
      "#3300CC",
      "#3300FF",
      "#3333CC",
      "#3333FF",
      "#3366CC",
      "#3366FF",
      "#3399CC",
      "#3399FF",
      "#33CC00",
      "#33CC33",
      "#33CC66",
      "#33CC99",
      "#33CCCC",
      "#33CCFF",
      "#6600CC",
      "#6600FF",
      "#6633CC",
      "#6633FF",
      "#66CC00",
      "#66CC33",
      "#9900CC",
      "#9900FF",
      "#9933CC",
      "#9933FF",
      "#99CC00",
      "#99CC33",
      "#CC0000",
      "#CC0033",
      "#CC0066",
      "#CC0099",
      "#CC00CC",
      "#CC00FF",
      "#CC3300",
      "#CC3333",
      "#CC3366",
      "#CC3399",
      "#CC33CC",
      "#CC33FF",
      "#CC6600",
      "#CC6633",
      "#CC9900",
      "#CC9933",
      "#CCCC00",
      "#CCCC33",
      "#FF0000",
      "#FF0033",
      "#FF0066",
      "#FF0099",
      "#FF00CC",
      "#FF00FF",
      "#FF3300",
      "#FF3333",
      "#FF3366",
      "#FF3399",
      "#FF33CC",
      "#FF33FF",
      "#FF6600",
      "#FF6633",
      "#FF9900",
      "#FF9933",
      "#FFCC00",
      "#FFCC33"
    ];
    function useColors() {
      if (typeof window !== "undefined" && window.process && (window.process.type === "renderer" || window.process.__nwjs)) {
        return true;
      }
      if (typeof navigator !== "undefined" && navigator.userAgent && navigator.userAgent.toLowerCase().match(/(edge|trident)\/(\d+)/)) {
        return false;
      }
      let m;
      return typeof document !== "undefined" && document.documentElement && document.documentElement.style && document.documentElement.style.WebkitAppearance || // Is firebug? http://stackoverflow.com/a/398120/376773
      typeof window !== "undefined" && window.console && (window.console.firebug || window.console.exception && window.console.table) || // Is firefox >= v31?
      // https://developer.mozilla.org/en-US/docs/Tools/Web_Console#Styling_messages
      typeof navigator !== "undefined" && navigator.userAgent && (m = navigator.userAgent.toLowerCase().match(/firefox\/(\d+)/)) && parseInt(m[1], 10) >= 31 || // Double check webkit in userAgent just in case we are in a worker
      typeof navigator !== "undefined" && navigator.userAgent && navigator.userAgent.toLowerCase().match(/applewebkit\/(\d+)/);
    }
    function formatArgs(args) {
      args[0] = (this.useColors ? "%c" : "") + this.namespace + (this.useColors ? " %c" : " ") + args[0] + (this.useColors ? "%c " : " ") + "+" + module.exports.humanize(this.diff);
      if (!this.useColors) {
        return;
      }
      const c = "color: " + this.color;
      args.splice(1, 0, c, "color: inherit");
      let index = 0;
      let lastC = 0;
      args[0].replace(/%[a-zA-Z%]/g, (match) => {
        if (match === "%%") {
          return;
        }
        index++;
        if (match === "%c") {
          lastC = index;
        }
      });
      args.splice(lastC, 0, c);
    }
    exports.log = console.debug || console.log || (() => {
    });
    function save(namespaces) {
      try {
        if (namespaces) {
          exports.storage.setItem("debug", namespaces);
        } else {
          exports.storage.removeItem("debug");
        }
      } catch (error) {
      }
    }
    function load() {
      let r;
      try {
        r = exports.storage.getItem("debug") || exports.storage.getItem("DEBUG");
      } catch (error) {
      }
      if (!r && typeof process !== "undefined" && "env" in process) {
        r = process.env.DEBUG;
      }
      return r;
    }
    function localstorage() {
      try {
        return localStorage;
      } catch (error) {
      }
    }
    module.exports = require_common()(exports);
    var { formatters } = module.exports;
    formatters.j = function(v) {
      try {
        return JSON.stringify(v);
      } catch (error) {
        return "[UnexpectedJSONParseError]: " + error.message;
      }
    };
  }
});

// node_modules/debug/src/node.js
var require_node = __commonJS({
  "node_modules/debug/src/node.js"(exports, module) {
    var tty = __require("tty");
    var util4 = __require("util");
    exports.init = init;
    exports.log = log;
    exports.formatArgs = formatArgs;
    exports.save = save;
    exports.load = load;
    exports.useColors = useColors;
    exports.destroy = util4.deprecate(
      () => {
      },
      "Instance method `debug.destroy()` is deprecated and no longer does anything. It will be removed in the next major version of `debug`."
    );
    exports.colors = [6, 2, 3, 4, 5, 1];
    try {
      const supportsColor = __require("supports-color");
      if (supportsColor && (supportsColor.stderr || supportsColor).level >= 2) {
        exports.colors = [
          20,
          21,
          26,
          27,
          32,
          33,
          38,
          39,
          40,
          41,
          42,
          43,
          44,
          45,
          56,
          57,
          62,
          63,
          68,
          69,
          74,
          75,
          76,
          77,
          78,
          79,
          80,
          81,
          92,
          93,
          98,
          99,
          112,
          113,
          128,
          129,
          134,
          135,
          148,
          149,
          160,
          161,
          162,
          163,
          164,
          165,
          166,
          167,
          168,
          169,
          170,
          171,
          172,
          173,
          178,
          179,
          184,
          185,
          196,
          197,
          198,
          199,
          200,
          201,
          202,
          203,
          204,
          205,
          206,
          207,
          208,
          209,
          214,
          215,
          220,
          221
        ];
      }
    } catch (error) {
    }
    exports.inspectOpts = Object.keys(process.env).filter((key) => {
      return /^debug_/i.test(key);
    }).reduce((obj, key) => {
      const prop = key.substring(6).toLowerCase().replace(/_([a-z])/g, (_, k) => {
        return k.toUpperCase();
      });
      let val = process.env[key];
      if (/^(yes|on|true|enabled)$/i.test(val)) {
        val = true;
      } else if (/^(no|off|false|disabled)$/i.test(val)) {
        val = false;
      } else if (val === "null") {
        val = null;
      } else {
        val = Number(val);
      }
      obj[prop] = val;
      return obj;
    }, {});
    function useColors() {
      return "colors" in exports.inspectOpts ? Boolean(exports.inspectOpts.colors) : tty.isatty(process.stderr.fd);
    }
    function formatArgs(args) {
      const { namespace: name, useColors: useColors2 } = this;
      if (useColors2) {
        const c = this.color;
        const colorCode = "\x1B[3" + (c < 8 ? c : "8;5;" + c);
        const prefix = `  ${colorCode};1m${name} \x1B[0m`;
        args[0] = prefix + args[0].split("\n").join("\n" + prefix);
        args.push(colorCode + "m+" + module.exports.humanize(this.diff) + "\x1B[0m");
      } else {
        args[0] = getDate() + name + " " + args[0];
      }
    }
    function getDate() {
      if (exports.inspectOpts.hideDate) {
        return "";
      }
      return (/* @__PURE__ */ new Date()).toISOString() + " ";
    }
    function log(...args) {
      return process.stderr.write(util4.formatWithOptions(exports.inspectOpts, ...args) + "\n");
    }
    function save(namespaces) {
      if (namespaces) {
        process.env.DEBUG = namespaces;
      } else {
        delete process.env.DEBUG;
      }
    }
    function load() {
      return process.env.DEBUG;
    }
    function init(debug) {
      debug.inspectOpts = {};
      const keys = Object.keys(exports.inspectOpts);
      for (let i = 0; i < keys.length; i++) {
        debug.inspectOpts[keys[i]] = exports.inspectOpts[keys[i]];
      }
    }
    module.exports = require_common()(exports);
    var { formatters } = module.exports;
    formatters.o = function(v) {
      this.inspectOpts.colors = this.useColors;
      return util4.inspect(v, this.inspectOpts).split("\n").map((str) => str.trim()).join(" ");
    };
    formatters.O = function(v) {
      this.inspectOpts.colors = this.useColors;
      return util4.inspect(v, this.inspectOpts);
    };
  }
});

// node_modules/debug/src/index.js
var require_src = __commonJS({
  "node_modules/debug/src/index.js"(exports, module) {
    if (typeof process === "undefined" || process.type === "renderer" || process.browser === true || process.__nwjs) {
      module.exports = require_browser();
    } else {
      module.exports = require_node();
    }
  }
});

// node_modules/agent-base/dist/src/promisify.js
var require_promisify = __commonJS({
  "node_modules/agent-base/dist/src/promisify.js"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    function promisify(fn) {
      return function(req, opts) {
        return new Promise((resolve, reject) => {
          fn.call(this, req, opts, (err, rtn) => {
            if (err) {
              reject(err);
            } else {
              resolve(rtn);
            }
          });
        });
      };
    }
    exports.default = promisify;
  }
});

// node_modules/agent-base/dist/src/index.js
var require_src2 = __commonJS({
  "node_modules/agent-base/dist/src/index.js"(exports, module) {
    "use strict";
    var __importDefault = exports && exports.__importDefault || function(mod) {
      return mod && mod.__esModule ? mod : { "default": mod };
    };
    var events_1 = __require("events");
    var debug_1 = __importDefault(require_src());
    var promisify_1 = __importDefault(require_promisify());
    var debug = debug_1.default("agent-base");
    function isAgent(v) {
      return Boolean(v) && typeof v.addRequest === "function";
    }
    function isSecureEndpoint() {
      const { stack } = new Error();
      if (typeof stack !== "string")
        return false;
      return stack.split("\n").some((l) => l.indexOf("(https.js:") !== -1 || l.indexOf("node:https:") !== -1);
    }
    function createAgent(callback, opts) {
      return new createAgent.Agent(callback, opts);
    }
    (function(createAgent2) {
      class Agent extends events_1.EventEmitter {
        constructor(callback, _opts) {
          super();
          let opts = _opts;
          if (typeof callback === "function") {
            this.callback = callback;
          } else if (callback) {
            opts = callback;
          }
          this.timeout = null;
          if (opts && typeof opts.timeout === "number") {
            this.timeout = opts.timeout;
          }
          this.maxFreeSockets = 1;
          this.maxSockets = 1;
          this.maxTotalSockets = Infinity;
          this.sockets = {};
          this.freeSockets = {};
          this.requests = {};
          this.options = {};
        }
        get defaultPort() {
          if (typeof this.explicitDefaultPort === "number") {
            return this.explicitDefaultPort;
          }
          return isSecureEndpoint() ? 443 : 80;
        }
        set defaultPort(v) {
          this.explicitDefaultPort = v;
        }
        get protocol() {
          if (typeof this.explicitProtocol === "string") {
            return this.explicitProtocol;
          }
          return isSecureEndpoint() ? "https:" : "http:";
        }
        set protocol(v) {
          this.explicitProtocol = v;
        }
        callback(req, opts, fn) {
          throw new Error('"agent-base" has no default implementation, you must subclass and override `callback()`');
        }
        /**
         * Called by node-core's "_http_client.js" module when creating
         * a new HTTP request with this Agent instance.
         *
         * @api public
         */
        addRequest(req, _opts) {
          const opts = Object.assign({}, _opts);
          if (typeof opts.secureEndpoint !== "boolean") {
            opts.secureEndpoint = isSecureEndpoint();
          }
          if (opts.host == null) {
            opts.host = "localhost";
          }
          if (opts.port == null) {
            opts.port = opts.secureEndpoint ? 443 : 80;
          }
          if (opts.protocol == null) {
            opts.protocol = opts.secureEndpoint ? "https:" : "http:";
          }
          if (opts.host && opts.path) {
            delete opts.path;
          }
          delete opts.agent;
          delete opts.hostname;
          delete opts._defaultAgent;
          delete opts.defaultPort;
          delete opts.createConnection;
          req._last = true;
          req.shouldKeepAlive = false;
          let timedOut = false;
          let timeoutId = null;
          const timeoutMs = opts.timeout || this.timeout;
          const onerror = (err) => {
            if (req._hadError)
              return;
            req.emit("error", err);
            req._hadError = true;
          };
          const ontimeout = () => {
            timeoutId = null;
            timedOut = true;
            const err = new Error(`A "socket" was not created for HTTP request before ${timeoutMs}ms`);
            err.code = "ETIMEOUT";
            onerror(err);
          };
          const callbackError = (err) => {
            if (timedOut)
              return;
            if (timeoutId !== null) {
              clearTimeout(timeoutId);
              timeoutId = null;
            }
            onerror(err);
          };
          const onsocket = (socket) => {
            if (timedOut)
              return;
            if (timeoutId != null) {
              clearTimeout(timeoutId);
              timeoutId = null;
            }
            if (isAgent(socket)) {
              debug("Callback returned another Agent instance %o", socket.constructor.name);
              socket.addRequest(req, opts);
              return;
            }
            if (socket) {
              socket.once("free", () => {
                this.freeSocket(socket, opts);
              });
              req.onSocket(socket);
              return;
            }
            const err = new Error(`no Duplex stream was returned to agent-base for \`${req.method} ${req.path}\``);
            onerror(err);
          };
          if (typeof this.callback !== "function") {
            onerror(new Error("`callback` is not defined"));
            return;
          }
          if (!this.promisifiedCallback) {
            if (this.callback.length >= 3) {
              debug("Converting legacy callback function to promise");
              this.promisifiedCallback = promisify_1.default(this.callback);
            } else {
              this.promisifiedCallback = this.callback;
            }
          }
          if (typeof timeoutMs === "number" && timeoutMs > 0) {
            timeoutId = setTimeout(ontimeout, timeoutMs);
          }
          if ("port" in opts && typeof opts.port !== "number") {
            opts.port = Number(opts.port);
          }
          try {
            debug("Resolving socket for %o request: %o", opts.protocol, `${req.method} ${req.path}`);
            Promise.resolve(this.promisifiedCallback(req, opts)).then(onsocket, callbackError);
          } catch (err) {
            Promise.reject(err).catch(callbackError);
          }
        }
        freeSocket(socket, opts) {
          debug("Freeing socket %o %o", socket.constructor.name, opts);
          socket.destroy();
        }
        destroy() {
          debug("Destroying agent %o", this.constructor.name);
        }
      }
      createAgent2.Agent = Agent;
      createAgent2.prototype = createAgent2.Agent.prototype;
    })(createAgent || (createAgent = {}));
    module.exports = createAgent;
  }
});

// node_modules/https-proxy-agent/dist/parse-proxy-response.js
var require_parse_proxy_response = __commonJS({
  "node_modules/https-proxy-agent/dist/parse-proxy-response.js"(exports) {
    "use strict";
    var __importDefault = exports && exports.__importDefault || function(mod) {
      return mod && mod.__esModule ? mod : { "default": mod };
    };
    Object.defineProperty(exports, "__esModule", { value: true });
    var debug_1 = __importDefault(require_src());
    var debug = debug_1.default("https-proxy-agent:parse-proxy-response");
    function parseProxyResponse(socket) {
      return new Promise((resolve, reject) => {
        let buffersLength = 0;
        const buffers = [];
        function read() {
          const b = socket.read();
          if (b)
            ondata(b);
          else
            socket.once("readable", read);
        }
        function cleanup() {
          socket.removeListener("end", onend);
          socket.removeListener("error", onerror);
          socket.removeListener("close", onclose);
          socket.removeListener("readable", read);
        }
        function onclose(err) {
          debug("onclose had error %o", err);
        }
        function onend() {
          debug("onend");
        }
        function onerror(err) {
          cleanup();
          debug("onerror %o", err);
          reject(err);
        }
        function ondata(b) {
          buffers.push(b);
          buffersLength += b.length;
          const buffered = Buffer.concat(buffers, buffersLength);
          const endOfHeaders = buffered.indexOf("\r\n\r\n");
          if (endOfHeaders === -1) {
            debug("have not received end of HTTP headers yet...");
            read();
            return;
          }
          const firstLine = buffered.toString("ascii", 0, buffered.indexOf("\r\n"));
          const statusCode = +firstLine.split(" ")[1];
          debug("got proxy server response: %o", firstLine);
          resolve({
            statusCode,
            buffered
          });
        }
        socket.on("error", onerror);
        socket.on("close", onclose);
        socket.on("end", onend);
        read();
      });
    }
    exports.default = parseProxyResponse;
  }
});

// node_modules/https-proxy-agent/dist/agent.js
var require_agent = __commonJS({
  "node_modules/https-proxy-agent/dist/agent.js"(exports) {
    "use strict";
    var __awaiter = exports && exports.__awaiter || function(thisArg, _arguments, P, generator) {
      function adopt(value) {
        return value instanceof P ? value : new P(function(resolve) {
          resolve(value);
        });
      }
      return new (P || (P = Promise))(function(resolve, reject) {
        function fulfilled(value) {
          try {
            step(generator.next(value));
          } catch (e) {
            reject(e);
          }
        }
        function rejected(value) {
          try {
            step(generator["throw"](value));
          } catch (e) {
            reject(e);
          }
        }
        function step(result) {
          result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected);
        }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
      });
    };
    var __importDefault = exports && exports.__importDefault || function(mod) {
      return mod && mod.__esModule ? mod : { "default": mod };
    };
    Object.defineProperty(exports, "__esModule", { value: true });
    var net_1 = __importDefault(__require("net"));
    var tls_1 = __importDefault(__require("tls"));
    var url_1 = __importDefault(__require("url"));
    var assert_1 = __importDefault(__require("assert"));
    var debug_1 = __importDefault(require_src());
    var agent_base_1 = require_src2();
    var parse_proxy_response_1 = __importDefault(require_parse_proxy_response());
    var debug = debug_1.default("https-proxy-agent:agent");
    var HttpsProxyAgent2 = class extends agent_base_1.Agent {
      constructor(_opts) {
        let opts;
        if (typeof _opts === "string") {
          opts = url_1.default.parse(_opts);
        } else {
          opts = _opts;
        }
        if (!opts) {
          throw new Error("an HTTP(S) proxy server `host` and `port` must be specified!");
        }
        debug("creating new HttpsProxyAgent instance: %o", opts);
        super(opts);
        const proxy = Object.assign({}, opts);
        this.secureProxy = opts.secureProxy || isHTTPS(proxy.protocol);
        proxy.host = proxy.hostname || proxy.host;
        if (typeof proxy.port === "string") {
          proxy.port = parseInt(proxy.port, 10);
        }
        if (!proxy.port && proxy.host) {
          proxy.port = this.secureProxy ? 443 : 80;
        }
        if (this.secureProxy && !("ALPNProtocols" in proxy)) {
          proxy.ALPNProtocols = ["http 1.1"];
        }
        if (proxy.host && proxy.path) {
          delete proxy.path;
          delete proxy.pathname;
        }
        this.proxy = proxy;
      }
      /**
       * Called when the node-core HTTP client library is creating a
       * new HTTP request.
       *
       * @api protected
       */
      callback(req, opts) {
        return __awaiter(this, void 0, void 0, function* () {
          const { proxy, secureProxy } = this;
          let socket;
          if (secureProxy) {
            debug("Creating `tls.Socket`: %o", proxy);
            socket = tls_1.default.connect(proxy);
          } else {
            debug("Creating `net.Socket`: %o", proxy);
            socket = net_1.default.connect(proxy);
          }
          const headers = Object.assign({}, proxy.headers);
          const hostname = `${opts.host}:${opts.port}`;
          let payload = `CONNECT ${hostname} HTTP/1.1\r
`;
          if (proxy.auth) {
            headers["Proxy-Authorization"] = `Basic ${Buffer.from(proxy.auth).toString("base64")}`;
          }
          let { host, port, secureEndpoint } = opts;
          if (!isDefaultPort(port, secureEndpoint)) {
            host += `:${port}`;
          }
          headers.Host = host;
          headers.Connection = "close";
          for (const name of Object.keys(headers)) {
            payload += `${name}: ${headers[name]}\r
`;
          }
          const proxyResponsePromise = parse_proxy_response_1.default(socket);
          socket.write(`${payload}\r
`);
          const { statusCode, buffered } = yield proxyResponsePromise;
          if (statusCode === 200) {
            req.once("socket", resume);
            if (opts.secureEndpoint) {
              debug("Upgrading socket connection to TLS");
              const servername = opts.servername || opts.host;
              return tls_1.default.connect(Object.assign(Object.assign({}, omit(opts, "host", "hostname", "path", "port")), {
                socket,
                servername
              }));
            }
            return socket;
          }
          socket.destroy();
          const fakeSocket = new net_1.default.Socket({ writable: false });
          fakeSocket.readable = true;
          req.once("socket", (s) => {
            debug("replaying proxy buffer for failed request");
            assert_1.default(s.listenerCount("data") > 0);
            s.push(buffered);
            s.push(null);
          });
          return fakeSocket;
        });
      }
    };
    exports.default = HttpsProxyAgent2;
    function resume(socket) {
      socket.resume();
    }
    function isDefaultPort(port, secure) {
      return Boolean(!secure && port === 80 || secure && port === 443);
    }
    function isHTTPS(protocol) {
      return typeof protocol === "string" ? /^https:?$/i.test(protocol) : false;
    }
    function omit(obj, ...keys) {
      const ret = {};
      let key;
      for (key in obj) {
        if (!keys.includes(key)) {
          ret[key] = obj[key];
        }
      }
      return ret;
    }
  }
});

// node_modules/https-proxy-agent/dist/index.js
var require_dist = __commonJS({
  "node_modules/https-proxy-agent/dist/index.js"(exports, module) {
    "use strict";
    var __importDefault = exports && exports.__importDefault || function(mod) {
      return mod && mod.__esModule ? mod : { "default": mod };
    };
    var agent_1 = __importDefault(require_agent());
    function createHttpsProxyAgent(opts) {
      return new agent_1.default(opts);
    }
    (function(createHttpsProxyAgent2) {
      createHttpsProxyAgent2.HttpsProxyAgent = agent_1.default;
      createHttpsProxyAgent2.prototype = agent_1.default.prototype;
    })(createHttpsProxyAgent || (createHttpsProxyAgent = {}));
    module.exports = createHttpsProxyAgent;
  }
});

// node_modules/follow-redirects/debug.js
var require_debug = __commonJS({
  "node_modules/follow-redirects/debug.js"(exports, module) {
    var debug;
    module.exports = function() {
      if (!debug) {
        try {
          debug = require_src()("follow-redirects");
        } catch (error) {
        }
        if (typeof debug !== "function") {
          debug = function() {
          };
        }
      }
      debug.apply(null, arguments);
    };
  }
});

// node_modules/follow-redirects/index.js
var require_follow_redirects = __commonJS({
  "node_modules/follow-redirects/index.js"(exports, module) {
    var url2 = __require("url");
    var URL2 = url2.URL;
    var http3 = __require("http");
    var https2 = __require("https");
    var Writable = __require("stream").Writable;
    var assert = __require("assert");
    var debug = require_debug();
    (function detectUnsupportedEnvironment() {
      var looksLikeNode = typeof process !== "undefined";
      var looksLikeBrowser = typeof window !== "undefined" && typeof document !== "undefined";
      var looksLikeV8 = isFunction3(Error.captureStackTrace);
      if (!looksLikeNode && (looksLikeBrowser || !looksLikeV8)) {
        console.warn("The follow-redirects package should be excluded from browser builds.");
      }
    })();
    var useNativeURL = false;
    try {
      assert(new URL2(""));
    } catch (error) {
      useNativeURL = error.code === "ERR_INVALID_URL";
    }
    var sensitiveHeaders = [
      "Authorization",
      "Proxy-Authorization",
      "Cookie"
    ];
    var preservedUrlFields = [
      "auth",
      "host",
      "hostname",
      "href",
      "path",
      "pathname",
      "port",
      "protocol",
      "query",
      "search",
      "hash"
    ];
    var events = ["abort", "aborted", "connect", "error", "socket", "timeout"];
    var eventHandlers = /* @__PURE__ */ Object.create(null);
    events.forEach(function(event) {
      eventHandlers[event] = function(arg1, arg2, arg3) {
        this._redirectable.emit(event, arg1, arg2, arg3);
      };
    });
    var InvalidUrlError = createErrorType(
      "ERR_INVALID_URL",
      "Invalid URL",
      TypeError
    );
    var RedirectionError = createErrorType(
      "ERR_FR_REDIRECTION_FAILURE",
      "Redirected request failed"
    );
    var TooManyRedirectsError = createErrorType(
      "ERR_FR_TOO_MANY_REDIRECTS",
      "Maximum number of redirects exceeded",
      RedirectionError
    );
    var MaxBodyLengthExceededError = createErrorType(
      "ERR_FR_MAX_BODY_LENGTH_EXCEEDED",
      "Request body larger than maxBodyLength limit"
    );
    var WriteAfterEndError = createErrorType(
      "ERR_STREAM_WRITE_AFTER_END",
      "write after end"
    );
    var destroy = Writable.prototype.destroy || noop2;
    function RedirectableRequest(options, responseCallback) {
      Writable.call(this);
      this._sanitizeOptions(options);
      this._options = options;
      this._ended = false;
      this._ending = false;
      this._redirectCount = 0;
      this._redirects = [];
      this._requestBodyLength = 0;
      this._requestBodyBuffers = [];
      if (responseCallback) {
        this.on("response", responseCallback);
      }
      var self2 = this;
      this._onNativeResponse = function(response) {
        try {
          self2._processResponse(response);
        } catch (cause) {
          self2.emit("error", cause instanceof RedirectionError ? cause : new RedirectionError({ cause }));
        }
      };
      this._headerFilter = new RegExp("^(?:" + sensitiveHeaders.concat(options.sensitiveHeaders).map(escapeRegex).join("|") + ")$", "i");
      this._performRequest();
    }
    RedirectableRequest.prototype = Object.create(Writable.prototype);
    RedirectableRequest.prototype.abort = function() {
      destroyRequest(this._currentRequest);
      this._currentRequest.abort();
      this.emit("abort");
    };
    RedirectableRequest.prototype.destroy = function(error) {
      destroyRequest(this._currentRequest, error);
      destroy.call(this, error);
      return this;
    };
    RedirectableRequest.prototype.write = function(data, encoding, callback) {
      if (this._ending) {
        throw new WriteAfterEndError();
      }
      if (!isString2(data) && !isBuffer2(data)) {
        throw new TypeError("data should be a string, Buffer or Uint8Array");
      }
      if (isFunction3(encoding)) {
        callback = encoding;
        encoding = null;
      }
      if (data.length === 0) {
        if (callback) {
          callback();
        }
        return;
      }
      if (this._requestBodyLength + data.length <= this._options.maxBodyLength) {
        this._requestBodyLength += data.length;
        this._requestBodyBuffers.push({ data, encoding });
        this._currentRequest.write(data, encoding, callback);
      } else {
        this.emit("error", new MaxBodyLengthExceededError());
        this.abort();
      }
    };
    RedirectableRequest.prototype.end = function(data, encoding, callback) {
      if (isFunction3(data)) {
        callback = data;
        data = encoding = null;
      } else if (isFunction3(encoding)) {
        callback = encoding;
        encoding = null;
      }
      if (!data) {
        this._ended = this._ending = true;
        this._currentRequest.end(null, null, callback);
      } else {
        var self2 = this;
        var currentRequest = this._currentRequest;
        this.write(data, encoding, function() {
          self2._ended = true;
          currentRequest.end(null, null, callback);
        });
        this._ending = true;
      }
    };
    RedirectableRequest.prototype.setHeader = function(name, value) {
      this._options.headers[name] = value;
      this._currentRequest.setHeader(name, value);
    };
    RedirectableRequest.prototype.removeHeader = function(name) {
      delete this._options.headers[name];
      this._currentRequest.removeHeader(name);
    };
    RedirectableRequest.prototype.setTimeout = function(msecs, callback) {
      var self2 = this;
      function destroyOnTimeout(socket) {
        socket.setTimeout(msecs);
        socket.removeListener("timeout", socket.destroy);
        socket.addListener("timeout", socket.destroy);
      }
      function startTimer(socket) {
        if (self2._timeout) {
          clearTimeout(self2._timeout);
        }
        self2._timeout = setTimeout(function() {
          self2.emit("timeout");
          clearTimer();
        }, msecs);
        destroyOnTimeout(socket);
      }
      function clearTimer() {
        if (self2._timeout) {
          clearTimeout(self2._timeout);
          self2._timeout = null;
        }
        self2.removeListener("abort", clearTimer);
        self2.removeListener("error", clearTimer);
        self2.removeListener("response", clearTimer);
        self2.removeListener("close", clearTimer);
        if (callback) {
          self2.removeListener("timeout", callback);
        }
        if (!self2.socket) {
          self2._currentRequest.removeListener("socket", startTimer);
        }
      }
      if (callback) {
        this.on("timeout", callback);
      }
      if (this.socket) {
        startTimer(this.socket);
      } else {
        this._currentRequest.once("socket", startTimer);
      }
      this.on("socket", destroyOnTimeout);
      this.on("abort", clearTimer);
      this.on("error", clearTimer);
      this.on("response", clearTimer);
      this.on("close", clearTimer);
      return this;
    };
    [
      "flushHeaders",
      "getHeader",
      "setNoDelay",
      "setSocketKeepAlive"
    ].forEach(function(method) {
      RedirectableRequest.prototype[method] = function(a, b) {
        return this._currentRequest[method](a, b);
      };
    });
    ["aborted", "connection", "socket"].forEach(function(property) {
      Object.defineProperty(RedirectableRequest.prototype, property, {
        get: function() {
          return this._currentRequest[property];
        }
      });
    });
    RedirectableRequest.prototype._sanitizeOptions = function(options) {
      if (!options.headers) {
        options.headers = {};
      } else if (Array.isArray(options.headers)) {
        var headerList = options.headers;
        options.headers = {};
        for (var i = 0; i < headerList.length; i += 2) {
          options.headers[headerList[i]] = headerList[i + 1];
        }
      }
      if (!Array.isArray(options.sensitiveHeaders)) {
        options.sensitiveHeaders = [];
      }
      if (options.host) {
        if (!options.hostname) {
          options.hostname = options.host;
        }
        delete options.host;
      }
      if (!options.pathname && options.path) {
        var searchPos = options.path.indexOf("?");
        if (searchPos < 0) {
          options.pathname = options.path;
        } else {
          options.pathname = options.path.substring(0, searchPos);
          options.search = options.path.substring(searchPos);
        }
      }
    };
    RedirectableRequest.prototype._performRequest = function() {
      var protocol = this._options.protocol;
      var nativeProtocol = this._options.nativeProtocols[protocol];
      if (!nativeProtocol) {
        throw new TypeError("Unsupported protocol " + protocol);
      }
      if (this._options.agents) {
        var scheme = protocol.slice(0, -1);
        this._options.agent = this._options.agents[scheme];
      }
      var request = this._currentRequest = nativeProtocol.request(this._options, this._onNativeResponse);
      request._redirectable = this;
      for (var event of events) {
        request.on(event, eventHandlers[event]);
      }
      this._currentUrl = /^\//.test(this._options.path) ? url2.format(this._options) : (
        // When making a request to a proxy, […]
        // a client MUST send the target URI in absolute-form […].
        this._options.path
      );
      if (this._isRedirect) {
        var i = 0;
        var self2 = this;
        var buffers = this._requestBodyBuffers;
        (function writeNext(error) {
          if (request === self2._currentRequest) {
            if (error) {
              self2.emit("error", error);
            } else if (i < buffers.length) {
              var buffer = buffers[i++];
              if (!request.finished) {
                request.write(buffer.data, buffer.encoding, writeNext);
              }
            } else if (self2._ended) {
              request.end();
            }
          }
        })();
      }
    };
    RedirectableRequest.prototype._processResponse = function(response) {
      var statusCode = response.statusCode;
      if (this._options.trackRedirects) {
        this._redirects.push({
          url: this._currentUrl,
          headers: response.headers,
          statusCode
        });
      }
      var location = response.headers.location;
      if (!location || this._options.followRedirects === false || statusCode < 300 || statusCode >= 400) {
        response.responseUrl = this._currentUrl;
        response.redirects = this._redirects;
        this.emit("response", response);
        this._requestBodyBuffers = [];
        return;
      }
      destroyRequest(this._currentRequest);
      response.destroy();
      if (++this._redirectCount > this._options.maxRedirects) {
        throw new TooManyRedirectsError();
      }
      var requestHeaders;
      var beforeRedirect = this._options.beforeRedirect;
      if (beforeRedirect) {
        requestHeaders = Object.assign({
          // The Host header was set by nativeProtocol.request
          Host: response.req.getHeader("host")
        }, this._options.headers);
      }
      var method = this._options.method;
      if ((statusCode === 301 || statusCode === 302) && this._options.method === "POST" || // RFC7231§6.4.4: The 303 (See Other) status code indicates that
      // the server is redirecting the user agent to a different resource […]
      // A user agent can perform a retrieval request targeting that URI
      // (a GET or HEAD request if using HTTP) […]
      statusCode === 303 && !/^(?:GET|HEAD)$/.test(this._options.method)) {
        this._options.method = "GET";
        this._requestBodyBuffers = [];
        removeMatchingHeaders(/^content-/i, this._options.headers);
      }
      var currentHostHeader = removeMatchingHeaders(/^host$/i, this._options.headers);
      var currentUrlParts = parseUrl2(this._currentUrl);
      var currentHost = currentHostHeader || currentUrlParts.host;
      var currentUrl = /^\w+:/.test(location) ? this._currentUrl : url2.format(Object.assign(currentUrlParts, { host: currentHost }));
      var redirectUrl = resolveUrl(location, currentUrl);
      debug("redirecting to", redirectUrl.href);
      this._isRedirect = true;
      spreadUrlObject(redirectUrl, this._options);
      if (redirectUrl.protocol !== currentUrlParts.protocol && redirectUrl.protocol !== "https:" || redirectUrl.host !== currentHost && !isSubdomain(redirectUrl.host, currentHost)) {
        removeMatchingHeaders(this._headerFilter, this._options.headers);
      }
      if (isFunction3(beforeRedirect)) {
        var responseDetails = {
          headers: response.headers,
          statusCode
        };
        var requestDetails = {
          url: currentUrl,
          method,
          headers: requestHeaders
        };
        beforeRedirect(this._options, responseDetails, requestDetails);
        this._sanitizeOptions(this._options);
      }
      this._performRequest();
    };
    function wrap(protocols) {
      var exports2 = {
        maxRedirects: 21,
        maxBodyLength: 10 * 1024 * 1024
      };
      var nativeProtocols = {};
      Object.keys(protocols).forEach(function(scheme) {
        var protocol = scheme + ":";
        var nativeProtocol = nativeProtocols[protocol] = protocols[scheme];
        var wrappedProtocol = exports2[scheme] = Object.create(nativeProtocol);
        function request(input, options, callback) {
          if (isURL(input)) {
            input = spreadUrlObject(input);
          } else if (isString2(input)) {
            input = spreadUrlObject(parseUrl2(input));
          } else {
            callback = options;
            options = validateUrl(input);
            input = { protocol };
          }
          if (isFunction3(options)) {
            callback = options;
            options = null;
          }
          options = Object.assign({
            maxRedirects: exports2.maxRedirects,
            maxBodyLength: exports2.maxBodyLength
          }, input, options);
          options.nativeProtocols = nativeProtocols;
          if (!isString2(options.host) && !isString2(options.hostname)) {
            options.hostname = "::1";
          }
          assert.equal(options.protocol, protocol, "protocol mismatch");
          debug("options", options);
          return new RedirectableRequest(options, callback);
        }
        function get(input, options, callback) {
          var wrappedRequest = wrappedProtocol.request(input, options, callback);
          wrappedRequest.end();
          return wrappedRequest;
        }
        Object.defineProperties(wrappedProtocol, {
          request: { value: request, configurable: true, enumerable: true, writable: true },
          get: { value: get, configurable: true, enumerable: true, writable: true }
        });
      });
      return exports2;
    }
    function noop2() {
    }
    function parseUrl2(input) {
      var parsed;
      if (useNativeURL) {
        parsed = new URL2(input);
      } else {
        parsed = validateUrl(url2.parse(input));
        if (!isString2(parsed.protocol)) {
          throw new InvalidUrlError({ input });
        }
      }
      return parsed;
    }
    function resolveUrl(relative, base) {
      return useNativeURL ? new URL2(relative, base) : parseUrl2(url2.resolve(base, relative));
    }
    function validateUrl(input) {
      if (/^\[/.test(input.hostname) && !/^\[[:0-9a-f]+\]$/i.test(input.hostname)) {
        throw new InvalidUrlError({ input: input.href || input });
      }
      if (/^\[/.test(input.host) && !/^\[[:0-9a-f]+\](:\d+)?$/i.test(input.host)) {
        throw new InvalidUrlError({ input: input.href || input });
      }
      return input;
    }
    function spreadUrlObject(urlObject, target) {
      var spread3 = target || {};
      for (var key of preservedUrlFields) {
        spread3[key] = urlObject[key];
      }
      if (spread3.hostname.startsWith("[")) {
        spread3.hostname = spread3.hostname.slice(1, -1);
      }
      if (spread3.port !== "") {
        spread3.port = Number(spread3.port);
      }
      spread3.path = spread3.search ? spread3.pathname + spread3.search : spread3.pathname;
      return spread3;
    }
    function removeMatchingHeaders(regex, headers) {
      var lastValue;
      for (var header in headers) {
        if (regex.test(header)) {
          lastValue = headers[header];
          delete headers[header];
        }
      }
      return lastValue === null || typeof lastValue === "undefined" ? void 0 : String(lastValue).trim();
    }
    function createErrorType(code, message, baseClass) {
      function CustomError(properties) {
        if (isFunction3(Error.captureStackTrace)) {
          Error.captureStackTrace(this, this.constructor);
        }
        Object.assign(this, properties || {});
        this.code = code;
        this.message = this.cause ? message + ": " + this.cause.message : message;
      }
      CustomError.prototype = new (baseClass || Error)();
      Object.defineProperties(CustomError.prototype, {
        constructor: {
          value: CustomError,
          enumerable: false
        },
        name: {
          value: "Error [" + code + "]",
          enumerable: false
        }
      });
      return CustomError;
    }
    function destroyRequest(request, error) {
      for (var event of events) {
        request.removeListener(event, eventHandlers[event]);
      }
      request.on("error", noop2);
      request.destroy(error);
    }
    function isSubdomain(subdomain, domain) {
      assert(isString2(subdomain) && isString2(domain));
      var dot = subdomain.length - domain.length - 1;
      return dot > 0 && subdomain[dot] === "." && subdomain.endsWith(domain);
    }
    function isString2(value) {
      return typeof value === "string" || value instanceof String;
    }
    function isFunction3(value) {
      return typeof value === "function";
    }
    function isBuffer2(value) {
      return typeof value === "object" && "length" in value;
    }
    function isURL(value) {
      return URL2 && value instanceof URL2;
    }
    function escapeRegex(regex) {
      return regex.replace(/[[\]\\/()*+?.^$|]/g, "\\$&");
    }
    module.exports = wrap({ http: http3, https: https2 });
    module.exports.wrap = wrap;
  }
});

// node_modules/axios/lib/env/data.js
var VERSION;
var init_data = __esm({
  "node_modules/axios/lib/env/data.js"() {
    VERSION = "1.21.0";
  }
});

// node_modules/axios/lib/helpers/parseProtocol.js
function parseProtocol(url2) {
  const match = /^([-+\w]{1,25}):(?:\/\/)?/.exec(url2);
  return match && match[1].toLowerCase() || "";
}
var init_parseProtocol = __esm({
  "node_modules/axios/lib/helpers/parseProtocol.js"() {
    "use strict";
  }
});

// node_modules/axios/lib/helpers/fromDataURI.js
function fromDataURI(uri, asBlob, options) {
  const _Blob = options && options.Blob || platform_default.classes.Blob;
  const protocol = parseProtocol(uri);
  if (asBlob === void 0 && _Blob) {
    asBlob = true;
  }
  if (protocol === "data") {
    uri = protocol.length ? uri.slice(protocol.length + 1) : uri;
    const match = DATA_URL_PATTERN.exec(uri);
    if (!match) {
      throw new AxiosError_default("Invalid URL", AxiosError_default.ERR_INVALID_URL);
    }
    const type = match[1];
    const params = match[2];
    const encoding = match[3] ? "base64" : "utf8";
    const body = match[4];
    let mime = "";
    if (type) {
      mime = params ? type + params : type;
    } else if (params) {
      mime = "text/plain" + params;
    }
    const buffer = encoding === "base64" ? Buffer.from(
      body.replace(/%([\da-f]{2})/gi, (_, hex) => String.fromCharCode(parseInt(hex, 16))),
      "base64"
    ) : Buffer.from(decodeURIComponent(body), encoding);
    if (asBlob) {
      if (!_Blob) {
        throw new AxiosError_default("Blob is not supported", AxiosError_default.ERR_NOT_SUPPORT);
      }
      return new _Blob([buffer], { type: mime });
    }
    return buffer;
  }
  throw new AxiosError_default("Unsupported protocol " + protocol, AxiosError_default.ERR_NOT_SUPPORT);
}
var DATA_URL_PATTERN;
var init_fromDataURI = __esm({
  "node_modules/axios/lib/helpers/fromDataURI.js"() {
    "use strict";
    init_AxiosError();
    init_parseProtocol();
    init_platform();
    DATA_URL_PATTERN = /^([^,;/]+\/[^,;/]+)?((?:;[^,;=]+=[^,;]+)*)(;base64)?,([\s\S]*)$/i;
  }
});

// node_modules/axios/lib/core/setFormDataHeaders.js
function setFormDataHeaders(headers, formHeaders, policy) {
  if (policy !== "content-only") {
    headers.set(formHeaders);
    return;
  }
  Object.entries(formHeaders || {}).forEach(([key, val]) => {
    if (FORM_DATA_CONTENT_HEADERS.includes(key.toLowerCase())) {
      headers.set(key, val);
    }
  });
}
var FORM_DATA_CONTENT_HEADERS;
var init_setFormDataHeaders = __esm({
  "node_modules/axios/lib/core/setFormDataHeaders.js"() {
    "use strict";
    FORM_DATA_CONTENT_HEADERS = ["content-type", "content-length"];
  }
});

// node_modules/axios/lib/helpers/AxiosTransformStream.js
import stream from "stream";
var kInternals, AxiosTransformStream, AxiosTransformStream_default;
var init_AxiosTransformStream = __esm({
  "node_modules/axios/lib/helpers/AxiosTransformStream.js"() {
    "use strict";
    init_utils();
    kInternals = /* @__PURE__ */ Symbol("internals");
    AxiosTransformStream = class extends stream.Transform {
      constructor(options) {
        options = utils_default.toFlatObject(
          options,
          {
            maxRate: 0,
            chunkSize: 64 * 1024,
            minChunkSize: 100,
            timeWindow: 500,
            ticksRate: 2,
            samplesCount: 15
          },
          null,
          (prop, source) => {
            return !utils_default.isUndefined(source[prop]);
          }
        );
        super({
          readableHighWaterMark: options.chunkSize
        });
        const internals = this[kInternals] = {
          timeWindow: options.timeWindow,
          chunkSize: options.chunkSize,
          maxRate: options.maxRate,
          minChunkSize: options.minChunkSize,
          bytesSeen: 0,
          isCaptured: false,
          notifiedBytesLoaded: 0,
          ts: Date.now(),
          bytes: 0,
          onReadCallback: null
        };
        this.on("newListener", (event) => {
          if (event === "progress") {
            if (!internals.isCaptured) {
              internals.isCaptured = true;
            }
          }
        });
      }
      _read(size) {
        const internals = this[kInternals];
        if (internals.onReadCallback) {
          internals.onReadCallback();
        }
        return super._read(size);
      }
      _transform(chunk, encoding, callback) {
        const internals = this[kInternals];
        const maxRate = internals.maxRate;
        const readableHighWaterMark = this.readableHighWaterMark;
        const timeWindow = internals.timeWindow;
        const divider = 1e3 / timeWindow;
        const bytesThreshold = maxRate / divider;
        let minChunkSize = internals.minChunkSize !== false ? Math.max(internals.minChunkSize, bytesThreshold * 0.01) : 0;
        if (maxRate) {
          minChunkSize = Math.min(bytesThreshold, minChunkSize);
        }
        const pushChunk = (_chunk, _callback) => {
          const bytes = Buffer.byteLength(_chunk);
          internals.bytesSeen += bytes;
          internals.bytes += bytes;
          internals.isCaptured && this.emit("progress", internals.bytesSeen);
          if (this.push(_chunk)) {
            process.nextTick(_callback);
          } else {
            internals.onReadCallback = () => {
              internals.onReadCallback = null;
              process.nextTick(_callback);
            };
          }
        };
        const transformChunk = (_chunk, _callback) => {
          if (this.destroyed) {
            return _callback();
          }
          const chunkSize = Buffer.byteLength(_chunk);
          let chunkRemainder = null;
          let maxChunkSize = readableHighWaterMark;
          let bytesLeft;
          let passed = 0;
          if (maxRate) {
            const now = Date.now();
            if (!internals.ts || (passed = now - internals.ts) >= timeWindow) {
              internals.ts = now;
              bytesLeft = bytesThreshold - internals.bytes;
              internals.bytes = bytesLeft < 0 ? -bytesLeft : 0;
              passed = 0;
            }
            bytesLeft = bytesThreshold - internals.bytes;
          }
          if (maxRate) {
            if (bytesLeft <= 0) {
              return setTimeout(() => {
                _callback(null, _chunk);
              }, timeWindow - passed);
            }
            if (bytesLeft < maxChunkSize) {
              maxChunkSize = Math.max(1, Math.floor(bytesLeft));
            }
          }
          if (maxChunkSize && chunkSize > maxChunkSize && chunkSize - maxChunkSize > minChunkSize) {
            chunkRemainder = _chunk.subarray(maxChunkSize);
            _chunk = _chunk.subarray(0, maxChunkSize);
          }
          pushChunk(
            _chunk,
            chunkRemainder ? () => {
              process.nextTick(_callback, null, chunkRemainder);
            } : _callback
          );
        };
        transformChunk(chunk, function transformNextChunk(err, _chunk) {
          if (err) {
            return callback(err);
          }
          if (_chunk) {
            transformChunk(_chunk, transformNextChunk);
          } else {
            callback(null);
          }
        });
      }
    };
    AxiosTransformStream_default = AxiosTransformStream;
  }
});

// node_modules/axios/lib/helpers/readBlob.js
var asyncIterator, readBlob, readBlob_default;
var init_readBlob = __esm({
  "node_modules/axios/lib/helpers/readBlob.js"() {
    ({ asyncIterator } = Symbol);
    readBlob = async function* (blob) {
      if (blob.stream) {
        yield* blob.stream();
      } else if (blob.arrayBuffer) {
        yield await blob.arrayBuffer();
      } else if (blob[asyncIterator]) {
        yield* blob[asyncIterator]();
      } else {
        yield blob;
      }
    };
    readBlob_default = readBlob;
  }
});

// node_modules/axios/lib/helpers/formDataToStream.js
import util from "util";
import { Readable } from "stream";
var BOUNDARY_ALPHABET, textEncoder, CRLF, CRLF_BYTES, CRLF_BYTES_COUNT, FormDataPart, formDataToStream, formDataToStream_default;
var init_formDataToStream = __esm({
  "node_modules/axios/lib/helpers/formDataToStream.js"() {
    init_utils();
    init_readBlob();
    init_platform();
    BOUNDARY_ALPHABET = platform_default.ALPHABET.ALPHA_DIGIT + "-_";
    textEncoder = typeof TextEncoder === "function" ? new TextEncoder() : new util.TextEncoder();
    CRLF = "\r\n";
    CRLF_BYTES = textEncoder.encode(CRLF);
    CRLF_BYTES_COUNT = 2;
    FormDataPart = class {
      constructor(name, value) {
        const { escapeName: escapeName2 } = this.constructor;
        const isStringValue = utils_default.isString(value);
        let headers = `Content-Disposition: form-data; name="${escapeName2(name)}"${!isStringValue && value.name ? `; filename="${escapeName2(value.name)}"` : ""}${CRLF}`;
        if (isStringValue) {
          value = textEncoder.encode(String(value).replace(/\r?\n|\r\n?/g, CRLF));
        } else {
          const safeType = String(value.type || "application/octet-stream").replace(/[\r\n]/g, "");
          headers += `Content-Type: ${safeType}${CRLF}`;
        }
        this.headers = textEncoder.encode(headers + CRLF);
        this.contentLength = isStringValue ? value.byteLength : value.size;
        this.size = this.headers.byteLength + this.contentLength + CRLF_BYTES_COUNT;
        this.name = name;
        this.value = value;
      }
      async *encode() {
        yield this.headers;
        const { value } = this;
        if (utils_default.isTypedArray(value)) {
          yield value;
        } else {
          yield* readBlob_default(value);
        }
        yield CRLF_BYTES;
      }
      static escapeName(name) {
        return String(name).replace(
          /[\r\n"]/g,
          (match) => ({
            "\r": "%0D",
            "\n": "%0A",
            '"': "%22"
          })[match]
        );
      }
    };
    formDataToStream = (form, headersHandler, options) => {
      const {
        tag = "form-data-boundary",
        size = 25,
        boundary = tag + "-" + platform_default.generateString(size, BOUNDARY_ALPHABET)
      } = options || {};
      if (!utils_default.isFormData(form)) {
        throw new TypeError("FormData instance required");
      }
      if (boundary.length < 1 || boundary.length > 70) {
        throw new Error("boundary must be 1-70 characters long");
      }
      const boundaryBytes = textEncoder.encode("--" + boundary + CRLF);
      const footerBytes = textEncoder.encode("--" + boundary + "--" + CRLF);
      let contentLength = footerBytes.byteLength;
      const parts = Array.from(form.entries()).map(([name, value]) => {
        const part = new FormDataPart(name, value);
        contentLength += part.size;
        return part;
      });
      contentLength += boundaryBytes.byteLength * parts.length;
      contentLength = utils_default.toFiniteNumber(contentLength);
      const computedHeaders = {
        "Content-Type": `multipart/form-data; boundary=${boundary}`
      };
      if (Number.isFinite(contentLength)) {
        computedHeaders["Content-Length"] = contentLength;
      }
      headersHandler && headersHandler(computedHeaders);
      return Readable.from(
        (async function* () {
          for (const part of parts) {
            yield boundaryBytes;
            yield* part.encode();
          }
          yield footerBytes;
        })()
      );
    };
    formDataToStream_default = formDataToStream;
  }
});

// node_modules/axios/lib/helpers/ZlibHeaderTransformStream.js
import stream2 from "stream";
var ZlibHeaderTransformStream, ZlibHeaderTransformStream_default;
var init_ZlibHeaderTransformStream = __esm({
  "node_modules/axios/lib/helpers/ZlibHeaderTransformStream.js"() {
    "use strict";
    ZlibHeaderTransformStream = class extends stream2.Transform {
      __transform(chunk, encoding, callback) {
        this.push(chunk);
        callback();
      }
      _transform(chunk, encoding, callback) {
        if (chunk.length !== 0) {
          this._transform = this.__transform;
          if (chunk[0] !== 120) {
            const header = Buffer.alloc(2);
            header[0] = 120;
            header[1] = 156;
            this.push(header, encoding);
          }
        }
        this.__transform(chunk, encoding, callback);
      }
    };
    ZlibHeaderTransformStream_default = ZlibHeaderTransformStream;
  }
});

// node_modules/axios/lib/helpers/Http2Sessions.js
import http2 from "http2";
import util2 from "util";
var Http2Sessions, Http2Sessions_default;
var init_Http2Sessions = __esm({
  "node_modules/axios/lib/helpers/Http2Sessions.js"() {
    "use strict";
    Http2Sessions = class {
      constructor() {
        this.sessions = /* @__PURE__ */ Object.create(null);
      }
      getSession(authority, options) {
        options = Object.assign(
          /* @__PURE__ */ Object.create(null),
          {
            sessionTimeout: 1e3
          },
          options
        );
        let authoritySessions = this.sessions[authority];
        if (authoritySessions) {
          let len = authoritySessions.length;
          for (let i = 0; i < len; i++) {
            const [sessionHandle, sessionOptions] = authoritySessions[i];
            if (!sessionHandle.destroyed && !sessionHandle.closed && util2.isDeepStrictEqual(sessionOptions, options)) {
              return sessionHandle;
            }
          }
        }
        const session = http2.connect(authority, options);
        let removed;
        let timer;
        const removeSession = () => {
          if (removed) {
            return;
          }
          removed = true;
          if (timer) {
            clearTimeout(timer);
            timer = null;
          }
          let entries = authoritySessions, len = entries.length, i = len;
          while (i--) {
            if (entries[i][0] === session) {
              if (len === 1) {
                delete this.sessions[authority];
              } else {
                entries.splice(i, 1);
              }
              if (!session.closed) {
                session.close();
              }
              return;
            }
          }
        };
        const originalRequestFn = session.request;
        const { sessionTimeout } = options;
        if (sessionTimeout != null) {
          let streamsCount = 0;
          session.request = function() {
            const stream4 = originalRequestFn.apply(this, arguments);
            streamsCount++;
            if (timer) {
              clearTimeout(timer);
              timer = null;
            }
            stream4.once("close", () => {
              if (!--streamsCount && !removed) {
                timer = setTimeout(() => {
                  timer = null;
                  removeSession();
                }, sessionTimeout);
              }
            });
            return stream4;
          };
        }
        session.once("close", removeSession);
        session.once("error", removeSession);
        session.once("goaway", removeSession);
        let entry = [session, options];
        authoritySessions ? authoritySessions.push(entry) : authoritySessions = this.sessions[authority] = [entry];
        return session;
      }
    };
    Http2Sessions_default = Http2Sessions;
  }
});

// node_modules/axios/lib/helpers/callbackify.js
var callbackify, callbackify_default;
var init_callbackify = __esm({
  "node_modules/axios/lib/helpers/callbackify.js"() {
    init_utils();
    callbackify = (fn, reducer) => {
      return utils_default.isAsyncFn(fn) ? function(...args) {
        const cb = args.pop();
        fn.apply(this, args).then((value) => {
          try {
            reducer ? cb(null, ...reducer(value)) : cb(null, value);
          } catch (err) {
            cb(err);
          }
        }, cb);
      } : fn;
    };
    callbackify_default = callbackify;
  }
});

// node_modules/axios/lib/helpers/shouldBypassProxy.js
import { domainToASCII } from "url";
function shouldBypassProxy(location) {
  let parsed;
  try {
    parsed = new URL(location);
  } catch (_err) {
    return false;
  }
  const noProxy = (process.env.no_proxy || process.env.NO_PROXY || "").toLowerCase();
  if (!noProxy) {
    return false;
  }
  if (noProxy === "*") {
    return true;
  }
  const port = Number.parseInt(parsed.port, 10) || DEFAULT_PORTS2[parsed.protocol.split(":", 1)[0]] || 0;
  const hostname = normalizeNoProxyHost(parsed.hostname.toLowerCase());
  const hostnameBytes = ipToBytes(hostname);
  return noProxy.split(/[\s,]+/).some((entry) => {
    if (!entry) {
      return false;
    }
    if (entry === "*") {
      return true;
    }
    const cidr = parseCidrEntry(entry);
    if (cidr !== void 0) {
      return cidr !== null && !!hostnameBytes && hostnameBytes.length === cidr.bytes.length && isInSubnet(hostnameBytes, cidr.bytes, cidr.prefix);
    }
    let [entryHost, entryPort] = parseNoProxyEntry(entry);
    entryHost = normalizeNoProxyHost(entryHost);
    if (!entryHost) {
      return false;
    }
    if (entryPort && entryPort !== port) {
      return false;
    }
    if (entryHost.charAt(0) === "*") {
      entryHost = entryHost.slice(1);
    }
    if (entryHost.charAt(0) === ".") {
      return hostname.endsWith(entryHost);
    }
    return hostname === entryHost || isLoopback(hostname) && isLoopback(entryHost);
  });
}
var LOOPBACK_HOSTNAMES, trimTrailingDots, isIPv4Loopback, parseIPv4Octet, normalizeIPAddress, isIPv6ZeroGroup, isIPv6Unspecified, isIPv6Loopback, isLoopback, DEFAULT_PORTS2, parseNoProxyEntry, IPV4_MAPPED_DOTTED_RE, IPV4_MAPPED_HEX_RE, unmapIPv4MappedIPv6, IPV4_OCTET_RE, ipv4ToBytes, IPV6_GROUP_RE, ipv6ToBytes, ipToBytes, normalizeNoProxyHost, normalizeCidrBase, CIDR_ENTRY_RE, parseCidrEntry, isInSubnet;
var init_shouldBypassProxy = __esm({
  "node_modules/axios/lib/helpers/shouldBypassProxy.js"() {
    LOOPBACK_HOSTNAMES = /* @__PURE__ */ new Set(["localhost", "0.0.0.0"]);
    trimTrailingDots = (value) => {
      let end = value.length;
      while (end && value.charCodeAt(end - 1) === 46) {
        end--;
      }
      return end === value.length ? value : value.slice(0, end);
    };
    isIPv4Loopback = (host) => {
      const parts = host.split(".");
      if (parts.length !== 4) return false;
      if (parts[0] !== "127") return false;
      return parts.every((p) => /^\d+$/.test(p) && Number(p) >= 0 && Number(p) <= 255);
    };
    parseIPv4Octet = (text) => {
      if (/^0[xX][0-9a-fA-F]+$/.test(text)) {
        const n = parseInt(text.slice(2), 16);
        return Number.isFinite(n) ? n : null;
      }
      if (text.length > 1 && /^0[0-7]+$/.test(text)) {
        const n = parseInt(text, 8);
        return Number.isFinite(n) ? n : null;
      }
      if (text.length > 1 && /^0[0-9]+$/.test(text)) {
        return null;
      }
      if (/^[0-9]+$/.test(text)) {
        const n = parseInt(text, 10);
        return Number.isFinite(n) ? n : null;
      }
      return null;
    };
    normalizeIPAddress = (host) => {
      if (typeof host !== "string" || !host || host.indexOf(":") !== -1) {
        return host;
      }
      let h = host;
      if (h.charAt(0) === "[" && h.charAt(h.length - 1) === "]") {
        h = h.slice(1, -1);
      }
      h = trimTrailingDots(h);
      if (!/^[0-9.xXa-fA-F]+$/.test(h)) return host;
      const parts = h.split(".");
      if (parts.some((p) => p === "")) return host;
      if (parts.length === 4) {
        const octets = parts.map(parseIPv4Octet);
        if (octets.some((n) => n === null || n < 0 || n > 255)) return host;
        return octets.join(".");
      }
      if (parts.length > 4) {
        return host;
      }
      if (parts.length === 1) return host;
      const literalOctets = parts.slice(0, -1);
      const tail = parts[parts.length - 1];
      const tailSlots = 4 - literalOctets.length;
      const tailValue = parseIPv4Octet(tail);
      if (tailValue === null) return host;
      const maxTail = (1 << 8 * tailSlots) - 1;
      if (tailValue < 0 || tailValue > maxTail) return host;
      const tailOctets = new Array(tailSlots).fill(0);
      for (let i = tailSlots - 1, v = tailValue; i >= 0; i--, v >>= 8) {
        tailOctets[i] = v & 255;
      }
      const literal = literalOctets.map(parseIPv4Octet);
      if (literal.some((n) => n === null || n < 0 || n > 255)) return host;
      return [...literal, ...tailOctets].join(".");
    };
    isIPv6ZeroGroup = (group) => /^0{1,4}$/.test(group);
    isIPv6Unspecified = (host) => {
      if (host === "::") return true;
      const compressionIndex = host.indexOf("::");
      if (compressionIndex !== -1) {
        if (compressionIndex !== host.lastIndexOf("::")) return false;
        const left = host.slice(0, compressionIndex);
        const right = host.slice(compressionIndex + 2);
        const leftGroups = left ? left.split(":") : [];
        const rightGroups = right ? right.split(":") : [];
        const explicitGroups = leftGroups.length + rightGroups.length;
        return explicitGroups < 8 && leftGroups.every(isIPv6ZeroGroup) && rightGroups.every(isIPv6ZeroGroup);
      }
      const groups = host.split(":");
      return groups.length === 8 && groups.every(isIPv6ZeroGroup);
    };
    isIPv6Loopback = (host) => {
      if (host === "::1") return true;
      const v4MappedDotted = host.match(/^::ffff:(\d+\.\d+\.\d+\.\d+)$/i);
      if (v4MappedDotted) return isIPv4Loopback(v4MappedDotted[1]);
      const v4MappedHex = host.match(/^::ffff:([0-9a-f]{1,4}):([0-9a-f]{1,4})$/i);
      if (v4MappedHex) {
        const high = parseInt(v4MappedHex[1], 16);
        return high >= 32512 && high <= 32767;
      }
      const groups = host.split(":");
      if (groups.length === 8) {
        for (let i = 0; i < 7; i++) {
          if (!/^0+$/.test(groups[i])) return false;
        }
        return /^0*1$/.test(groups[7]);
      }
      return false;
    };
    isLoopback = (host) => {
      if (!host) return false;
      if (LOOPBACK_HOSTNAMES.has(host)) return true;
      if (isIPv4Loopback(host)) return true;
      if (isIPv6Unspecified(host)) return true;
      return isIPv6Loopback(host);
    };
    DEFAULT_PORTS2 = {
      http: 80,
      https: 443,
      ws: 80,
      wss: 443,
      ftp: 21
    };
    parseNoProxyEntry = (entry) => {
      let entryHost = entry;
      let entryPort = 0;
      if (entryHost.charAt(0) === "[") {
        const bracketIndex = entryHost.indexOf("]");
        if (bracketIndex !== -1) {
          const host = entryHost.slice(1, bracketIndex);
          const rest = entryHost.slice(bracketIndex + 1);
          if (rest.charAt(0) === ":" && /^\d+$/.test(rest.slice(1))) {
            entryPort = Number.parseInt(rest.slice(1), 10);
          }
          return [host, entryPort];
        }
      }
      const firstColon = entryHost.indexOf(":");
      const lastColon = entryHost.lastIndexOf(":");
      if (firstColon !== -1 && firstColon === lastColon && /^\d+$/.test(entryHost.slice(lastColon + 1))) {
        entryPort = Number.parseInt(entryHost.slice(lastColon + 1), 10);
        entryHost = entryHost.slice(0, lastColon);
      }
      return [entryHost, entryPort];
    };
    IPV4_MAPPED_DOTTED_RE = /^(?:::|(?:0{1,4}:){1,4}:|(?:0{1,4}:){5})ffff:(\d+\.\d+\.\d+\.\d+)$/i;
    IPV4_MAPPED_HEX_RE = /^(?:::|(?:0{1,4}:){1,4}:|(?:0{1,4}:){5})ffff:([0-9a-f]{1,4}):([0-9a-f]{1,4})$/i;
    unmapIPv4MappedIPv6 = (host) => {
      if (typeof host !== "string" || host.indexOf(":") === -1) return host;
      const dotted = host.match(IPV4_MAPPED_DOTTED_RE);
      if (dotted) return dotted[1];
      const hex = host.match(IPV4_MAPPED_HEX_RE);
      if (hex) {
        const high = parseInt(hex[1], 16);
        const low = parseInt(hex[2], 16);
        return `${high >> 8}.${high & 255}.${low >> 8}.${low & 255}`;
      }
      return host;
    };
    IPV4_OCTET_RE = /^(?:0|[1-9]\d{0,2})$/;
    ipv4ToBytes = (host) => {
      const parts = host.split(".");
      return parts.length === 4 && parts.every((part) => IPV4_OCTET_RE.test(part) && Number(part) <= 255) ? parts.map(Number) : null;
    };
    IPV6_GROUP_RE = /^[0-9a-f]{1,4}$/i;
    ipv6ToBytes = (host) => {
      const halves = host.split("::");
      if (halves.length > 2) {
        return null;
      }
      const groups = halves[0] ? halves[0].split(":") : [];
      if (halves.length === 2) {
        const rear = halves[1] ? halves[1].split(":") : [];
        const missing = 8 - groups.length - rear.length;
        if (missing < 1) {
          return null;
        }
        groups.push(...new Array(missing).fill("0"), ...rear);
      }
      if (groups.length !== 8 || groups.some((group) => !IPV6_GROUP_RE.test(group))) {
        return null;
      }
      return groups.flatMap((group) => {
        const value = Number.parseInt(group, 16);
        return [value >> 8 & 255, value & 255];
      });
    };
    ipToBytes = (host) => {
      if (typeof host !== "string" || !host) {
        return null;
      }
      return host.indexOf(":") !== -1 ? ipv6ToBytes(host) : ipv4ToBytes(host);
    };
    normalizeNoProxyHost = (hostname) => {
      if (!hostname) {
        return hostname;
      }
      if (hostname.charAt(0) === "[" && hostname.charAt(hostname.length - 1) === "]") {
        hostname = hostname.slice(1, -1);
      }
      let trimmed = trimTrailingDots(hostname);
      if (trimmed.indexOf(":") !== -1) {
        try {
          trimmed = new URL("http://[" + trimmed + "]/").hostname.slice(1, -1);
        } catch (_err) {
          return trimmed;
        }
      } else if (/[\u0080-\uffff]/.test(trimmed)) {
        const prefix = /^[*.]+/.exec(trimmed);
        const start = prefix ? prefix[0] : "";
        const ascii = domainToASCII(trimmed.slice(start.length));
        if (!ascii) return "";
        trimmed = start + trimTrailingDots(ascii);
      }
      const ipv4 = normalizeIPAddress(trimmed);
      if (ipv4 !== trimmed) {
        return ipv4;
      }
      return unmapIPv4MappedIPv6(trimmed);
    };
    normalizeCidrBase = (input) => {
      let base = input;
      const startsBracket = base.charAt(0) === "[";
      const endsBracket = base.charAt(base.length - 1) === "]";
      const hasBracket = base.includes("[") || base.includes("]");
      if (startsBracket || endsBracket) {
        if (!startsBracket || !endsBracket) {
          return null;
        }
        base = base.slice(1, -1);
        if (base.indexOf(":") === -1 || base.includes("[") || base.includes("]")) {
          return null;
        }
      } else if (hasBracket) {
        return null;
      }
      if (!base || base.charAt(base.length - 1) === ".") {
        return null;
      }
      const wasIPv6 = base.indexOf(":") !== -1;
      if (wasIPv6) {
        try {
          base = new URL(`http://[${base}]/`).hostname.slice(1, -1);
        } catch (_err) {
          return null;
        }
      } else {
        base = normalizeIPAddress(base);
        if (!ipv4ToBytes(base)) {
          return null;
        }
      }
      return { normalized: unmapIPv4MappedIPv6(base), wasIPv6 };
    };
    CIDR_ENTRY_RE = /^(.+)\/(0|[1-9]\d{0,2})$/;
    parseCidrEntry = (entry) => {
      if (entry.indexOf("/") === -1) {
        return void 0;
      }
      const match = CIDR_ENTRY_RE.exec(entry);
      if (!match) {
        return null;
      }
      let prefix = Number(match[2]);
      const parsedBase = normalizeCidrBase(match[1]);
      if (!parsedBase) {
        return null;
      }
      const { normalized, wasIPv6 } = parsedBase;
      if (wasIPv6 && normalized.indexOf(":") === -1) {
        if (prefix < 96) {
          return null;
        }
        prefix -= 96;
      }
      const bytes = ipToBytes(normalized);
      if (!bytes || prefix > bytes.length * 8) {
        return null;
      }
      return { bytes, prefix };
    };
    isInSubnet = (addressBytes, networkBytes, prefix) => {
      const fullBytes = prefix >> 3;
      for (let i = 0; i < fullBytes; i++) {
        if (addressBytes[i] !== networkBytes[i]) {
          return false;
        }
      }
      const remainingBits = prefix & 7;
      if (remainingBits) {
        const mask = 255 << 8 - remainingBits & 255;
        if ((addressBytes[fullBytes] & mask) !== (networkBytes[fullBytes] & mask)) {
          return false;
        }
      }
      return true;
    };
  }
});

// node_modules/axios/lib/helpers/speedometer.js
function speedometer(samplesCount, min) {
  samplesCount = samplesCount || 10;
  const bytes = new Array(samplesCount);
  const timestamps = new Array(samplesCount);
  let head = 0;
  let tail = 0;
  let firstSampleTS;
  min = min !== void 0 ? min : 1e3;
  return function push(chunkLength) {
    const now = Date.now();
    const startedAt = timestamps[tail];
    if (!firstSampleTS) {
      firstSampleTS = now;
    }
    bytes[head] = chunkLength;
    timestamps[head] = now;
    let i = tail;
    let bytesCount = 0;
    while (i !== head) {
      bytesCount += bytes[i++];
      i = i % samplesCount;
    }
    head = (head + 1) % samplesCount;
    if (head === tail) {
      tail = (tail + 1) % samplesCount;
    }
    if (now - firstSampleTS < min) {
      return;
    }
    const passed = startedAt && now - startedAt;
    return passed ? Math.round(bytesCount * 1e3 / passed) : void 0;
  };
}
var speedometer_default;
var init_speedometer = __esm({
  "node_modules/axios/lib/helpers/speedometer.js"() {
    "use strict";
    speedometer_default = speedometer;
  }
});

// node_modules/axios/lib/helpers/throttle.js
function throttle(fn, freq) {
  let timestamp = 0;
  let threshold = 1e3 / freq;
  let lastArgs;
  let timer;
  const invoke = (args, now = Date.now()) => {
    timestamp = now;
    lastArgs = null;
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
    fn(...args);
  };
  const throttled = (...args) => {
    const now = Date.now();
    const passed = now - timestamp;
    if (passed >= threshold) {
      invoke(args, now);
    } else {
      lastArgs = args;
      if (!timer) {
        timer = setTimeout(() => {
          timer = null;
          invoke(lastArgs);
        }, threshold - passed);
      }
    }
  };
  const flush = () => lastArgs && invoke(lastArgs);
  const flushWith = (...args) => invoke(args);
  return [throttled, flush, flushWith];
}
var throttle_default;
var init_throttle = __esm({
  "node_modules/axios/lib/helpers/throttle.js"() {
    throttle_default = throttle;
  }
});

// node_modules/axios/lib/helpers/progressEventReducer.js
var progressEventReducer, progressEventDecorator, asyncDecorator;
var init_progressEventReducer = __esm({
  "node_modules/axios/lib/helpers/progressEventReducer.js"() {
    init_speedometer();
    init_throttle();
    init_utils();
    progressEventReducer = (listener, isDownloadStream, freq = 3) => {
      let bytesNotified = 0;
      const _speedometer = speedometer_default(50, 250);
      return throttle_default((e) => {
        if (!e || !utils_default.isNumber(e.loaded)) {
          return;
        }
        const rawLoaded = e.loaded;
        const total = e.lengthComputable ? e.total : void 0;
        const loaded = Math.max(0, total != null ? Math.min(rawLoaded, total) : rawLoaded);
        const progressBytes = Math.max(0, loaded - bytesNotified);
        const rate = _speedometer(progressBytes);
        bytesNotified = Math.max(bytesNotified, loaded);
        const data = {
          loaded,
          total,
          progress: total ? loaded / total : void 0,
          bytes: progressBytes,
          rate: rate ? rate : void 0,
          estimated: rate && total ? (total - loaded) / rate : void 0,
          event: e,
          lengthComputable: total != null,
          [isDownloadStream ? "download" : "upload"]: true
        };
        listener(data);
      }, freq);
    };
    progressEventDecorator = (total, throttled) => {
      const lengthComputable = total != null;
      return [
        (loaded) => throttled[0]({
          lengthComputable,
          total,
          loaded
        }),
        throttled[1]
      ];
    };
    asyncDecorator = (fn, scheduler = utils_default.asap) => (...args) => scheduler(() => fn(...args));
  }
});

// node_modules/axios/lib/helpers/estimateDataURLDecodedBytes.js
function estimateDataURLDecodedBytes(url2) {
  const fragmentIndex = typeof url2 === "string" ? url2.indexOf("#") : -1;
  return estimateDataURLBytes(
    fragmentIndex === -1 ? url2 : url2.slice(0, fragmentIndex),
    estimatePercentDecodedBase64Bytes,
    /; *base64[\t\n\f\r ]*$/i
  );
}
function estimateDataURLBufferAllocation(url2) {
  return estimateDataURLBytes(url2, estimatePercentDecodedBase64BufferAllocation, /;base64$/i);
}
var isHexDigit, isPercentEncodedByte, hexValue, isBase64Char, isBase64Whitespace, base64Bytes, estimateBase64BufferAllocation, estimatePercentDecodedBase64BufferAllocation, estimatePercentDecodedBase64Bytes, estimateDataURLBytes;
var init_estimateDataURLDecodedBytes = __esm({
  "node_modules/axios/lib/helpers/estimateDataURLDecodedBytes.js"() {
    isHexDigit = (charCode) => charCode >= 48 && charCode <= 57 || charCode >= 65 && charCode <= 70 || charCode >= 97 && charCode <= 102;
    isPercentEncodedByte = (str, i, len) => i + 2 < len && isHexDigit(str.charCodeAt(i + 1)) && isHexDigit(str.charCodeAt(i + 2));
    hexValue = (charCode) => charCode <= 57 ? charCode - 48 : (charCode & 223) - 55;
    isBase64Char = (charCode) => charCode >= 65 && charCode <= 90 || // A-Z
    charCode >= 97 && charCode <= 122 || // a-z
    charCode >= 48 && charCode <= 57 || // 0-9
    charCode === 43 || // +
    charCode === 47 || // /
    charCode === 45 || // - (base64url)
    charCode === 95;
    isBase64Whitespace = (charCode) => charCode === 9 || charCode === 10 || charCode === 12 || charCode === 13 || charCode === 32;
    base64Bytes = (significant) => {
      const groups = Math.floor(significant / 4);
      const remainder = significant % 4;
      return groups * 3 + (remainder === 2 ? 1 : remainder === 3 ? 2 : 0);
    };
    estimateBase64BufferAllocation = (body) => {
      const len = body.length;
      let padding = 0;
      if (len > 0 && body.charCodeAt(len - 1) === 61) {
        padding++;
        if (len > 1 && body.charCodeAt(len - 2) === 61) {
          padding++;
        }
      }
      return Math.floor((len - padding) * 3 / 4);
    };
    estimatePercentDecodedBase64BufferAllocation = (body) => {
      if (body.indexOf("%") === -1) {
        return estimateBase64BufferAllocation(body);
      }
      let length = 0;
      let previous = 0;
      let last = 0;
      for (let i = 0, len = body.length; i < len; i++) {
        let code = body.charCodeAt(i);
        if (code === 37 && isPercentEncodedByte(body, i, len)) {
          code = hexValue(body.charCodeAt(i + 1)) * 16 + hexValue(body.charCodeAt(i + 2));
          i += 2;
        }
        length++;
        previous = last;
        last = code;
      }
      const padding = last === 61 ? previous === 61 ? 2 : 1 : 0;
      return Math.floor((length - padding) * 3 / 4);
    };
    estimatePercentDecodedBase64Bytes = (body) => {
      const len = body.length;
      let significant = 0;
      let padding = 0;
      let invalid = false;
      for (let i = 0; i < len; i++) {
        let code = body.charCodeAt(i);
        if (code === 37 && isPercentEncodedByte(body, i, len)) {
          code = hexValue(body.charCodeAt(i + 1)) * 16 + hexValue(body.charCodeAt(i + 2));
          i += 2;
        }
        if (isBase64Whitespace(code)) {
          continue;
        }
        if (code === 61) {
          padding++;
          continue;
        }
        if (!isBase64Char(code) || padding > 0) {
          invalid = true;
          continue;
        }
        significant++;
      }
      if (invalid || padding > 2 || padding > 0 && (significant + padding) % 4 !== 0 || significant % 4 === 1) {
        return estimateBase64BufferAllocation(body);
      }
      return base64Bytes(significant);
    };
    estimateDataURLBytes = (url2, estimateBase64, base64Pattern) => {
      if (!url2 || typeof url2 !== "string") return 0;
      if (!/^data:/i.test(url2)) return 0;
      const comma = url2.indexOf(",");
      if (comma < 0) return 0;
      const meta = url2.slice(5, comma);
      const body = url2.slice(comma + 1);
      const isBase64 = base64Pattern.test(meta);
      if (isBase64) {
        return estimateBase64(body);
      }
      let bytes = 0;
      for (let i = 0, len = body.length; i < len; i++) {
        const c = body.charCodeAt(i);
        if (c === 37 && isPercentEncodedByte(body, i, len)) {
          bytes += 1;
          i += 2;
        } else if (c < 128) {
          bytes += 1;
        } else if (c < 2048) {
          bytes += 2;
        } else if (c >= 55296 && c <= 56319 && i + 1 < len) {
          const next = body.charCodeAt(i + 1);
          if (next >= 56320 && next <= 57343) {
            bytes += 4;
            i++;
          } else {
            bytes += 3;
          }
        } else {
          bytes += 3;
        }
      }
      return bytes;
    };
  }
});

// node_modules/axios/lib/adapters/http.js
import http from "http";
import https from "https";
import http22 from "http2";
import util3 from "util";
import { resolve as resolvePath } from "path";
import zlib from "zlib";
import stream3 from "stream";
import { EventEmitter } from "events";
function handleSocketError(err) {
  const current = this[kAxiosCurrentReq];
  if (current && !current.destroyed) {
    current.destroy(err);
  }
}
function isNodeNativeEnvProxySupported(nodeVersion = process.versions && process.versions.node) {
  if (!nodeVersion) {
    return false;
  }
  const [major, minor] = nodeVersion.split(".").map((part) => Number(part));
  if (!Number.isInteger(major) || !Number.isInteger(minor)) {
    return false;
  }
  if (major > 24) {
    return true;
  }
  return NODE_NATIVE_ENV_PROXY_SUPPORT[major] != null && minor >= NODE_NATIVE_ENV_PROXY_SUPPORT[major];
}
function isNodeEnvProxyEnabled(agent, nodeVersion = process.versions && process.versions.node) {
  if (!isNodeNativeEnvProxySupported(nodeVersion)) {
    return false;
  }
  const agentOptions = agent && agent.options;
  return Boolean(
    agentOptions && utils_default.hasOwnProp(agentOptions, "proxyEnv") && agentOptions.proxyEnv != null
  );
}
function getProxyEnvAgent(options, configHttpAgent, configHttpsAgent) {
  return isHttps.test(options.protocol) ? configHttpsAgent || https.globalAgent : configHttpAgent || http.globalAgent;
}
function getDirectAgent(configuredAgent, transport) {
  if (!process.allowedNodeEnvironmentFlags || !process.allowedNodeEnvironmentFlags.has("--use-env-proxy")) {
    return configuredAgent;
  }
  const agent = configuredAgent || transport.globalAgent;
  const options = agent && agent.options;
  if (!(agent instanceof transport.Agent) || agent.protocol !== transport.globalAgent.protocol || !options || !utils_default.hasOwnProp(options, "proxyEnv") || typeof options.proxyEnv !== "object" || options.proxyEnv === null) {
    return configuredAgent;
  }
  for (const hook of nativeAgentPoolHooks) {
    if (utils_default.isFunction(transport.Agent.prototype[hook]) && agent[hook] !== transport.Agent.prototype[hook]) {
      throw new AxiosError_default(
        "proxy: false cannot adapt an agent overriding " + hook + "; use an agent constructed without proxyEnv",
        AxiosError_default.ERR_BAD_OPTION_VALUE
      );
    }
  }
  let direct = directAgents.get(agent);
  if (!direct) {
    direct = new transport.Agent({ ...options, proxyEnv: void 0 });
    const destroy = agent.destroy;
    agent.destroy = function destroyWithDirectAgent() {
      try {
        return destroy.apply(this, arguments);
      } finally {
        direct.destroy();
      }
    };
    directAgents.set(agent, direct);
  }
  for (const hook of directAgentHooks) {
    direct[hook] = agent[hook] === transport.Agent.prototype[hook] ? transport.Agent.prototype[hook] : bind(agent[hook], agent);
  }
  return direct;
}
function setDirectAgent(options, configHttpAgent, configHttpsAgent) {
  const secure = isHttps.test(options.protocol);
  const agent = getDirectAgent(secure ? configHttpsAgent : configHttpAgent, secure ? https : http);
  options.agent = agent;
  if (options.agents) {
    options.agents[secure ? "https" : "http"] = agent;
  }
}
function getTunnelingAgent(agentOptions, userHttpsAgent) {
  const key = agentOptions.protocol + "//" + agentOptions.hostname + ":" + (agentOptions.port || "") + "#" + (agentOptions.auth || "");
  const cache = userHttpsAgent ? tunnelingAgentCacheUser.get(userHttpsAgent) || tunnelingAgentCacheUser.set(userHttpsAgent, /* @__PURE__ */ new Map()).get(userHttpsAgent) : tunnelingAgentCache;
  let agent = cache.get(key);
  if (agent) return agent;
  const merged = userHttpsAgent && userHttpsAgent.options ? { ...userHttpsAgent.options, ...agentOptions } : agentOptions;
  agent = new import_https_proxy_agent.default(merged);
  const callback = agent.callback;
  agent.callback = function axiosTunnelingAgentCallback(req, opts) {
    const originTLSAgent = userHttpsAgent || https.globalAgent;
    const originTLSOptions = originTLSAgent && originTLSAgent.options;
    return callback.call(this, req, { ...originTLSOptions, ...opts });
  };
  agent[kAxiosInstalledTunnel] = true;
  cache.set(key, agent);
  return agent;
}
function dispatchBeforeRedirect(options, responseDetails, requestDetails) {
  if (!isSameOriginRedirect(options, requestDetails)) {
    stripMatchingHeaders(options.headers, /* @__PURE__ */ new Set(["authorization", "cookie"]));
    delete options.auth;
  }
  if (options.beforeRedirects.proxy) {
    options.beforeRedirects.proxy(options);
  }
  if (options.beforeRedirects.auth) {
    options.beforeRedirects.auth(options);
  }
  if (options.beforeRedirects.sensitiveHeaders) {
    options.beforeRedirects.sensitiveHeaders(options, requestDetails);
  }
  if (options.beforeRedirects.direct) {
    options.beforeRedirects.direct(options);
  }
  if (options.beforeRedirects.config) {
    options.beforeRedirects.config(options, responseDetails, requestDetails);
  }
}
function stripMatchingHeaders(headers, sensitiveSet) {
  if (!headers) {
    return;
  }
  Object.keys(headers).forEach((header) => {
    if (sensitiveSet.has(header.toLowerCase())) {
      delete headers[header];
    }
  });
}
function isSameOriginRedirect(redirectOptions, requestDetails) {
  if (!requestDetails) {
    return false;
  }
  try {
    return new URL(requestDetails.url).origin === new URL(redirectOptions.href).origin;
  } catch (e) {
    return false;
  }
}
function setProxy(options, configProxy, location, isRedirect, configHttpsAgent, configHttpAgent, allowEnvProxy = true) {
  let proxy = configProxy;
  const proxyEnvAgent = getProxyEnvAgent(options, configHttpAgent, configHttpsAgent);
  if (!proxy && proxy !== false && allowEnvProxy && !isNodeEnvProxyEnabled(proxyEnvAgent)) {
    const proxyUrl = getProxyForUrl(location);
    if (proxyUrl) {
      if (!shouldBypassProxy(location)) {
        proxy = new URL(proxyUrl);
      }
    }
  }
  if (isRedirect && options.headers) {
    for (const name of Object.keys(options.headers)) {
      if (name.toLowerCase() === "proxy-authorization") {
        delete options.headers[name];
      }
    }
  }
  if (isRedirect && options.agent && options.agent[kAxiosInstalledTunnel]) {
    options.agent = void 0;
  }
  if (isRedirect && options.agents && options.agents.https && options.agents.https[kAxiosInstalledTunnel]) {
    options.agents.https = configHttpsAgent;
  }
  if (proxy) {
    const isProxyURL = proxy instanceof URL;
    const readProxyField = (key) => isProxyURL || utils_default.hasOwnProp(proxy, key) ? proxy[key] : void 0;
    const proxyUsername = readProxyField("username");
    const proxyPassword = readProxyField("password");
    let proxyAuth = utils_default.hasOwnProp(proxy, "auth") ? proxy.auth : void 0;
    if (proxyUsername) {
      proxyAuth = (proxyUsername || "") + ":" + (proxyPassword || "");
    }
    if (proxyAuth) {
      const authIsObject = typeof proxyAuth === "object";
      const authUsername = authIsObject && utils_default.hasOwnProp(proxyAuth, "username") ? proxyAuth.username : void 0;
      const authPassword = authIsObject && utils_default.hasOwnProp(proxyAuth, "password") ? proxyAuth.password : void 0;
      const validProxyAuth = Boolean(authUsername || authPassword);
      if (validProxyAuth) {
        proxyAuth = (authUsername || "") + ":" + (authPassword || "");
      } else if (authIsObject) {
        throw new AxiosError_default("Invalid proxy authorization", AxiosError_default.ERR_BAD_OPTION, { proxy });
      }
    }
    const targetIsHttps = isHttps.test(options.protocol);
    if (targetIsHttps) {
      if (!(configHttpsAgent instanceof import_https_proxy_agent.default)) {
        const proxyHost = readProxyField("hostname") || readProxyField("host");
        const proxyPort = readProxyField("port");
        const rawProxyProtocol = readProxyField("protocol");
        const normalizedProtocol = rawProxyProtocol ? rawProxyProtocol.includes(":") ? rawProxyProtocol : `${rawProxyProtocol}:` : "http:";
        const proxyHostForURL = proxyHost && proxyHost.includes(":") && !proxyHost.startsWith("[") ? `[${proxyHost}]` : proxyHost;
        const proxyURL = new URL(
          `${normalizedProtocol}//${proxyHostForURL}${proxyPort ? ":" + proxyPort : ""}`
        );
        const agentOptions = {
          protocol: proxyURL.protocol,
          hostname: proxyURL.hostname.replace(/^\[|\]$/g, ""),
          port: proxyURL.port,
          auth: proxyAuth && typeof proxyAuth === "string" ? proxyAuth : void 0
        };
        if (proxyURL.protocol === "https:") {
          agentOptions.ALPNProtocols = ["http/1.1"];
        }
        const tunnelingAgent = getTunnelingAgent(agentOptions, configHttpsAgent);
        options.agent = tunnelingAgent;
        if (options.agents) {
          options.agents.https = tunnelingAgent;
        }
      }
    } else {
      if (proxyAuth) {
        const base64 = Buffer.from(proxyAuth, "utf8").toString("base64");
        options.headers["Proxy-Authorization"] = "Basic " + base64;
      }
      let hasUserHostHeader = false;
      for (const name of Object.keys(options.headers)) {
        if (name.toLowerCase() === "host") {
          hasUserHostHeader = true;
          break;
        }
      }
      if (!hasUserHostHeader) {
        options.headers.host = options.hostname + (options.port ? ":" + options.port : "");
      }
      const proxyHost = readProxyField("hostname") || readProxyField("host");
      options.hostname = proxyHost;
      options.host = proxyHost;
      options.port = readProxyField("port");
      options.path = location;
      const proxyProtocol = readProxyField("protocol");
      if (proxyProtocol) {
        options.protocol = proxyProtocol.includes(":") ? proxyProtocol : `${proxyProtocol}:`;
      }
    }
  }
  options.beforeRedirects.proxy = function beforeRedirect(redirectOptions) {
    setProxy(
      redirectOptions,
      configProxy,
      redirectOptions.href,
      true,
      configHttpsAgent,
      configHttpAgent,
      allowEnvProxy
    );
  };
  return Boolean(
    proxy || configProxy !== false && allowEnvProxy && isNodeEnvProxyEnabled(proxyEnvAgent)
  );
}
var import_https_proxy_agent, import_follow_redirects, zlibOptions, brotliOptions, zstdOptions, isBrotliSupported, isZstdSupported, ACCEPT_ENCODING, ACCEPT_ENCODING_WITH_ZSTD, scheduleProgress, httpFollow, httpsFollow, isHttps, kAxiosSocketListener, kAxiosCurrentReq, kAxiosInstalledTunnel, tunnelingAgentCache, tunnelingAgentCacheUser, directAgents, directAgentHooks, nativeAgentPoolHooks, NODE_NATIVE_ENV_PROXY_SUPPORT, supportedProtocols, decodeURIComponentSafe, flushOnFinish, http2Sessions, isHttpAdapterSupported, wrapAsync, resolveFamily, buildAddressEntry, normalizedLookupCache, normalizeLookup, http2Transport, http_default;
var init_http = __esm({
  "node_modules/axios/lib/adapters/http.js"() {
    init_utils();
    init_settle();
    init_buildFullPath();
    init_buildURL();
    init_proxy_from_env();
    import_https_proxy_agent = __toESM(require_dist(), 1);
    import_follow_redirects = __toESM(require_follow_redirects(), 1);
    init_data();
    init_transitional();
    init_AxiosError();
    init_CanceledError();
    init_platform();
    init_fromDataURI();
    init_AxiosHeaders();
    init_setFormDataHeaders();
    init_AxiosTransformStream();
    init_formDataToStream();
    init_readBlob();
    init_ZlibHeaderTransformStream();
    init_Http2Sessions();
    init_callbackify();
    init_shouldBypassProxy();
    init_bind();
    init_sanitizeHeaderValue();
    init_progressEventReducer();
    init_estimateDataURLDecodedBytes();
    zlibOptions = {
      flush: zlib.constants.Z_SYNC_FLUSH,
      finishFlush: zlib.constants.Z_SYNC_FLUSH
    };
    brotliOptions = {
      flush: zlib.constants.BROTLI_OPERATION_FLUSH,
      finishFlush: zlib.constants.BROTLI_OPERATION_FLUSH
    };
    zstdOptions = {
      flush: zlib.constants.ZSTD_e_flush,
      finishFlush: zlib.constants.ZSTD_e_flush
    };
    isBrotliSupported = utils_default.isFunction(zlib.createBrotliDecompress);
    isZstdSupported = utils_default.isFunction(zlib.createZstdDecompress);
    ACCEPT_ENCODING = "gzip, compress, deflate" + (isBrotliSupported ? ", br" : "");
    ACCEPT_ENCODING_WITH_ZSTD = ACCEPT_ENCODING + (isZstdSupported ? ", zstd" : "");
    scheduleProgress = typeof process !== "undefined" && process.nextTick ? process.nextTick.bind(process) : utils_default.asap;
    ({ http: httpFollow, https: httpsFollow } = import_follow_redirects.default);
    isHttps = /https:?/;
    kAxiosSocketListener = /* @__PURE__ */ Symbol("axios.http.socketListener");
    kAxiosCurrentReq = /* @__PURE__ */ Symbol("axios.http.currentReq");
    kAxiosInstalledTunnel = /* @__PURE__ */ Symbol("axios.http.installedTunnel");
    tunnelingAgentCache = /* @__PURE__ */ new Map();
    tunnelingAgentCacheUser = /* @__PURE__ */ new WeakMap();
    directAgents = /* @__PURE__ */ new WeakMap();
    directAgentHooks = ["createConnection", "getName"];
    nativeAgentPoolHooks = [
      "addRequest",
      "createSocket",
      "removeSocket",
      "keepSocketAlive",
      "reuseSocket",
      "_getSession",
      "_cacheSession",
      "_evictSession"
    ];
    NODE_NATIVE_ENV_PROXY_SUPPORT = {
      22: 21,
      24: 5
    };
    supportedProtocols = platform_default.protocols.map((protocol) => {
      return protocol + ":";
    });
    decodeURIComponentSafe = (value) => {
      if (!utils_default.isString(value)) {
        return value;
      }
      try {
        return decodeURIComponent(value);
      } catch (error) {
        return value;
      }
    };
    flushOnFinish = (stream4, [throttled, flush]) => {
      stream4.on("end", flush).on("error", flush);
      return throttled;
    };
    http2Sessions = new Http2Sessions_default();
    isHttpAdapterSupported = typeof process !== "undefined" && utils_default.kindOf(process) === "process";
    wrapAsync = (asyncExecutor) => {
      return new Promise((resolve, reject) => {
        let onDone;
        let isDone;
        const done = (value, isRejected) => {
          if (isDone) return;
          isDone = true;
          onDone && onDone(value, isRejected);
        };
        const _resolve = (value) => {
          done(value);
          resolve(value);
        };
        const _reject = (reason) => {
          done(reason, true);
          reject(reason);
        };
        asyncExecutor(_resolve, _reject, (onDoneHandler) => onDone = onDoneHandler).catch(_reject);
      });
    };
    resolveFamily = ({ address, family }) => {
      if (!utils_default.isString(address)) {
        throw new AxiosError_default("address must be a string", AxiosError_default.ERR_BAD_OPTION_VALUE);
      }
      return {
        address,
        family: family || (address.indexOf(".") < 0 ? 6 : 4)
      };
    };
    buildAddressEntry = (address, family) => resolveFamily(utils_default.isObject(address) ? address : { address, family });
    normalizedLookupCache = /* @__PURE__ */ new WeakMap();
    normalizeLookup = (lookup2) => {
      let normalized = normalizedLookupCache.get(lookup2);
      if (normalized) {
        return normalized;
      }
      const callbackLookup = callbackify_default(lookup2, (value) => utils_default.isArray(value) ? value : [value]);
      normalized = (hostname, opt, cb) => {
        callbackLookup(hostname, opt, (err, arg0, arg1) => {
          if (err) {
            return cb(err);
          }
          let addresses;
          try {
            addresses = utils_default.isArray(arg0) ? arg0.map((addr) => buildAddressEntry(addr)) : [buildAddressEntry(arg0, arg1)];
          } catch (error) {
            return cb(error);
          }
          opt.all ? cb(err, addresses) : cb(err, addresses[0].address, addresses[0].family);
        });
      };
      normalizedLookupCache.set(lookup2, normalized);
      return normalized;
    };
    http2Transport = {
      request(options, cb) {
        const authority = options.protocol + "//" + options.hostname + ":" + (options.port || (options.protocol === "https:" ? 443 : 80));
        const { http2Options, headers } = options;
        const session = http2Sessions.getSession(authority, http2Options);
        const { HTTP2_HEADER_SCHEME, HTTP2_HEADER_METHOD, HTTP2_HEADER_PATH, HTTP2_HEADER_STATUS } = http22.constants;
        const http2Headers = {
          [HTTP2_HEADER_SCHEME]: options.protocol.replace(":", ""),
          [HTTP2_HEADER_METHOD]: options.method,
          [HTTP2_HEADER_PATH]: options.path
        };
        utils_default.forEach(headers, (header, name) => {
          name.charAt(0) !== ":" && (http2Headers[name] = header);
        });
        const req = session.request(http2Headers);
        req.once("response", (responseHeaders) => {
          const response = req;
          responseHeaders = Object.assign({}, responseHeaders);
          const status = responseHeaders[HTTP2_HEADER_STATUS];
          delete responseHeaders[HTTP2_HEADER_STATUS];
          response.headers = responseHeaders;
          response.statusCode = +status;
          cb(response);
        });
        return req;
      }
    };
    http_default = isHttpAdapterSupported && function httpAdapter(config) {
      return wrapAsync(async function dispatchHttpRequest(resolve, reject, onDone) {
        const own2 = (key) => utils_default.getSafeProp(config, key);
        const transitional2 = own2("transitional") || transitional_default;
        let data = own2("data");
        let lookup2 = own2("lookup");
        let family = own2("family");
        let httpVersion = own2("httpVersion");
        if (httpVersion === void 0) httpVersion = 1;
        const rawHttpVersion = httpVersion;
        let http2Options = own2("http2Options");
        const configProxy = own2("proxy");
        const httpAgent = own2("httpAgent");
        const httpsAgent = own2("httpsAgent");
        const responseType = own2("responseType");
        const responseEncoding = own2("responseEncoding");
        const socketPath = own2("socketPath");
        const method = own2("method").toUpperCase();
        const maxRedirects = own2("maxRedirects");
        const maxBodyLength = own2("maxBodyLength");
        const maxContentLength = own2("maxContentLength");
        const decompress = own2("decompress");
        let isDone;
        let rejected = false;
        let req;
        let connectPhaseTimer;
        try {
          httpVersion = +httpVersion;
        } catch (err) {
          throw new AxiosError_default(
            "Invalid protocol version: value is not a number",
            AxiosError_default.ERR_BAD_OPTION_VALUE,
            config
          );
        }
        if (Number.isNaN(httpVersion)) {
          throw new AxiosError_default(
            `Invalid protocol version: '${rawHttpVersion}' is not a number`,
            AxiosError_default.ERR_BAD_OPTION_VALUE,
            config
          );
        }
        if (httpVersion !== 1 && httpVersion !== 2) {
          throw new AxiosError_default(
            `Unsupported protocol version '${httpVersion}'`,
            AxiosError_default.ERR_BAD_OPTION_VALUE,
            config
          );
        }
        const isHttp2 = httpVersion === 2;
        if (lookup2) {
          lookup2 = normalizeLookup(lookup2);
        }
        const abortEmitter = new EventEmitter();
        function abort(reason) {
          try {
            abortEmitter.emit(
              "abort",
              !reason || reason.type ? new CanceledError_default(null, config, req) : reason
            );
          } catch (err) {
          }
        }
        function clearConnectPhaseTimer() {
          if (connectPhaseTimer) {
            clearTimeout(connectPhaseTimer);
            connectPhaseTimer = null;
          }
        }
        function createTimeoutError() {
          const configTimeout = own2("timeout");
          let timeoutErrorMessage = configTimeout ? "timeout of " + configTimeout + "ms exceeded" : "timeout exceeded";
          const configTimeoutErrorMessage = own2("timeoutErrorMessage");
          if (configTimeoutErrorMessage) {
            timeoutErrorMessage = configTimeoutErrorMessage;
          }
          return new AxiosError_default(
            timeoutErrorMessage,
            transitional2.clarifyTimeoutError ? AxiosError_default.ETIMEDOUT : AxiosError_default.ECONNABORTED,
            config,
            req
          );
        }
        abortEmitter.once("abort", reject);
        const onFinished = () => {
          clearConnectPhaseTimer();
          if (config.cancelToken) {
            config.cancelToken.unsubscribe(abort);
          }
          if (config.signal) {
            config.signal.removeEventListener("abort", abort);
          }
          abortEmitter.removeAllListeners();
        };
        if (config.cancelToken || config.signal) {
          config.cancelToken && config.cancelToken.subscribe(abort);
          if (config.signal) {
            config.signal.aborted ? abort() : config.signal.addEventListener("abort", abort);
          }
        }
        onDone((response, isRejected) => {
          isDone = true;
          clearConnectPhaseTimer();
          if (isRejected) {
            rejected = true;
            onFinished();
            return;
          }
          const { data: data2 } = response;
          if (data2 instanceof stream3.Readable || data2 instanceof stream3.Duplex) {
            const offListeners = stream3.finished(data2, () => {
              offListeners();
              onFinished();
            });
          } else {
            onFinished();
          }
        });
        const fullPath = buildFullPath(own2("baseURL"), own2("url"), own2("allowAbsoluteUrls"), config);
        const urlBase = socketPath ? "http://localhost" : platform_default.hasBrowserEnv ? platform_default.origin : void 0;
        const parsed = new URL(fullPath, urlBase);
        const protocol = parsed.protocol || supportedProtocols[0];
        if (protocol === "data:") {
          if (maxContentLength > -1) {
            const dataUrl = String(own2("url") || fullPath || "");
            const estimated = estimateDataURLBufferAllocation(dataUrl);
            if (estimated > maxContentLength) {
              return reject(
                new AxiosError_default(
                  "maxContentLength size of " + maxContentLength + " exceeded",
                  AxiosError_default.ERR_BAD_RESPONSE,
                  config
                )
              );
            }
          }
          let convertedData;
          if (method !== "GET") {
            return settle(resolve, reject, {
              status: 405,
              statusText: "method not allowed",
              headers: {},
              config
            });
          }
          try {
            convertedData = fromDataURI(own2("url"), responseType === "blob", {
              Blob: config.env && config.env.Blob
            });
          } catch (err) {
            throw AxiosError_default.from(err, AxiosError_default.ERR_BAD_REQUEST, config);
          }
          if (maxContentLength > -1 && (responseType === "blob" ? convertedData.size : convertedData.length) > maxContentLength) {
            return reject(
              new AxiosError_default(
                "maxContentLength size of " + maxContentLength + " exceeded",
                AxiosError_default.ERR_BAD_RESPONSE,
                config
              )
            );
          }
          if (responseType === "text") {
            convertedData = convertedData.toString(responseEncoding);
            if (!responseEncoding || responseEncoding === "utf8") {
              convertedData = utils_default.stripBOM(convertedData);
            }
          } else if (responseType === "stream") {
            convertedData = stream3.Readable.from(convertedData);
          }
          return settle(resolve, reject, {
            data: convertedData,
            status: 200,
            statusText: "OK",
            headers: new AxiosHeaders_default(),
            config
          });
        }
        if (supportedProtocols.indexOf(protocol) === -1) {
          return reject(
            new AxiosError_default("Unsupported protocol " + protocol, AxiosError_default.ERR_BAD_REQUEST, config)
          );
        }
        const headers = AxiosHeaders_default.from(config.headers).normalize();
        headers.set("User-Agent", "axios/" + VERSION, false);
        const { onUploadProgress, onDownloadProgress } = config;
        const maxRate = config.maxRate;
        let maxUploadRate = void 0;
        let maxDownloadRate = void 0;
        if (utils_default.isSpecCompliantForm(data)) {
          const userBoundary = headers.getContentType(/boundary=([-_\w\d]{10,70})/i);
          data = formDataToStream_default(
            data,
            (formHeaders) => {
              headers.set(formHeaders);
            },
            {
              tag: `axios-${VERSION}-boundary`,
              boundary: userBoundary && userBoundary[1] || void 0
            }
          );
        } else if (utils_default.isFormData(data) && utils_default.isFunction(data.getHeaders) && data.getHeaders !== Object.prototype.getHeaders) {
          setFormDataHeaders(headers, data.getHeaders(), own2("formDataHeaderPolicy"));
          if (!headers.hasContentLength()) {
            try {
              const knownLength = await util3.promisify(data.getLength).call(data);
              Number.isFinite(knownLength) && knownLength >= 0 && headers.setContentLength(knownLength);
            } catch (e) {
            }
          }
        } else if (utils_default.isBlob(data) || utils_default.isFile(data)) {
          data.size && headers.setContentType(data.type || "application/octet-stream");
          headers.setContentLength(data.size || 0);
          data = stream3.Readable.from(readBlob_default(data));
        } else if (data && !utils_default.isStream(data)) {
          if (Buffer.isBuffer(data)) {
          } else if (utils_default.isArrayBuffer(data)) {
            data = Buffer.from(new Uint8Array(data));
          } else if (utils_default.isString(data)) {
            data = Buffer.from(data, "utf-8");
          } else {
            return reject(
              new AxiosError_default(
                "Data after transformation must be a string, an ArrayBuffer, a Buffer, or a Stream",
                AxiosError_default.ERR_BAD_REQUEST,
                config
              )
            );
          }
          headers.setContentLength(data.length, false);
          if (maxBodyLength > -1 && data.length > maxBodyLength) {
            return reject(
              new AxiosError_default(
                "Request body larger than maxBodyLength limit",
                AxiosError_default.ERR_BAD_REQUEST,
                config
              )
            );
          }
        }
        const contentLength = utils_default.toFiniteNumber(headers.getContentLength());
        if (utils_default.isArray(maxRate)) {
          maxUploadRate = maxRate[0];
          maxDownloadRate = maxRate[1];
        } else {
          maxUploadRate = maxDownloadRate = maxRate;
        }
        if (data && (onUploadProgress || maxUploadRate)) {
          if (!utils_default.isStream(data)) {
            data = stream3.Readable.from(data, { objectMode: false });
          }
          data = stream3.pipeline(
            [
              data,
              new AxiosTransformStream_default({
                maxRate: utils_default.toFiniteNumber(maxUploadRate)
              })
            ],
            utils_default.noop
          );
          onUploadProgress && data.on(
            "progress",
            flushOnFinish(
              data,
              progressEventDecorator(
                contentLength,
                progressEventReducer(asyncDecorator(onUploadProgress, scheduleProgress), false, 3)
              )
            )
          );
        }
        let auth = void 0;
        const configAuth = own2("auth");
        if (configAuth) {
          const username = utils_default.getSafeProp(configAuth, "username") || "";
          const password = utils_default.getSafeProp(configAuth, "password") || "";
          auth = username + ":" + password;
        }
        if (!auth && (parsed.username || parsed.password)) {
          const urlUsername = decodeURIComponentSafe(parsed.username);
          const urlPassword = decodeURIComponentSafe(parsed.password);
          auth = urlUsername + ":" + urlPassword;
        }
        auth && headers.delete("authorization");
        let path;
        try {
          path = buildURL(
            parsed.pathname + parsed.search,
            own2("params"),
            own2("paramsSerializer")
          ).replace(/^\?/, "");
        } catch (err) {
          return reject(
            AxiosError_default.from(err, AxiosError_default.ERR_BAD_REQUEST, config, null, null, {
              url: own2("url"),
              exists: true
            })
          );
        }
        headers.set(
          "Accept-Encoding",
          utils_default.hasOwnProp(transitional2, "advertiseZstdAcceptEncoding") && transitional2.advertiseZstdAcceptEncoding === true ? ACCEPT_ENCODING_WITH_ZSTD : ACCEPT_ENCODING,
          false
        );
        if (isHttp2 && lookup2) {
          http2Options = Object.assign(/* @__PURE__ */ Object.create(null), http2Options, { lookup: lookup2 });
        }
        const options = Object.assign(/* @__PURE__ */ Object.create(null), {
          path,
          method,
          headers: toByteStringHeaderObject(headers),
          agents: { http: httpAgent, https: httpsAgent },
          auth,
          protocol,
          family,
          beforeRedirect: dispatchBeforeRedirect,
          beforeRedirects: /* @__PURE__ */ Object.create(null),
          http2Options,
          createConnection: void 0
        });
        !utils_default.isUndefined(lookup2) && (options.lookup = lookup2);
        let proxyApplied = false;
        if (socketPath) {
          if (typeof socketPath !== "string") {
            return reject(
              new AxiosError_default("socketPath must be a string", AxiosError_default.ERR_BAD_OPTION_VALUE, config)
            );
          }
          const allowedSocketPaths = own2("allowedSocketPaths");
          if (allowedSocketPaths != null) {
            const allowed = Array.isArray(allowedSocketPaths) ? allowedSocketPaths : [allowedSocketPaths];
            const resolvedSocket = resolvePath(socketPath);
            const isAllowed = allowed.some(
              (entry) => typeof entry === "string" && resolvePath(entry) === resolvedSocket
            );
            if (!isAllowed) {
              return reject(
                new AxiosError_default(
                  `socketPath "${socketPath}" is not permitted by allowedSocketPaths`,
                  AxiosError_default.ERR_BAD_OPTION_VALUE,
                  config
                )
              );
            }
          }
          options.socketPath = socketPath;
        } else {
          options.hostname = parsed.hostname.startsWith("[") ? parsed.hostname.slice(1, -1) : parsed.hostname;
          options.port = parsed.port;
          proxyApplied = setProxy(
            options,
            configProxy,
            protocol + "//" + parsed.hostname + (parsed.port ? ":" + parsed.port : "") + options.path,
            false,
            httpsAgent,
            httpAgent,
            // The HTTP/2 transport connects independently of HTTP/1 agents, so it
            // cannot apply either axios-resolved or agent-local environment proxies.
            // Explicit proxy config is still processed and rejected below.
            !isHttp2
          );
        }
        let transport;
        let isNativeTransport = false;
        const isHttpsRequest = isHttps.test(options.protocol);
        if (!isHttp2 && configProxy === false) {
          setDirectAgent(options, httpAgent, httpsAgent);
          options.beforeRedirects.direct = (redirectOptions) => {
            setDirectAgent(redirectOptions, httpAgent, httpsAgent);
          };
        } else if (options.agent == null) {
          options.agent = isHttpsRequest ? httpsAgent : httpAgent;
        }
        if (isHttp2) {
          if (proxyApplied) {
            return reject(
              new AxiosError_default(
                "HTTP/2 requests with a proxy are not supported",
                AxiosError_default.ERR_NOT_SUPPORT,
                config
              )
            );
          }
          transport = http2Transport;
        } else {
          const configTransport = own2("transport");
          if (configTransport) {
            transport = configTransport;
          } else if (maxRedirects === 0) {
            transport = isHttpsRequest ? https : http;
            isNativeTransport = true;
          } else {
            options.sensitiveHeaders = [];
            if (maxRedirects) {
              options.maxRedirects = maxRedirects;
            }
            const configBeforeRedirect = own2("beforeRedirect");
            if (configBeforeRedirect) {
              options.beforeRedirects.config = configBeforeRedirect;
            }
            if (auth) {
              const requestOrigin = parsed.origin;
              const authToRestore = auth;
              options.beforeRedirects.auth = function beforeRedirectAuth(redirectOptions) {
                try {
                  if (new URL(redirectOptions.href).origin === requestOrigin) {
                    redirectOptions.auth = authToRestore;
                  }
                } catch (e) {
                }
              };
            }
            const sensitiveHeaders = own2("sensitiveHeaders");
            if (sensitiveHeaders != null) {
              if (!utils_default.isArray(sensitiveHeaders)) {
                return reject(
                  new AxiosError_default(
                    "sensitiveHeaders must be an array of strings",
                    AxiosError_default.ERR_BAD_OPTION_VALUE,
                    config
                  )
                );
              }
              const sensitiveSet = /* @__PURE__ */ new Set();
              for (const header of sensitiveHeaders) {
                if (!utils_default.isString(header)) {
                  return reject(
                    new AxiosError_default(
                      "sensitiveHeaders must be an array of strings",
                      AxiosError_default.ERR_BAD_OPTION_VALUE,
                      config
                    )
                  );
                }
                sensitiveSet.add(header.toLowerCase());
              }
              if (sensitiveSet.size) {
                options.sensitiveHeaders = Array.from(sensitiveSet);
                options.beforeRedirects.sensitiveHeaders = function beforeRedirectSensitiveHeaders(redirectOptions, requestDetails) {
                  if (!isSameOriginRedirect(redirectOptions, requestDetails)) {
                    stripMatchingHeaders(redirectOptions.headers, sensitiveSet);
                  }
                };
              }
            }
            transport = isHttpsRequest ? httpsFollow : httpFollow;
          }
        }
        if (maxBodyLength > -1) {
          options.maxBodyLength = maxBodyLength;
        } else {
          options.maxBodyLength = Infinity;
        }
        options.insecureHTTPParser = Boolean(own2("insecureHTTPParser"));
        req = transport.request(options, function handleResponse(res) {
          clearConnectPhaseTimer();
          if (req.destroyed) return;
          const streams = [res];
          const responseLength = utils_default.toFiniteNumber(res.headers["content-length"]);
          if (onDownloadProgress || maxDownloadRate) {
            const transformStream = new AxiosTransformStream_default({
              maxRate: utils_default.toFiniteNumber(maxDownloadRate)
            });
            onDownloadProgress && transformStream.on(
              "progress",
              flushOnFinish(
                transformStream,
                progressEventDecorator(
                  responseLength,
                  progressEventReducer(
                    asyncDecorator(onDownloadProgress, scheduleProgress),
                    true,
                    3
                  )
                )
              )
            );
            streams.push(transformStream);
          }
          let responseStream = res;
          const lastRequest = res.req || req;
          if (decompress !== false && res.headers["content-encoding"]) {
            if (method === "HEAD" || res.statusCode === 204) {
              delete res.headers["content-encoding"];
            }
            switch ((res.headers["content-encoding"] || "").toLowerCase()) {
              /*eslint default-case:0*/
              case "gzip":
              case "x-gzip":
              case "compress":
              case "x-compress":
                streams.push(zlib.createUnzip(zlibOptions));
                delete res.headers["content-encoding"];
                break;
              case "deflate":
                streams.push(new ZlibHeaderTransformStream_default());
                streams.push(zlib.createUnzip(zlibOptions));
                delete res.headers["content-encoding"];
                break;
              case "br":
                if (isBrotliSupported) {
                  streams.push(zlib.createBrotliDecompress(brotliOptions));
                  delete res.headers["content-encoding"];
                }
                break;
              case "zstd":
                if (isZstdSupported) {
                  streams.push(zlib.createZstdDecompress(zstdOptions));
                  delete res.headers["content-encoding"];
                }
                break;
            }
          }
          responseStream = streams.length > 1 ? stream3.pipeline(streams, utils_default.noop) : streams[0];
          const response = {
            status: res.statusCode,
            statusText: res.statusMessage,
            headers: new AxiosHeaders_default(res.headers),
            config,
            request: lastRequest
          };
          if (responseType === "stream") {
            if (maxContentLength > -1) {
              const limit = maxContentLength;
              const source = responseStream;
              async function* enforceMaxContentLength() {
                let totalResponseBytes = 0;
                for await (const chunk of source) {
                  totalResponseBytes += chunk.length;
                  if (totalResponseBytes > limit) {
                    throw new AxiosError_default(
                      "maxContentLength size of " + limit + " exceeded",
                      AxiosError_default.ERR_BAD_RESPONSE,
                      config,
                      lastRequest
                    );
                  }
                  yield chunk;
                }
              }
              responseStream = stream3.Readable.from(enforceMaxContentLength(), {
                objectMode: false
              });
            }
            response.data = responseStream;
            settle(resolve, reject, response);
          } else {
            const responseBuffer = [];
            let totalResponseBytes = 0;
            responseStream.on("data", function handleStreamData(chunk) {
              responseBuffer.push(chunk);
              totalResponseBytes += chunk.length;
              if (maxContentLength > -1 && totalResponseBytes > maxContentLength) {
                rejected = true;
                responseStream.destroy();
                abort(
                  new AxiosError_default(
                    "maxContentLength size of " + maxContentLength + " exceeded",
                    AxiosError_default.ERR_BAD_RESPONSE,
                    config,
                    lastRequest
                  )
                );
              }
            });
            responseStream.on("aborted", function handlerStreamAborted() {
              if (rejected) {
                return;
              }
              const err = new AxiosError_default(
                "stream has been aborted",
                AxiosError_default.ERR_BAD_RESPONSE,
                config,
                lastRequest,
                response
              );
              responseStream.destroy(err);
              reject(err);
            });
            responseStream.on("error", function handleStreamError(err) {
              if (rejected) return;
              reject(AxiosError_default.from(err, null, config, lastRequest, response));
            });
            responseStream.on("end", function handleStreamEnd() {
              try {
                let responseData = responseBuffer.length === 1 ? responseBuffer[0] : Buffer.concat(responseBuffer);
                if (responseType !== "arraybuffer") {
                  responseData = responseData.toString(responseEncoding);
                  if (!responseEncoding || responseEncoding === "utf8") {
                    responseData = utils_default.stripBOM(responseData);
                  }
                }
                response.data = responseData;
              } catch (err) {
                return reject(AxiosError_default.from(err, null, config, response.request, response));
              }
              settle(resolve, reject, response);
            });
          }
          abortEmitter.once("abort", (err) => {
            if (!responseStream.destroyed) {
              responseStream.emit("error", err);
              responseStream.destroy();
            }
          });
        });
        abortEmitter.once("abort", (err) => {
          if (req.close) {
            req.close();
          } else {
            req.destroy(err);
          }
        });
        req.on("error", function handleRequestError(err) {
          reject(AxiosError_default.from(err, null, config, req));
        });
        const boundSockets = /* @__PURE__ */ new Set();
        req.on("socket", function handleRequestSocket(socket) {
          if (typeof socket.setKeepAlive === "function") {
            socket.setKeepAlive(true, 1e3 * 60);
          }
          if (!socket[kAxiosSocketListener]) {
            socket.on("error", handleSocketError);
            socket[kAxiosSocketListener] = true;
          }
          socket[kAxiosCurrentReq] = req;
          boundSockets.add(socket);
        });
        req.once("close", function clearCurrentReq() {
          clearConnectPhaseTimer();
          for (const socket of boundSockets) {
            if (socket[kAxiosCurrentReq] === req) {
              socket[kAxiosCurrentReq] = null;
            }
          }
          boundSockets.clear();
        });
        if (own2("timeout")) {
          const timeout = parseInt(own2("timeout"), 10);
          if (Number.isNaN(timeout)) {
            abort(
              new AxiosError_default(
                "error trying to parse `config.timeout` to int",
                AxiosError_default.ERR_BAD_OPTION_VALUE,
                config,
                req
              )
            );
            return;
          }
          const handleTimeout = function handleTimeout2() {
            if (isDone) return;
            abort(createTimeoutError());
          };
          if (isNativeTransport && timeout > 0) {
            connectPhaseTimer = setTimeout(handleTimeout, timeout);
          }
          req.setTimeout(timeout, handleTimeout);
        } else {
          req.setTimeout(0);
        }
        if (utils_default.isStream(data)) {
          let ended = false;
          let errored = false;
          data.on("end", () => {
            ended = true;
          });
          data.once("error", (err) => {
            errored = true;
            req.destroy(err);
          });
          data.on("close", () => {
            if (!ended && !errored) {
              abort(new CanceledError_default("Request stream has been aborted", config, req));
            }
          });
          let uploadStream = data;
          if (maxBodyLength > -1) {
            const limit = maxBodyLength;
            let bytesSent = 0;
            uploadStream = stream3.pipeline(
              [
                data,
                new stream3.Transform({
                  transform(chunk, _enc, cb) {
                    bytesSent += chunk.length;
                    if (bytesSent > limit) {
                      return cb(
                        new AxiosError_default(
                          "Request body larger than maxBodyLength limit",
                          AxiosError_default.ERR_BAD_REQUEST,
                          config,
                          req
                        )
                      );
                    }
                    cb(null, chunk);
                  }
                })
              ],
              utils_default.noop
            );
            uploadStream.on("error", (err) => {
              if (!req.destroyed) req.destroy(err);
            });
          }
          uploadStream.pipe(req);
        } else {
          data && req.write(data);
          req.end();
        }
      });
    };
  }
});

// node_modules/axios/lib/helpers/cookies.js
var decodeCookieValue, cookies_default;
var init_cookies = __esm({
  "node_modules/axios/lib/helpers/cookies.js"() {
    init_utils();
    init_platform();
    decodeCookieValue = (value) => {
      try {
        return decodeURIComponent(value);
      } catch (e) {
        return value;
      }
    };
    cookies_default = platform_default.hasStandardBrowserEnv ? (
      // Standard browser envs support document.cookie
      {
        write(name, value, expires, path, domain, secure, sameSite) {
          if (typeof document === "undefined") return;
          const cookie = [`${name}=${encodeURIComponent(value)}`];
          if (utils_default.isNumber(expires)) {
            cookie.push(`expires=${new Date(expires).toUTCString()}`);
          }
          if (utils_default.isString(path)) {
            cookie.push(`path=${path}`);
          }
          if (utils_default.isString(domain)) {
            cookie.push(`domain=${domain}`);
          }
          if (secure === true) {
            cookie.push("secure");
          }
          if (utils_default.isString(sameSite)) {
            cookie.push(`SameSite=${sameSite}`);
          }
          document.cookie = cookie.join("; ");
        },
        read(name) {
          if (typeof document === "undefined") return null;
          const cookieString = document.cookie;
          if (!cookieString) return null;
          const cookies = cookieString.split(";");
          for (let i = 0; i < cookies.length; i++) {
            const cookie = cookies[i].replace(/^\s+/, "");
            const eq = cookie.indexOf("=");
            if (eq !== -1 && cookie.slice(0, eq) === name) {
              return decodeCookieValue(cookie.slice(eq + 1));
            }
          }
          return null;
        },
        async readAsync(name) {
          try {
            const store = typeof window !== "undefined" && window.cookieStore;
            if (store && utils_default.isFunction(store.get)) {
              const cookie = await store.get(name);
              return cookie ? decodeCookieValue(cookie.value) : null;
            }
          } catch (e) {
          }
          return this.read(name);
        },
        remove(name) {
          this.write(name, "", Date.now() - 864e5, "/");
        }
      }
    ) : (
      // Non-standard browser env (web workers, react-native) lack needed support.
      {
        write() {
        },
        read() {
          return null;
        },
        async readAsync() {
          return null;
        },
        remove() {
        }
      }
    );
  }
});

// node_modules/axios/lib/helpers/isURLSameOrigin.js
var isURLSameOrigin_default;
var init_isURLSameOrigin = __esm({
  "node_modules/axios/lib/helpers/isURLSameOrigin.js"() {
    init_platform();
    isURLSameOrigin_default = platform_default.hasStandardBrowserEnv ? /* @__PURE__ */ ((origin2) => (url2) => {
      url2 = new URL(url2, document.baseURI || platform_default.origin);
      return origin2.protocol === url2.protocol && origin2.host === url2.host && origin2.port === url2.port;
    })(new URL(platform_default.origin)) : () => true;
  }
});

// node_modules/axios/lib/core/assertParamsAcyclic.js
function assertParamsAcyclic(params) {
  const pending = [{ value: params }];
  const active = /* @__PURE__ */ new WeakSet();
  const visited = /* @__PURE__ */ new WeakSet();
  while (pending.length) {
    const { value, done } = pending.pop();
    if (!utils_default.isPlainObject(value) && !utils_default.isArray(value)) continue;
    if (done) {
      active.delete(value);
      continue;
    }
    if (active.has(value)) {
      throw new AxiosError_default(
        "Circular reference detected in params",
        AxiosError_default.ERR_BAD_OPTION_VALUE
      );
    }
    if (visited.has(value)) continue;
    visited.add(value);
    active.add(value);
    pending.push({ value, done: true });
    const keys = Object.keys(value);
    if (Object.getOwnPropertySymbols && Object.getOwnPropertyDescriptor) {
      Object.getOwnPropertySymbols(value).forEach((key) => {
        const descriptor = Object.getOwnPropertyDescriptor(value, key);
        if (descriptor && descriptor.enumerable) keys.push(key);
      });
    }
    keys.forEach((key) => {
      if (key === "__proto__" || key === "constructor" || key === "prototype") {
        const descriptor = Object.getOwnPropertyDescriptor(value, key);
        if (descriptor && utils_default.hasOwnProp(descriptor, "value")) {
          pending.push({ value: descriptor.value });
        }
      } else {
        pending.push({ value: value[key] });
      }
    });
  }
}
var init_assertParamsAcyclic = __esm({
  "node_modules/axios/lib/core/assertParamsAcyclic.js"() {
    "use strict";
    init_utils();
    init_AxiosError();
  }
});

// node_modules/axios/lib/core/mergeConfig.js
function mergeParams(a, b) {
  const pending = [];
  const copies = /* @__PURE__ */ new WeakMap();
  const empty = {};
  function copy(target, source) {
    if (!utils_default.isPlainObject(source)) {
      return utils_default.isArray(source) ? source.slice() : source;
    }
    const base = utils_default.isPlainObject(target) ? target : empty;
    let bases = copies.get(source);
    if (!bases) {
      bases = /* @__PURE__ */ new WeakMap();
      copies.set(source, bases);
    }
    if (bases.has(base)) return bases.get(base);
    const result2 = {};
    bases.set(base, result2);
    pending.push({ base, source, result: result2 });
    return result2;
  }
  const result = utils_default.isUndefined(b) ? copy(void 0, a) : copy(a, b);
  while (pending.length) {
    const { base, source, result: target } = pending.pop();
    const baseKeys = new Set(ownEnumerableKeys(base));
    const sourceKeys = new Set(ownEnumerableKeys(source));
    const keys = /* @__PURE__ */ new Set([...baseKeys, ...sourceKeys]);
    keys.forEach((key) => {
      if (key === "__proto__" || key === "constructor" || key === "prototype") return;
      const hasSource = sourceKeys.has(key);
      const value = hasSource ? source[key] : void 0;
      const previous = (!hasSource || utils_default.isPlainObject(value)) && baseKeys.has(key) ? base[key] : void 0;
      target[key] = hasSource ? copy(previous, value) : copy(void 0, previous);
    });
  }
  assertParamsAcyclic(result);
  return result;
}
function getMergedValue(target, source, prop, caseless) {
  if (utils_default.isPlainObject(target) && utils_default.isPlainObject(source)) {
    return utils_default.merge.call({ caseless }, target, source);
  } else if (utils_default.isPlainObject(source)) {
    return utils_default.merge({}, source);
  } else if (utils_default.isArray(source)) {
    return source.slice();
  }
  return source;
}
function mergeDeepProperties(a, b, prop, caseless) {
  if (!utils_default.isUndefined(b)) {
    return getMergedValue(a, b, prop, caseless);
  } else if (!utils_default.isUndefined(a)) {
    return getMergedValue(void 0, a, prop, caseless);
  }
}
function valueFromConfig2(a, b) {
  if (!utils_default.isUndefined(b)) {
    return getMergedValue(void 0, b);
  }
}
function defaultToConfig2(a, b) {
  if (!utils_default.isUndefined(b)) {
    return getMergedValue(void 0, b);
  } else if (!utils_default.isUndefined(a)) {
    return getMergedValue(void 0, a);
  }
}
function getMergedTransitionalOption(config1, config2, prop) {
  const transitional2 = utils_default.hasOwnProp(config2, "transitional") ? config2.transitional : void 0;
  if (!utils_default.isUndefined(transitional2)) {
    if (utils_default.isPlainObject(transitional2)) {
      if (utils_default.hasOwnProp(transitional2, prop)) {
        return transitional2[prop];
      }
    } else {
      return void 0;
    }
  }
  const transitional1 = utils_default.hasOwnProp(config1, "transitional") ? config1.transitional : void 0;
  if (utils_default.isPlainObject(transitional1) && utils_default.hasOwnProp(transitional1, prop)) {
    return transitional1[prop];
  }
  return void 0;
}
function mergeDirectKeys(a, b, prop, config1, config2) {
  if (utils_default.hasOwnProp(config2, prop)) {
    return getMergedValue(a, b);
  } else if (utils_default.hasOwnProp(config1, prop)) {
    return getMergedValue(void 0, a);
  }
}
function mergeHeaders(a, b, prop) {
  return mergeDeepProperties(headersToObject(a), headersToObject(b), prop, true);
}
function mergeConfig(config1, config2) {
  config1 = config1 || {};
  config2 = config2 || {};
  const config = /* @__PURE__ */ Object.create(null);
  Object.defineProperty(config, "hasOwnProperty", hasOwnPropertyDescriptor);
  const keys = ownEnumerableKeys({ ...config1, ...config2 });
  for (let i = 0, len = keys.length; i < len; i++) {
    const prop = keys[i];
    if (prop === "__proto__" || prop === "constructor" || prop === "prototype") continue;
    const merge2 = utils_default.hasOwnProp(mergeMap, prop) ? mergeMap[prop] : mergeDeepProperties;
    const a = utils_default.hasOwnProp(config1, prop) ? config1[prop] : void 0;
    const b = utils_default.hasOwnProp(config2, prop) ? config2[prop] : void 0;
    const configValue = merge2 === mergeDirectKeys ? merge2(a, b, prop, config1, config2) : merge2(a, b, prop);
    utils_default.isUndefined(configValue) && merge2 !== mergeDirectKeys || (config[prop] = configValue);
  }
  if (utils_default.hasOwnProp(config2, "validateStatus") && utils_default.isUndefined(config2.validateStatus) && getMergedTransitionalOption(config1, config2, "validateStatusUndefinedResolves") === false) {
    if (utils_default.hasOwnProp(config1, "validateStatus")) {
      config.validateStatus = getMergedValue(void 0, config1.validateStatus);
    } else {
      delete config.validateStatus;
    }
  }
  return config;
}
var headersToObject, ownEnumerableKeys, hasOwnPropertyDescriptor, mergeMap;
var init_mergeConfig = __esm({
  "node_modules/axios/lib/core/mergeConfig.js"() {
    "use strict";
    init_utils();
    init_AxiosHeaders();
    init_assertParamsAcyclic();
    headersToObject = (thing) => thing instanceof AxiosHeaders_default ? { ...thing } : thing;
    ownEnumerableKeys = (thing) => {
      if (Object.getOwnPropertySymbols && Object.getOwnPropertyDescriptor) {
        return Object.keys(thing).concat(
          Object.getOwnPropertySymbols(thing).filter((symbol) => {
            const descriptor = Object.getOwnPropertyDescriptor(thing, symbol);
            return descriptor && descriptor.enumerable;
          })
        );
      }
      return Object.keys(thing);
    };
    hasOwnPropertyDescriptor = Object.freeze({
      __proto__: null,
      value: Object.prototype.hasOwnProperty,
      enumerable: false,
      writable: true,
      configurable: true
    });
    mergeMap = {
      url: valueFromConfig2,
      method: valueFromConfig2,
      data: valueFromConfig2,
      baseURL: defaultToConfig2,
      transformRequest: defaultToConfig2,
      transformResponse: defaultToConfig2,
      params: mergeParams,
      paramsSerializer: defaultToConfig2,
      timeout: defaultToConfig2,
      timeoutErrorMessage: defaultToConfig2,
      withCredentials: defaultToConfig2,
      withXSRFToken: defaultToConfig2,
      adapter: defaultToConfig2,
      responseType: defaultToConfig2,
      xsrfCookieName: defaultToConfig2,
      xsrfHeaderName: defaultToConfig2,
      onUploadProgress: defaultToConfig2,
      onDownloadProgress: defaultToConfig2,
      decompress: defaultToConfig2,
      maxContentLength: defaultToConfig2,
      maxBodyLength: defaultToConfig2,
      beforeRedirect: defaultToConfig2,
      transport: defaultToConfig2,
      httpAgent: defaultToConfig2,
      httpsAgent: defaultToConfig2,
      cancelToken: defaultToConfig2,
      socketPath: defaultToConfig2,
      allowedSocketPaths: defaultToConfig2,
      responseEncoding: defaultToConfig2,
      validateStatus: mergeDirectKeys,
      headers: mergeHeaders
    };
  }
});

// node_modules/axios/lib/helpers/resolveConfig.js
function resolveConfig(config, readXsrfCookie = readCookie) {
  const newConfig = mergeConfig({}, config);
  const own2 = (key) => utils_default.hasOwnProp(newConfig, key) ? newConfig[key] : void 0;
  const data = own2("data");
  let withXSRFToken = own2("withXSRFToken");
  const xsrfHeaderName = own2("xsrfHeaderName");
  const xsrfCookieName = own2("xsrfCookieName");
  let headers = own2("headers");
  const auth = own2("auth");
  const baseURL = own2("baseURL");
  const allowAbsoluteUrls = own2("allowAbsoluteUrls");
  const url2 = own2("url");
  newConfig.headers = headers = AxiosHeaders_default.from(headers);
  newConfig.url = buildURL(
    buildFullPath(baseURL, url2, allowAbsoluteUrls, newConfig),
    own2("params"),
    own2("paramsSerializer")
  );
  if (auth) {
    const username = utils_default.getSafeProp(auth, "username") || "";
    const password = utils_default.getSafeProp(auth, "password") || "";
    const transitional2 = own2("transitional");
    const encodeUsername = transitional2 && utils_default.hasOwnProp(transitional2, "encodeBasicAuthUsername") && transitional2.encodeBasicAuthUsername === true;
    try {
      headers.set(
        "Authorization",
        // Preserve legacy username bytes unless the caller opts in to UTF-8.
        "Basic " + btoa(
          (encodeUsername ? encodeUTF8(username) : username) + ":" + (password ? encodeUTF8(password) : "")
        )
      );
    } catch (e) {
      throw AxiosError_default.from(e, AxiosError_default.ERR_BAD_OPTION_VALUE, config);
    }
  }
  if (utils_default.isFormData(data)) {
    const getHeaders = utils_default.getSafeProp(data, "getHeaders");
    if (platform_default.hasStandardBrowserEnv || platform_default.hasStandardBrowserWebWorkerEnv || utils_default.isReactNative(data)) {
      headers.setContentType(void 0);
    } else if (utils_default.isFunction(getHeaders)) {
      setFormDataHeaders(headers, getHeaders.call(data), own2("formDataHeaderPolicy"));
    }
  }
  if (platform_default.hasStandardBrowserEnv) {
    if (utils_default.isFunction(withXSRFToken)) {
      withXSRFToken = withXSRFToken(newConfig);
    }
    const shouldSendXSRF = withXSRFToken === true || withXSRFToken == null && isURLSameOrigin_default(newConfig.url);
    if (shouldSendXSRF) {
      const xsrfValue = xsrfHeaderName && xsrfCookieName && readXsrfCookie(xsrfCookieName);
      if (xsrfValue) {
        headers.set(xsrfHeaderName, xsrfValue);
      }
    }
  }
  return newConfig;
}
var encodeUTF8, readCookie, resolveConfig_default;
var init_resolveConfig = __esm({
  "node_modules/axios/lib/helpers/resolveConfig.js"() {
    init_platform();
    init_utils();
    init_AxiosError();
    init_isURLSameOrigin();
    init_cookies();
    init_buildFullPath();
    init_mergeConfig();
    init_AxiosHeaders();
    init_setFormDataHeaders();
    init_buildURL();
    encodeUTF8 = (str) => encodeURIComponent(str).replace(
      /%([0-9A-F]{2})/gi,
      (_, hex) => String.fromCharCode(parseInt(hex, 16))
    );
    readCookie = (name) => cookies_default.read(name);
    resolveConfig_default = resolveConfig;
  }
});

// node_modules/axios/lib/adapters/xhr.js
var isXHRAdapterSupported, xhr_default;
var init_xhr = __esm({
  "node_modules/axios/lib/adapters/xhr.js"() {
    init_utils();
    init_settle();
    init_transitional();
    init_AxiosError();
    init_CanceledError();
    init_normalizeURLForProtocolCheck();
    init_parseProtocol();
    init_platform();
    init_AxiosHeaders();
    init_cookies();
    init_progressEventReducer();
    init_resolveConfig();
    init_sanitizeHeaderValue();
    isXHRAdapterSupported = typeof XMLHttpRequest !== "undefined";
    xhr_default = isXHRAdapterSupported && function(config) {
      return new Promise(function dispatchXhrRequest(resolve, reject) {
        let useAsyncXsrf;
        try {
          const store = platform_default.hasStandardBrowserEnv && typeof window !== "undefined" && window.cookieStore;
          useAsyncXsrf = store && utils_default.isFunction(store.get);
        } catch (e) {
        }
        let xsrfCookieName;
        const _config = resolveConfig_default(
          config,
          useAsyncXsrf ? (name) => {
            xsrfCookieName = name;
            return null;
          } : void 0
        );
        let requestData = _config.data;
        const requestHeaders = AxiosHeaders_default.from(_config.headers).normalize();
        let { responseType, onUploadProgress, onDownloadProgress } = _config;
        let onCanceled;
        let timeoutId;
        let uploadThrottled, downloadThrottled;
        let flushUpload, flushDownload, flushDownloadWithEvent;
        function done() {
          clearTimeout(timeoutId);
          try {
            flushUpload && flushUpload();
          } catch (err) {
            setTimeout(() => {
              throw err;
            });
          }
          try {
            flushDownload && flushDownload();
          } catch (err) {
            setTimeout(() => {
              throw err;
            });
          }
          if (request) {
            downloadThrottled && request.removeEventListener("progress", downloadThrottled);
            if (request.upload) {
              uploadThrottled && request.upload.removeEventListener("progress", uploadThrottled);
              flushUpload && request.upload.removeEventListener("loadend", flushUpload);
            }
          }
          _config.cancelToken && _config.cancelToken.unsubscribe(onCanceled);
          _config.signal && _config.signal.removeEventListener("abort", onCanceled);
        }
        let request = new XMLHttpRequest();
        request.open(_config.method.toUpperCase(), _config.url, true);
        request.timeout = _config.timeout;
        function onloadend(event) {
          if (!request) {
            return;
          }
          if (request.status === 0 && (parseProtocol(normalizeURLForProtocolCheck(_config.url)) || parseProtocol(platform_default.origin)) !== "file" && !(request.responseURL && request.responseURL.startsWith("file:"))) {
            reject(new AxiosError_default("Request aborted", AxiosError_default.ECONNABORTED, config, request));
            done();
            request = null;
            return;
          }
          try {
            if (event) {
              flushDownloadWithEvent && flushDownloadWithEvent(event);
            } else {
              flushDownload && flushDownload();
            }
          } catch (err) {
            setTimeout(() => {
              throw err;
            });
          }
          if (!request) {
            return;
          }
          const responseHeaders = AxiosHeaders_default.from(
            "getAllResponseHeaders" in request && request.getAllResponseHeaders()
          );
          const responseData = !responseType || responseType === "text" || responseType === "json" ? request.responseText : request.response;
          const response = {
            data: responseData,
            status: request.status,
            statusText: request.statusText,
            headers: responseHeaders,
            config,
            request
          };
          settle(
            function _resolve(value) {
              resolve(value);
              done();
            },
            function _reject(err) {
              reject(err);
              done();
            },
            response
          );
          request = null;
        }
        if ("onloadend" in request) {
          request.onloadend = onloadend;
        } else {
          request.onreadystatechange = function handleLoad() {
            if (!request || request.readyState !== 4) {
              return;
            }
            if (request.status === 0 && !(request.responseURL && request.responseURL.startsWith("file:"))) {
              return;
            }
            setTimeout(onloadend);
          };
        }
        request.onabort = function handleAbort() {
          if (!request) {
            return;
          }
          reject(new AxiosError_default("Request aborted", AxiosError_default.ECONNABORTED, config, request));
          done();
          request = null;
        };
        request.onerror = function handleError(event) {
          const msg = event && event.message ? event.message : "Network Error";
          const err = new AxiosError_default(msg, AxiosError_default.ERR_NETWORK, config, request);
          err.event = event || null;
          reject(err);
          done();
          request = null;
        };
        request.ontimeout = function handleTimeout() {
          if (!request) return;
          let timeoutErrorMessage = _config.timeout ? "timeout of " + _config.timeout + "ms exceeded" : "timeout exceeded";
          const transitional2 = _config.transitional || transitional_default;
          if (_config.timeoutErrorMessage) {
            timeoutErrorMessage = _config.timeoutErrorMessage;
          }
          reject(
            new AxiosError_default(
              timeoutErrorMessage,
              transitional2.clarifyTimeoutError ? AxiosError_default.ETIMEDOUT : AxiosError_default.ECONNABORTED,
              config,
              request
            )
          );
          done();
          request = null;
        };
        requestData === void 0 && requestHeaders.setContentType(null);
        if (!utils_default.isUndefined(_config.withCredentials)) {
          request.withCredentials = !!_config.withCredentials;
        }
        if (responseType && responseType !== "json") {
          request.responseType = _config.responseType;
        }
        if (onDownloadProgress) {
          [downloadThrottled, flushDownload, flushDownloadWithEvent] = progressEventReducer(
            onDownloadProgress,
            true
          );
          request.addEventListener("progress", downloadThrottled);
        }
        if (onUploadProgress && request.upload) {
          [uploadThrottled, flushUpload] = progressEventReducer(onUploadProgress);
          request.upload.addEventListener("progress", uploadThrottled);
          request.upload.addEventListener("loadend", flushUpload);
        }
        if (_config.cancelToken || _config.signal) {
          onCanceled = (cancel) => {
            if (!request) {
              return;
            }
            reject(!cancel || cancel.type ? new CanceledError_default(null, config, request) : cancel);
            const xhr = request;
            done();
            request = null;
            xhr.abort();
          };
          _config.cancelToken && _config.cancelToken.subscribe(onCanceled);
          if (!request) return;
          if (_config.signal) {
            _config.signal.aborted ? onCanceled() : _config.signal.addEventListener("abort", onCanceled);
          }
        }
        if (!request) return;
        const protocol = parseProtocol(_config.url);
        if (protocol && !platform_default.protocols.includes(protocol)) {
          reject(
            new AxiosError_default(
              "Unsupported protocol " + protocol + ":",
              AxiosError_default.ERR_BAD_REQUEST,
              config
            )
          );
          done();
          return;
        }
        const sendRequest = () => {
          if (!request) return;
          if ("setRequestHeader" in request) {
            utils_default.forEach(
              toByteStringHeaderObject(requestHeaders),
              function setRequestHeader(val, key) {
                request.setRequestHeader(key, val);
              }
            );
          }
          request.send(requestData || null);
        };
        if (xsrfCookieName) {
          const now = typeof performance !== "undefined" && utils_default.isFunction(performance.now) ? () => performance.now() : Date.now;
          const deadline = request.timeout > 0 ? now() + Number(request.timeout) : void 0;
          const abortOnTimeout = () => {
            if (!request) return;
            const xhr = request;
            xhr.ontimeout();
            xhr.abort();
          };
          if (deadline !== void 0) {
            const scheduleTimeout = () => {
              const remaining = deadline - now();
              if (remaining > 0) {
                timeoutId = setTimeout(scheduleTimeout, Math.min(remaining, 2147483647));
              } else {
                abortOnTimeout();
              }
            };
            scheduleTimeout();
          }
          cookies_default.readAsync(xsrfCookieName).then((value) => {
            if (!request) return;
            if (deadline !== void 0 && now() >= deadline) {
              abortOnTimeout();
              return;
            }
            if (value) {
              const headerName = utils_default.hasOwnProp(_config, "xsrfHeaderName") && _config.xsrfHeaderName;
              requestHeaders.set(headerName, value);
            }
            sendRequest();
          }).catch((error) => {
            if (!request) return;
            reject(AxiosError_default.from(error, null, config, request));
            done();
            request = null;
          });
        } else {
          sendRequest();
        }
      });
    };
  }
});

// node_modules/axios/lib/helpers/composeSignals.js
var composeSignals, composeSignals_default;
var init_composeSignals = __esm({
  "node_modules/axios/lib/helpers/composeSignals.js"() {
    init_CanceledError();
    init_AxiosError();
    init_utils();
    composeSignals = (signals, timeout) => {
      signals = signals ? signals.filter(Boolean) : [];
      if (!timeout && !signals.length) {
        return;
      }
      const controller = new AbortController();
      let aborted = false;
      const onabort = function(reason) {
        if (!aborted) {
          aborted = true;
          unsubscribe();
          const err = reason instanceof Error ? reason : this.reason;
          controller.abort(
            err instanceof AxiosError_default ? err : new CanceledError_default(err instanceof Error ? err.message : err)
          );
        }
      };
      let timer = timeout && setTimeout(() => {
        timer = null;
        onabort(new AxiosError_default(`timeout of ${timeout}ms exceeded`, AxiosError_default.ETIMEDOUT));
      }, timeout);
      const unsubscribe = () => {
        if (!signals) {
          return;
        }
        timer && clearTimeout(timer);
        timer = null;
        signals.forEach((signal2) => {
          const unsubscribeSignal = utils_default.getSafeProp(signal2, "unsubscribe");
          utils_default.isFunction(unsubscribeSignal) ? unsubscribeSignal.call(signal2, onabort) : signal2.removeEventListener("abort", onabort);
        });
        signals = null;
      };
      signals.forEach((signal2) => {
        if (aborted) {
          return;
        }
        if (signal2.aborted) {
          onabort.call(signal2);
          return;
        }
        signal2.addEventListener("abort", onabort, { once: true });
      });
      const { signal } = controller;
      signal.unsubscribe = () => utils_default.asap(unsubscribe);
      return signal;
    };
    composeSignals_default = composeSignals;
  }
});

// node_modules/axios/lib/helpers/trackStream.js
var streamChunk, readBytes, readStream, trackStream;
var init_trackStream = __esm({
  "node_modules/axios/lib/helpers/trackStream.js"() {
    streamChunk = function* (chunk, chunkSize) {
      let len = chunk.byteLength;
      if (!chunkSize || len < chunkSize) {
        yield chunk;
        return;
      }
      let pos = 0;
      let end;
      while (pos < len) {
        end = pos + chunkSize;
        yield chunk.slice(pos, end);
        pos = end;
      }
    };
    readBytes = async function* (iterable, chunkSize) {
      for await (const chunk of readStream(iterable)) {
        yield* streamChunk(chunk, chunkSize);
      }
    };
    readStream = async function* (stream4) {
      if (stream4[Symbol.asyncIterator]) {
        yield* stream4;
        return;
      }
      const reader = stream4.getReader();
      try {
        for (; ; ) {
          const { done, value } = await reader.read();
          if (done) {
            break;
          }
          yield value;
        }
      } finally {
        await reader.cancel();
      }
    };
    trackStream = (stream4, chunkSize, onProgress, onFinish) => {
      const iterator2 = readBytes(stream4, chunkSize);
      let bytes = 0;
      let done;
      let _onFinish = (e) => {
        if (!done) {
          done = true;
          onFinish && onFinish(e);
        }
      };
      return new ReadableStream(
        {
          async pull(controller) {
            try {
              const { done: done2, value } = await iterator2.next();
              if (done2) {
                _onFinish();
                controller.close();
                return;
              }
              let len = value.byteLength;
              if (onProgress) {
                let loadedBytes = bytes += len;
                onProgress(loadedBytes);
              }
              controller.enqueue(new Uint8Array(value));
            } catch (err) {
              _onFinish(err);
              throw err;
            }
          },
          cancel(reason) {
            _onFinish(reason);
            return iterator2.return();
          }
        },
        {
          highWaterMark: 2
        }
      );
    };
  }
});

// node_modules/axios/lib/helpers/formDataToBlob.js
function formDataToBlob(form, Blob2, limit, limitError, boundary) {
  if (!utils_default.isFunction(Blob2)) {
    throw new FormDataError("Blob is not supported", "blob");
  }
  if (!utils_default.isString(boundary) || !/^[A-Za-z0-9'()+_,./:=? -]{1,70}$/.test(boundary) || / $/.test(boundary)) {
    throw new FormDataError("Invalid multipart boundary", "boundary");
  }
  const footer = "--" + boundary + "--" + CRLF2;
  let length = footer.length;
  const parts = [];
  const checkLength = (extra) => {
    if (length + extra > limit) throw limitError();
  };
  checkLength(0);
  for (const [name, value] of form) {
    const isString2 = utils_default.isString(value);
    const valueLength = isString2 ? stringBytes(value, true) : value.size;
    if (!Number.isFinite(valueLength) || valueLength < 0) {
      throw new FormDataError("Unsupported FormData part size", "part");
    }
    checkLength(
      valueLength + stringBytes(name, true) + (!isString2 && value.name ? stringBytes(value.name, false) : 0) + (!isString2 && value.type ? stringBytes(value.type, false) : 0)
    );
    let header = "--" + boundary + CRLF2 + 'Content-Disposition: form-data; name="' + escapeName(normalizeLines(name)) + '"';
    if (!isString2) {
      header += '; filename="' + escapeName(utils_default.isString(value.name) ? value.name : "blob") + '"' + CRLF2 + "Content-Type: " + String(value.type || "application/octet-stream").replace(/[\r\n]/g, "");
    }
    header += CRLF2 + CRLF2;
    const partLength = stringBytes(header, false) + valueLength + CRLF2.length;
    checkLength(partLength);
    length += partLength;
    parts.push({ header, value, isString: isString2 });
  }
  const blobParts = [];
  for (const part of parts) {
    blobParts.push(part.header, part.isString ? normalizeLines(part.value) : part.value, CRLF2);
  }
  blobParts.push(footer);
  return new Blob2(blobParts, { type: "multipart/form-data; boundary=" + boundary });
}
var CRLF2, normalizeLines, escapeName, stringBytes, FormDataError;
var init_formDataToBlob = __esm({
  "node_modules/axios/lib/helpers/formDataToBlob.js"() {
    init_utils();
    CRLF2 = "\r\n";
    normalizeLines = (value) => String(value).replace(/\r\n|\r|\n/g, CRLF2);
    escapeName = (value) => String(value).replace(
      /[\r\n"]/g,
      (char) => char === "\r" ? "%0D" : char === "\n" ? "%0A" : "%22"
    );
    stringBytes = (value, normalize) => {
      let length = 0;
      for (let i = 0; i < value.length; i++) {
        const code = value.charCodeAt(i);
        if (normalize && (code === 13 || code === 10)) {
          if (code === 13 && value.charCodeAt(i + 1) === 10) i++;
          length += 2;
        } else if (code < 128) {
          length++;
        } else if (code < 2048) {
          length += 2;
        } else if (code >= 55296 && code <= 56319 && value.charCodeAt(i + 1) >= 56320 && value.charCodeAt(i + 1) <= 57343) {
          length += 4;
          i++;
        } else {
          length += 3;
        }
      }
      return length;
    };
    FormDataError = class extends TypeError {
      constructor(message, reason) {
        super(message);
        this.name = "FormDataError";
        this.reason = reason;
      }
    };
  }
});

// node_modules/axios/lib/core/createMultipartBody.js
function createMultipartBody(form, Blob2, limit, limitError, boundary) {
  if (!utils_default.isFunction(Blob2)) {
    throw new AxiosError_default("Blob is not supported", AxiosError_default.ERR_NOT_SUPPORT);
  }
  if (boundary === void 0) {
    if (utils_default.isFunction(platform_default.generateString)) {
      boundary = "axios-" + platform_default.generateString(36, "0123456789abcdef");
    } else {
      const crypto2 = utils_default.global.crypto;
      if (!crypto2 || !utils_default.isFunction(crypto2.getRandomValues)) {
        throw new AxiosError_default("Random byte generation is not supported", AxiosError_default.ERR_NOT_SUPPORT);
      }
      const random = new Uint8Array(18);
      crypto2.getRandomValues(random);
      boundary = "axios-" + Array.from(random, (value) => ("0" + value.toString(16)).slice(-2)).join("");
    }
  }
  try {
    return formDataToBlob(form, Blob2, limit, limitError, boundary);
  } catch (error) {
    if (error instanceof FormDataError) {
      const code = error.reason === "boundary" ? AxiosError_default.ERR_BAD_OPTION_VALUE : AxiosError_default.ERR_BAD_REQUEST;
      throw new AxiosError_default(error.message, code);
    }
    throw error;
  }
}
var init_createMultipartBody = __esm({
  "node_modules/axios/lib/core/createMultipartBody.js"() {
    init_utils();
    init_platform();
    init_AxiosError();
    init_formDataToBlob();
  }
});

// node_modules/axios/lib/adapters/fetch.js
var DEFAULT_CHUNK_SIZE, DEFAULT_REQUEST_OPTIONS, isFunction2, encodeUTF82, decodeURIComponentSafe2, test, maybeWithAuthCredentials, factory, seedCache, getFetch, adapter;
var init_fetch = __esm({
  "node_modules/axios/lib/adapters/fetch.js"() {
    init_platform();
    init_utils();
    init_AxiosError();
    init_composeSignals();
    init_trackStream();
    init_createMultipartBody();
    init_AxiosHeaders();
    init_progressEventReducer();
    init_resolveConfig();
    init_settle();
    init_estimateDataURLDecodedBytes();
    init_data();
    init_sanitizeHeaderValue();
    DEFAULT_CHUNK_SIZE = 64 * 1024;
    DEFAULT_REQUEST_OPTIONS = {
      cache: "default",
      redirect: "follow",
      referrer: "about:client",
      referrerPolicy: "",
      mode: "cors",
      integrity: "",
      keepalive: false,
      priority: "auto",
      window: null
    };
    ({ isFunction: isFunction2 } = utils_default);
    encodeUTF82 = (str) => encodeURIComponent(str).replace(
      /%([0-9A-F]{2})/gi,
      (_, hex) => String.fromCharCode(parseInt(hex, 16))
    );
    decodeURIComponentSafe2 = (value) => {
      if (!utils_default.isString(value)) {
        return value;
      }
      try {
        return decodeURIComponent(value);
      } catch (error) {
        return value;
      }
    };
    test = (fn, ...args) => {
      try {
        return !!fn(...args);
      } catch (e) {
        return false;
      }
    };
    maybeWithAuthCredentials = (url2) => {
      const protocolIndex = url2.indexOf("://");
      let urlToCheck = url2;
      if (protocolIndex !== -1) {
        urlToCheck = urlToCheck.slice(protocolIndex + 3);
      }
      return urlToCheck.includes("@") || urlToCheck.includes(":");
    };
    factory = (env) => {
      const globalObject = utils_default.global !== void 0 && utils_default.global !== null ? utils_default.global : globalThis;
      const { ReadableStream: ReadableStream2, TextEncoder: TextEncoder2 } = globalObject;
      env = utils_default.merge.call(
        {
          skipUndefined: true
        },
        {
          Request: globalObject.Request,
          Response: globalObject.Response,
          Blob: globalObject.Blob
        },
        env
      );
      const envFetch = utils_default.hasOwnProp(env, "fetch") ? env.fetch : void 0;
      const { Request, Response, Blob: Blob2 } = env;
      const isFetchSupported = envFetch ? isFunction2(envFetch) : typeof fetch === "function";
      const isRequestSupported = isFunction2(Request);
      const isResponseSupported = isFunction2(Response);
      if (!isFetchSupported) {
        return false;
      }
      const isReadableStreamSupported = isFetchSupported && isFunction2(ReadableStream2);
      const encodeText = isFetchSupported && (typeof TextEncoder2 === "function" ? /* @__PURE__ */ ((encoder) => (str) => encoder.encode(str))(new TextEncoder2()) : async (str) => new Uint8Array(await new Request(str).arrayBuffer()));
      const supportsRequestStream = isRequestSupported && isReadableStreamSupported && test(() => {
        let duplexAccessed = false;
        const request = new Request(platform_default.origin, {
          body: new ReadableStream2(),
          method: "POST",
          get duplex() {
            duplexAccessed = true;
            return "half";
          }
        });
        const hasContentType = request.headers.has("Content-Type");
        if (request.body != null) {
          request.body.cancel();
        }
        return duplexAccessed && !hasContentType;
      });
      const supportsResponseStream = isResponseSupported && isReadableStreamSupported && test(() => utils_default.isReadableStream(new Response("").body));
      const requestDefaults = Object.assign(/* @__PURE__ */ Object.create(null), DEFAULT_REQUEST_OPTIONS);
      if (isRequestSupported && !test(() => new Request(platform_default.origin, requestDefaults))) {
        delete requestDefaults.cache;
        if (!test(() => new Request(platform_default.origin, requestDefaults))) {
          requestDefaults.cache = DEFAULT_REQUEST_OPTIONS.cache;
        }
      }
      const resolvers = {
        stream: supportsResponseStream && ((res) => res.body)
      };
      isFetchSupported && (() => {
        ["text", "arrayBuffer", "blob", "formData", "stream"].forEach((type) => {
          !resolvers[type] && (resolvers[type] = (res, config) => {
            let method = res && res[type];
            if (method) {
              return method.call(res);
            }
            throw new AxiosError_default(
              `Response type '${type}' is not supported`,
              AxiosError_default.ERR_NOT_SUPPORT,
              config
            );
          });
        });
      })();
      const getBodyLength = async (body) => {
        if (body == null) {
          return 0;
        }
        if (utils_default.isBlob(body)) {
          return body.size;
        }
        if (utils_default.isArrayBufferView(body) || utils_default.isArrayBuffer(body)) {
          return body.byteLength;
        }
        if (utils_default.isURLSearchParams(body)) {
          body = body + "";
        }
        if (utils_default.isString(body)) {
          return (await encodeText(body)).byteLength;
        }
      };
      const resolveBodyLength = async (headers, body) => {
        const length = utils_default.toFiniteNumber(headers.getContentLength());
        return length == null ? getBodyLength(body) : length;
      };
      return async (config) => {
        let {
          url: url2,
          method,
          data,
          signal,
          cancelToken,
          timeout,
          onDownloadProgress,
          onUploadProgress,
          responseType,
          headers,
          withCredentials = "same-origin",
          fetchOptions,
          maxContentLength,
          maxBodyLength,
          maxRedirects
        } = resolveConfig_default(config);
        const hasMaxContentLength = utils_default.isNumber(maxContentLength) && maxContentLength > -1;
        const hasMaxBodyLength = utils_default.isNumber(maxBodyLength) && maxBodyLength > -1;
        const own2 = (key) => utils_default.hasOwnProp(config, key) ? config[key] : void 0;
        let _fetch = envFetch || fetch;
        responseType = responseType ? (responseType + "").toLowerCase() : "text";
        let composedSignal = composeSignals_default(
          [signal, cancelToken && cancelToken.toAbortSignal()],
          timeout
        );
        let request = null;
        const unsubscribe = composedSignal && composedSignal.unsubscribe && (() => {
          composedSignal.unsubscribe();
        });
        let requestContentLength;
        let pendingBodyError = null;
        let pendingResponseError = null;
        const maxBodyLengthError = () => new AxiosError_default(
          "Request body larger than maxBodyLength limit",
          AxiosError_default.ERR_BAD_REQUEST,
          config,
          request
        );
        try {
          let auth = void 0;
          const configAuth = own2("auth");
          if (configAuth) {
            const username = utils_default.getSafeProp(configAuth, "username") || "";
            const password = utils_default.getSafeProp(configAuth, "password") || "";
            auth = {
              username,
              password
            };
          }
          let parsedURL;
          if (maybeWithAuthCredentials(url2)) {
            parsedURL = new URL(url2, platform_default.origin);
            if (!auth && (parsedURL.username || parsedURL.password)) {
              const urlUsername = decodeURIComponentSafe2(parsedURL.username);
              const urlPassword = decodeURIComponentSafe2(parsedURL.password);
              auth = {
                username: urlUsername,
                password: urlPassword
              };
            }
            if (parsedURL.username || parsedURL.password) {
              parsedURL.username = "";
              parsedURL.password = "";
              url2 = parsedURL.href;
            }
          }
          if (auth) {
            headers.delete("authorization");
            headers.set(
              "Authorization",
              "Basic " + btoa(encodeUTF82((auth.username || "") + ":" + (auth.password || "")))
            );
          }
          if (hasMaxContentLength && parsedURL && parsedURL.protocol === "data:") {
            const estimated = estimateDataURLDecodedBytes(parsedURL.href);
            if (estimated > maxContentLength) {
              throw new AxiosError_default(
                "maxContentLength size of " + maxContentLength + " exceeded",
                AxiosError_default.ERR_BAD_RESPONSE,
                config,
                request
              );
            }
          }
          const formContentType = utils_default.isSpecCompliantForm(data) && AxiosHeaders_default.from(own2("headers")).getContentType();
          let formBoundary;
          if (utils_default.isString(formContentType) && /^multipart\/form-data(?:\s*;|$)/i.test(formContentType.trim())) {
            const parameters = AxiosHeaders_default.parseParameters(formContentType);
            if (utils_default.hasOwnProp(parameters, "boundary")) formBoundary = parameters.boundary;
          }
          if (utils_default.isSpecCompliantForm(data) && method !== "get" && method !== "head" && (hasMaxBodyLength || onUploadProgress && supportsRequestStream || formBoundary !== void 0)) {
            data = createMultipartBody(
              data,
              Blob2,
              hasMaxBodyLength ? maxBodyLength : Infinity,
              maxBodyLengthError,
              formBoundary
            );
            headers.setContentType(formBoundary === void 0 ? data.type : formContentType);
            requestContentLength = data.size;
          }
          if (hasMaxBodyLength && method !== "get" && method !== "head") {
            const outboundLength = await getBodyLength(data);
            if (typeof outboundLength === "number" && isFinite(outboundLength)) {
              requestContentLength = outboundLength;
              if (outboundLength > maxBodyLength) {
                throw maxBodyLengthError();
              }
            }
          }
          const mustEnforceStreamBody = hasMaxBodyLength && (utils_default.isReadableStream(data) || utils_default.isStream(data));
          const trackRequestStream = (stream4, onProgress, flush) => trackStream(
            stream4,
            DEFAULT_CHUNK_SIZE,
            (loadedBytes) => {
              if (hasMaxBodyLength && loadedBytes > maxBodyLength) {
                throw pendingBodyError = maxBodyLengthError();
              }
              onProgress && onProgress(loadedBytes);
            },
            flush
          );
          if (supportsRequestStream && method !== "get" && method !== "head" && (onUploadProgress || mustEnforceStreamBody)) {
            requestContentLength = requestContentLength == null ? await resolveBodyLength(headers, data) : requestContentLength;
            if (requestContentLength !== 0 || mustEnforceStreamBody) {
              let _request = new Request(url2, {
                method: "POST",
                body: data,
                duplex: "half"
              });
              let contentTypeHeader;
              if (utils_default.isFormData(data) && (contentTypeHeader = _request.headers.get("content-type"))) {
                headers.setContentType(contentTypeHeader);
              }
              if (_request.body) {
                const [onProgress, flush] = onUploadProgress && progressEventDecorator(
                  requestContentLength,
                  progressEventReducer(asyncDecorator(onUploadProgress))
                ) || [];
                data = trackRequestStream(_request.body, onProgress, flush);
              }
            }
          } else if (mustEnforceStreamBody && !isRequestSupported && isReadableStreamSupported && method !== "get" && method !== "head") {
            data = trackRequestStream(data);
          } else if (mustEnforceStreamBody && isRequestSupported && !supportsRequestStream && method !== "get" && method !== "head") {
            throw new AxiosError_default(
              "Stream request bodies are not supported by the current fetch implementation",
              AxiosError_default.ERR_NOT_SUPPORT,
              config,
              request
            );
          }
          if (!utils_default.isString(withCredentials)) {
            withCredentials = withCredentials ? "include" : "omit";
          }
          const isCredentialsSupported = isRequestSupported && "credentials" in Request.prototype;
          if (utils_default.isFormData(data)) {
            const contentType = headers.getContentType();
            if (contentType && /^multipart\/form-data/i.test(contentType) && !/boundary=/i.test(contentType)) {
              headers.delete("content-type");
            }
          }
          headers.set("User-Agent", "axios/" + VERSION, false);
          const safeFetchOptions = fetchOptions == null ? fetchOptions : Object.assign(/* @__PURE__ */ Object.create(null), fetchOptions);
          if (safeFetchOptions) {
            delete safeFetchOptions.body;
            delete safeFetchOptions.headers;
            delete safeFetchOptions.method;
            delete safeFetchOptions.signal;
            delete safeFetchOptions.duplex;
            delete safeFetchOptions.credentials;
          }
          const resolvedOptions = Object.assign(/* @__PURE__ */ Object.create(null), safeFetchOptions, {
            signal: composedSignal,
            method: method.toUpperCase(),
            headers: toByteStringHeaderObject(headers.normalize()),
            body: data,
            duplex: "half",
            credentials: isCredentialsSupported ? withCredentials : void 0
          });
          if (isRequestSupported) {
            utils_default.forEach(requestDefaults, (value, key) => {
              if (resolvedOptions[key] === void 0) {
                resolvedOptions[key] = value;
              }
            });
            if (resolvedOptions.signal === void 0) {
              resolvedOptions.signal = null;
            }
            if (resolvedOptions.body === void 0) {
              resolvedOptions.body = null;
            }
          }
          if (maxRedirects === 0) {
            resolvedOptions.redirect = "manual";
            if (safeFetchOptions) {
              safeFetchOptions.redirect = "manual";
            }
          }
          request = isRequestSupported && new Request(url2, resolvedOptions);
          let response = await (isRequestSupported ? _fetch(request, safeFetchOptions) : _fetch(url2, resolvedOptions));
          const responseHeaders = AxiosHeaders_default.from(response.headers);
          if (hasMaxContentLength) {
            const declaredLength = utils_default.toFiniteNumber(responseHeaders.getContentLength());
            if (declaredLength != null && declaredLength > maxContentLength) {
              throw new AxiosError_default(
                "maxContentLength size of " + maxContentLength + " exceeded",
                AxiosError_default.ERR_BAD_RESPONSE,
                config,
                request
              );
            }
          }
          const isStreamResponse = supportsResponseStream && (responseType === "stream" || responseType === "response");
          if (supportsResponseStream && response.body && (onDownloadProgress || hasMaxContentLength || isStreamResponse && unsubscribe)) {
            const options = {};
            ["status", "statusText", "headers"].forEach((prop) => {
              options[prop] = response[prop];
            });
            const responseContentLength = utils_default.toFiniteNumber(responseHeaders.getContentLength());
            const [onProgress, flush] = onDownloadProgress && progressEventDecorator(
              responseContentLength,
              progressEventReducer(asyncDecorator(onDownloadProgress), true)
            ) || [];
            let bytesRead = 0;
            const onChunkProgress = (loadedBytes) => {
              if (hasMaxContentLength) {
                bytesRead = loadedBytes;
                if (bytesRead > maxContentLength) {
                  throw pendingResponseError = new AxiosError_default(
                    "maxContentLength size of " + maxContentLength + " exceeded",
                    AxiosError_default.ERR_BAD_RESPONSE,
                    config,
                    request
                  );
                }
              }
              onProgress && onProgress(loadedBytes);
            };
            response = new Response(
              trackStream(response.body, DEFAULT_CHUNK_SIZE, onChunkProgress, () => {
                flush && flush();
                unsubscribe && unsubscribe();
              }),
              options
            );
          }
          responseType = responseType || "text";
          let responseData = await resolvers[utils_default.findKey(resolvers, responseType) || "text"](
            response,
            config
          );
          if (hasMaxContentLength && !supportsResponseStream && !isStreamResponse) {
            let materializedSize;
            if (responseData != null) {
              if (typeof responseData.byteLength === "number") {
                materializedSize = responseData.byteLength;
              } else if (typeof responseData.size === "number") {
                materializedSize = responseData.size;
              } else if (typeof responseData === "string") {
                materializedSize = typeof TextEncoder2 === "function" ? new TextEncoder2().encode(responseData).byteLength : responseData.length;
              }
            }
            if (typeof materializedSize === "number" && materializedSize > maxContentLength) {
              throw new AxiosError_default(
                "maxContentLength size of " + maxContentLength + " exceeded",
                AxiosError_default.ERR_BAD_RESPONSE,
                config,
                request
              );
            }
          }
          !isStreamResponse && unsubscribe && unsubscribe();
          return await new Promise((resolve, reject) => {
            settle(resolve, reject, {
              data: responseData,
              headers: AxiosHeaders_default.from(response.headers),
              status: response.status,
              statusText: response.statusText,
              config,
              request
            });
          });
        } catch (err) {
          unsubscribe && unsubscribe();
          if (composedSignal && composedSignal.aborted && composedSignal.reason instanceof AxiosError_default) {
            const canceledError = composedSignal.reason;
            canceledError.config = config;
            request && (canceledError.request = request);
            if (err !== canceledError) {
              Object.defineProperty(canceledError, "cause", {
                __proto__: null,
                value: err,
                writable: true,
                enumerable: false,
                configurable: true
              });
            }
            throw canceledError;
          }
          if (pendingBodyError) {
            request && !pendingBodyError.request && (pendingBodyError.request = request);
            throw pendingBodyError;
          }
          if (pendingResponseError) {
            throw pendingResponseError;
          }
          if (err instanceof AxiosError_default) {
            request && !err.request && (err.request = request);
            throw err;
          }
          if (err && err.name === "TypeError" && /Load failed|fetch/i.test(err.message)) {
            const networkError = new AxiosError_default(
              "Network Error",
              AxiosError_default.ERR_NETWORK,
              config,
              request,
              err && err.response
            );
            Object.defineProperty(networkError, "cause", {
              __proto__: null,
              value: err.cause || err,
              writable: true,
              enumerable: false,
              configurable: true
            });
            throw networkError;
          }
          throw AxiosError_default.from(err, err && err.code, config, request, err && err.response);
        }
      };
    };
    seedCache = /* @__PURE__ */ new Map();
    getFetch = (config) => {
      const env = /* @__PURE__ */ Object.create(null);
      const configuredEnv = config && utils_default.hasOwnProp(config, "env") && config.env;
      ["fetch", "Request", "Response", "Blob"].forEach((key) => {
        if (configuredEnv && utils_default.hasOwnProp(configuredEnv, key)) {
          env[key] = configuredEnv[key];
        }
      });
      const { fetch: fetch2, Request, Response, Blob: Blob2 } = env;
      const seeds = [Request, Response, fetch2, Blob2];
      let len = seeds.length, i = len, seed, target, map = seedCache;
      while (i--) {
        seed = seeds[i];
        target = map.get(seed);
        target === void 0 && map.set(seed, target = i ? /* @__PURE__ */ new Map() : factory(env));
        map = target;
      }
      return target;
    };
    adapter = getFetch();
  }
});

// node_modules/axios/lib/adapters/adapters.js
function getAdapter(adapters, config) {
  adapters = utils_default.isArray(adapters) ? adapters : [adapters];
  const { length } = adapters;
  let nameOrAdapter;
  let adapter2;
  const rejectedReasons = {};
  for (let i = 0; i < length; i++) {
    nameOrAdapter = adapters[i];
    let id;
    adapter2 = nameOrAdapter;
    if (!isResolvedHandle(nameOrAdapter)) {
      adapter2 = knownAdapters[(id = String(nameOrAdapter)).toLowerCase()];
      if (adapter2 === void 0) {
        throw new AxiosError_default(`Unknown adapter '${id}'`);
      }
    }
    if (adapter2 && (utils_default.isFunction(adapter2) || (adapter2 = adapter2.get(config)))) {
      break;
    }
    rejectedReasons[id || "#" + i] = adapter2;
  }
  if (!adapter2) {
    const reasons = Object.entries(rejectedReasons).map(
      ([id, state]) => `adapter ${id} ` + (state === false ? "is not supported by the environment" : "is not available in the build")
    );
    let s = length ? reasons.length > 1 ? "since :\n" + reasons.map(renderReason).join("\n") : " " + renderReason(reasons[0]) : "as no adapter specified";
    throw new AxiosError_default(
      `There is no suitable adapter to dispatch the request ` + s,
      AxiosError_default.ERR_NOT_SUPPORT
    );
  }
  return adapter2;
}
var knownAdapters, renderReason, isResolvedHandle, adapters_default;
var init_adapters = __esm({
  "node_modules/axios/lib/adapters/adapters.js"() {
    init_utils();
    init_http();
    init_xhr();
    init_fetch();
    init_AxiosError();
    knownAdapters = {
      http: http_default,
      xhr: xhr_default,
      fetch: {
        get: getFetch
      }
    };
    utils_default.forEach(knownAdapters, (fn, value) => {
      if (fn) {
        try {
          Object.defineProperty(fn, "name", { __proto__: null, value });
        } catch (e) {
        }
        Object.defineProperty(fn, "adapterName", { __proto__: null, value });
      }
    });
    renderReason = (reason) => `- ${reason}`;
    isResolvedHandle = (adapter2) => utils_default.isFunction(adapter2) || adapter2 === null || adapter2 === false;
    adapters_default = {
      /**
       * Resolve an adapter from a list of adapter names or functions.
       * @type {Function}
       */
      getAdapter,
      /**
       * Exposes all known adapters
       * @type {Object<string, Function|Object>}
       */
      adapters: knownAdapters
    };
  }
});

// node_modules/axios/lib/core/dispatchRequest.js
function throwIfCancellationRequested(config) {
  if (config.cancelToken) {
    config.cancelToken.throwIfRequested();
  }
  if (config.signal && config.signal.aborted) {
    throw new CanceledError_default(null, config);
  }
}
function elapsedSince(start) {
  return Math.max(0, Date.now() - start);
}
function dispatchRequest(_config) {
  const config = utils_default.toSafeFlatObject(_config);
  throwIfCancellationRequested(config);
  config.headers = AxiosHeaders_default.from(utils_default.getSafeProp(config, "headers"));
  config.data = transformData.call(config, config.transformRequest);
  if (["post", "put", "patch"].indexOf(config.method) !== -1) {
    config.headers.setContentType("application/x-www-form-urlencoded", false);
  }
  const adapter2 = adapters_default.getAdapter(config.adapter || defaults_default.adapter, config);
  const start = Date.now();
  return adapter2(config).then(
    function onAdapterResolution(response) {
      throwIfCancellationRequested(config);
      response.duration = elapsedSince(start);
      config.response = response;
      try {
        response.data = transformData.call(config, config.transformResponse, response);
      } finally {
        delete config.response;
      }
      response.headers = AxiosHeaders_default.from(response.headers);
      return response;
    },
    function onAdapterRejection(reason) {
      if (!isCancel(reason)) {
        throwIfCancellationRequested(config);
        if (reason && reason.response) {
          reason.response.duration = elapsedSince(start);
          config.response = reason.response;
          try {
            reason.response.data = transformData.call(
              config,
              config.transformResponse,
              reason.response
            );
          } finally {
            delete config.response;
          }
          reason.response.headers = AxiosHeaders_default.from(reason.response.headers);
        }
      }
      return Promise.reject(reason);
    }
  );
}
var init_dispatchRequest = __esm({
  "node_modules/axios/lib/core/dispatchRequest.js"() {
    "use strict";
    init_transformData();
    init_isCancel();
    init_defaults();
    init_CanceledError();
    init_AxiosHeaders();
    init_adapters();
    init_utils();
  }
});

// node_modules/axios/lib/helpers/validator.js
function assertOptions(options, schema, allowUnknown) {
  if (typeof options !== "object" || options === null) {
    throw new AxiosError_default("options must be an object", AxiosError_default.ERR_BAD_OPTION_VALUE);
  }
  const keys = Object.keys(options);
  let i = keys.length;
  while (i-- > 0) {
    const opt = keys[i];
    const validator = Object.prototype.hasOwnProperty.call(schema, opt) ? schema[opt] : void 0;
    if (validator) {
      const value = options[opt];
      const result = value === void 0 || validator(value, opt, options);
      if (result !== true) {
        throw new AxiosError_default(
          "option " + opt + " must be " + result,
          AxiosError_default.ERR_BAD_OPTION_VALUE
        );
      }
      continue;
    }
    if (allowUnknown !== true) {
      throw new AxiosError_default("Unknown option " + opt, AxiosError_default.ERR_BAD_OPTION);
    }
  }
}
var validators, deprecatedWarnings, validator_default;
var init_validator = __esm({
  "node_modules/axios/lib/helpers/validator.js"() {
    "use strict";
    init_data();
    init_AxiosError();
    validators = {};
    ["object", "boolean", "number", "function", "string", "symbol"].forEach((type, i) => {
      validators[type] = function validator(thing) {
        return typeof thing === type || "a" + (i < 1 ? "n " : " ") + type;
      };
    });
    deprecatedWarnings = {};
    validators.transitional = function transitional(validator, version, message) {
      function formatMessage(opt, desc) {
        return "[Axios v" + VERSION + "] Transitional option '" + opt + "'" + desc + (message ? ". " + message : "");
      }
      return (value, opt, opts) => {
        if (validator === false) {
          throw new AxiosError_default(
            formatMessage(opt, " has been removed" + (version ? " in " + version : "")),
            AxiosError_default.ERR_DEPRECATED
          );
        }
        if (version && !deprecatedWarnings[opt]) {
          deprecatedWarnings[opt] = true;
          console.warn(
            formatMessage(
              opt,
              " has been deprecated since v" + version + " and will be removed in the near future"
            )
          );
        }
        return validator ? validator(value, opt, opts) : true;
      };
    };
    validators.spelling = function spelling(correctSpelling) {
      return (value, opt) => {
        console.warn(`${opt} is likely a misspelling of ${correctSpelling}`);
        return true;
      };
    };
    validator_default = {
      assertOptions,
      validators
    };
  }
});

// node_modules/axios/lib/core/Axios.js
var validators2, isHeadersBucket, Axios, Axios_default;
var init_Axios = __esm({
  "node_modules/axios/lib/core/Axios.js"() {
    "use strict";
    init_utils();
    init_buildURL();
    init_InterceptorManager();
    init_dispatchRequest();
    init_mergeConfig();
    init_buildFullPath();
    init_methodList();
    init_validator();
    init_AxiosHeaders();
    init_transitional();
    init_AxiosError();
    validators2 = validator_default.validators;
    isHeadersBucket = (value) => {
      if (value == null || utils_default.isPlainObject(value) || value instanceof AxiosHeaders_default) {
        return true;
      }
      try {
        return utils_default.isObject(value) && utils_default.getSafeProp(value, Symbol.toStringTag) === "AxiosHeaders" && [
          "toJSON",
          "set",
          "get",
          "has",
          "delete",
          "clear",
          "normalize",
          "concat",
          "toString",
          Symbol.iterator
        ].every((method) => utils_default.isFunction(utils_default.getSafeProp(value, method)));
      } catch (e) {
        return false;
      }
    };
    Axios = class {
      constructor(instanceConfig) {
        this.defaults = instanceConfig || {};
        this.interceptors = {
          request: new InterceptorManager_default(),
          response: new InterceptorManager_default()
        };
      }
      /**
       * Dispatch a request
       *
       * @param {String|Object} configOrUrl The config specific for this request (merged with this.defaults)
       * @param {?Object} config
       *
       * @returns {Promise} The Promise to be fulfilled
       */
      async request(configOrUrl, config) {
        try {
          return await this._request(configOrUrl, config);
        } catch (err) {
          if (err instanceof Error) {
            try {
              let dummy = {};
              Error.captureStackTrace ? Error.captureStackTrace(dummy) : dummy = new Error();
              const dummyStack = dummy.stack;
              let stack = "";
              if (typeof dummyStack === "string") {
                const firstNewlineIndex = dummyStack.indexOf("\n");
                stack = firstNewlineIndex === -1 ? "" : dummyStack.slice(firstNewlineIndex + 1);
              }
              if (!err.stack) {
                err.stack = stack;
              } else if (stack) {
                const firstNewlineIndex = stack.indexOf("\n");
                const secondNewlineIndex = firstNewlineIndex === -1 ? -1 : stack.indexOf("\n", firstNewlineIndex + 1);
                const duplicateCheckSuffix = secondNewlineIndex === -1 ? stack : stack.slice(secondNewlineIndex + 1);
                const currentStack = String(err.stack);
                const causeIndex = getCauseStackIndex(err, currentStack);
                const stackHead = causeIndex === -1 ? currentStack : currentStack.slice(0, causeIndex);
                const causeSection = causeIndex === -1 ? "" : currentStack.slice(causeIndex);
                if (!stackHead.endsWith(duplicateCheckSuffix)) {
                  err.stack = stackHead + "\n" + stack + causeSection;
                }
              }
            } catch (e) {
            }
          }
          throw err;
        }
      }
      _request(configOrUrl, config) {
        if (typeof configOrUrl === "string") {
          config = config || {};
          config.url = configOrUrl;
        } else {
          config = configOrUrl || {};
        }
        config = mergeConfig(this.defaults, config);
        const { transitional: transitional2, paramsSerializer, headers } = config;
        if (transitional2 !== void 0) {
          validator_default.assertOptions(
            transitional2,
            {
              silentJSONParsing: validators2.transitional(validators2.boolean),
              forcedJSONParsing: validators2.transitional(validators2.boolean),
              clarifyTimeoutError: validators2.transitional(validators2.boolean),
              legacyInterceptorReqResOrdering: validators2.transitional(validators2.boolean),
              advertiseZstdAcceptEncoding: validators2.transitional(validators2.boolean),
              validateStatusUndefinedResolves: validators2.transitional(validators2.boolean),
              encodeBasicAuthUsername: validators2.transitional(validators2.boolean)
            },
            false
          );
        }
        if (paramsSerializer != null) {
          if (utils_default.isFunction(paramsSerializer)) {
            config.paramsSerializer = {
              serialize: paramsSerializer
            };
          } else {
            validator_default.assertOptions(
              paramsSerializer,
              {
                encode: validators2.function,
                serialize: validators2.function
              },
              true
            );
          }
        }
        if (config.allowAbsoluteUrls !== void 0) {
        } else if (utils_default.hasOwnProp(this.defaults, "allowAbsoluteUrls") && this.defaults.allowAbsoluteUrls !== void 0) {
          config.allowAbsoluteUrls = this.defaults.allowAbsoluteUrls;
        } else {
          config.allowAbsoluteUrls = true;
        }
        validator_default.assertOptions(
          config,
          {
            baseUrl: validators2.spelling("baseURL"),
            withXsrfToken: validators2.spelling("withXSRFToken")
          },
          true
        );
        config.method = (utils_default.getSafeProp(config, "method") || utils_default.getSafeProp(this.defaults, "method") || "get").toLowerCase();
        const commonHeaders = utils_default.getSafeProp(headers, "common");
        const methodHeaders = utils_default.getSafeProp(headers, config.method);
        const contextHeaders = headers && utils_default.merge(
          isHeadersBucket(commonHeaders) ? commonHeaders : void 0,
          isHeadersBucket(methodHeaders) ? methodHeaders : void 0
        );
        headers && utils_default.forEach(methodList_default.concat("common"), (method) => {
          if (isHeadersBucket(utils_default.getSafeProp(headers, method))) {
            delete headers[method];
          }
        });
        config.headers = AxiosHeaders_default.concat(contextHeaders, headers);
        const requestInterceptorChain = [];
        let synchronousRequestInterceptors = true;
        this.interceptors.request.forEach(function unshiftRequestInterceptors(interceptor) {
          if (typeof interceptor.runWhen === "function" && interceptor.runWhen(config) === false) {
            return;
          }
          synchronousRequestInterceptors = synchronousRequestInterceptors && interceptor.synchronous;
          const transitional3 = config.transitional || transitional_default;
          const legacyInterceptorReqResOrdering = transitional3 && transitional3.legacyInterceptorReqResOrdering;
          if (legacyInterceptorReqResOrdering) {
            requestInterceptorChain.unshift(interceptor.fulfilled, interceptor.rejected);
          } else {
            requestInterceptorChain.push(interceptor.fulfilled, interceptor.rejected);
          }
        });
        const responseInterceptorChain = [];
        this.interceptors.response.forEach(function pushResponseInterceptors(interceptor) {
          responseInterceptorChain.push(interceptor.fulfilled, interceptor.rejected);
        });
        let promise;
        let i = 0;
        let len;
        if (!synchronousRequestInterceptors) {
          const chain = [dispatchRequest.bind(this), void 0];
          chain.unshift(...requestInterceptorChain);
          chain.push(...responseInterceptorChain);
          len = chain.length;
          promise = Promise.resolve(config);
          while (i < len) {
            promise = promise.then(chain[i++], chain[i++]);
          }
          return promise;
        }
        len = requestInterceptorChain.length;
        let newConfig = config;
        while (i < len) {
          const onFulfilled = requestInterceptorChain[i++];
          const onRejected = requestInterceptorChain[i++];
          try {
            newConfig = onFulfilled ? onFulfilled(newConfig) : newConfig;
          } catch (error) {
            if (!onRejected) {
              promise = Promise.reject(error);
              break;
            }
            try {
              const rejectedResult = onRejected.call(this, error);
              if (utils_default.isThenable(rejectedResult)) {
                promise = Promise.resolve(rejectedResult).then(
                  () => dispatchRequest.call(this, newConfig)
                );
              }
            } catch (rejectedError) {
              promise = Promise.reject(rejectedError);
            }
            break;
          }
        }
        if (!promise) {
          try {
            promise = dispatchRequest.call(this, newConfig);
          } catch (error) {
            promise = Promise.reject(error);
          }
        }
        i = 0;
        len = responseInterceptorChain.length;
        while (i < len) {
          promise = promise.then(responseInterceptorChain[i++], responseInterceptorChain[i++]);
        }
        return promise;
      }
      getUri(config) {
        config = mergeConfig(this.defaults, config);
        const fullPath = buildFullPath(config.baseURL, config.url, config.allowAbsoluteUrls, config);
        return buildURL(fullPath, config.params, config.paramsSerializer);
      }
    };
    utils_default.forEach(["delete", "get", "head", "options"], function forEachMethodNoData(method) {
      Axios.prototype[method] = async function(url2, config) {
        return this.request(
          mergeConfig(config || {}, {
            method,
            url: url2,
            data: config && utils_default.hasOwnProp(config, "data") ? config.data : void 0
          })
        );
      };
    });
    utils_default.forEach(["post", "put", "patch", "query"], function forEachMethodWithData(method) {
      function generateHTTPMethod(isForm) {
        return async function httpMethod(url2, data, config) {
          return this.request(
            mergeConfig(config || {}, {
              method,
              headers: isForm ? {
                "Content-Type": "multipart/form-data"
              } : {},
              url: url2,
              data
            })
          );
        };
      }
      Axios.prototype[method] = generateHTTPMethod();
      if (method !== "query") {
        Axios.prototype[method + "Form"] = generateHTTPMethod(true);
      }
    });
    Axios_default = Axios;
  }
});

// node_modules/axios/lib/cancel/CancelToken.js
var CancelToken, CancelToken_default;
var init_CancelToken = __esm({
  "node_modules/axios/lib/cancel/CancelToken.js"() {
    "use strict";
    init_CanceledError();
    CancelToken = class _CancelToken {
      constructor(executor) {
        if (typeof executor !== "function") {
          throw new TypeError("executor must be a function.");
        }
        let resolvePromise;
        this.promise = new Promise(function promiseExecutor(resolve) {
          resolvePromise = resolve;
        });
        const token = this;
        this.promise.then((cancel) => {
          if (!token._listeners) return;
          let i = token._listeners.length;
          while (i-- > 0) {
            token._listeners[i](cancel);
          }
          token._listeners = null;
        });
        this.promise.then = (onfulfilled) => {
          let _resolve;
          const promise = new Promise((resolve) => {
            token.subscribe(resolve);
            _resolve = resolve;
          }).then(onfulfilled);
          promise.cancel = function reject() {
            token.unsubscribe(_resolve);
          };
          return promise;
        };
        executor(function cancel(message, config, request) {
          if (token.reason) {
            return;
          }
          token.reason = new CanceledError_default(message, config, request);
          resolvePromise(token.reason);
        });
      }
      /**
       * Throws a `CanceledError` if cancellation has been requested.
       */
      throwIfRequested() {
        if (this.reason) {
          throw this.reason;
        }
      }
      /**
       * Subscribe to the cancel signal
       */
      subscribe(listener) {
        if (this.reason) {
          listener(this.reason);
          return;
        }
        if (this._listeners) {
          this._listeners.push(listener);
        } else {
          this._listeners = [listener];
        }
      }
      /**
       * Unsubscribe from the cancel signal
       */
      unsubscribe(listener) {
        if (!this._listeners) {
          return;
        }
        const index = this._listeners.indexOf(listener);
        if (index !== -1) {
          this._listeners.splice(index, 1);
        }
      }
      toAbortSignal() {
        const controller = new AbortController();
        const abort = (err) => {
          controller.abort(err);
        };
        this.subscribe(abort);
        controller.signal.unsubscribe = () => this.unsubscribe(abort);
        return controller.signal;
      }
      /**
       * Returns an object that contains a new `CancelToken` and a function that, when called,
       * cancels the `CancelToken`.
       */
      static source() {
        let cancel;
        const token = new _CancelToken(function executor(c) {
          cancel = c;
        });
        return {
          token,
          cancel
        };
      }
    };
    CancelToken_default = CancelToken;
  }
});

// node_modules/axios/lib/helpers/spread.js
function spread(callback) {
  return function wrap(arr) {
    return callback.apply(null, arr);
  };
}
var init_spread = __esm({
  "node_modules/axios/lib/helpers/spread.js"() {
    "use strict";
  }
});

// node_modules/axios/lib/helpers/isAxiosError.js
function isAxiosError(payload) {
  return utils_default.isObject(payload) && payload.isAxiosError === true;
}
var init_isAxiosError = __esm({
  "node_modules/axios/lib/helpers/isAxiosError.js"() {
    "use strict";
    init_utils();
  }
});

// node_modules/axios/lib/helpers/HttpStatusCode.js
var HttpStatusCode, HttpStatusCode_default;
var init_HttpStatusCode = __esm({
  "node_modules/axios/lib/helpers/HttpStatusCode.js"() {
    HttpStatusCode = {
      Continue: 100,
      SwitchingProtocols: 101,
      Processing: 102,
      EarlyHints: 103,
      Ok: 200,
      Created: 201,
      Accepted: 202,
      NonAuthoritativeInformation: 203,
      NoContent: 204,
      ResetContent: 205,
      PartialContent: 206,
      MultiStatus: 207,
      AlreadyReported: 208,
      ImUsed: 226,
      MultipleChoices: 300,
      MovedPermanently: 301,
      Found: 302,
      SeeOther: 303,
      NotModified: 304,
      UseProxy: 305,
      Unused: 306,
      TemporaryRedirect: 307,
      PermanentRedirect: 308,
      BadRequest: 400,
      Unauthorized: 401,
      PaymentRequired: 402,
      Forbidden: 403,
      NotFound: 404,
      MethodNotAllowed: 405,
      NotAcceptable: 406,
      ProxyAuthenticationRequired: 407,
      RequestTimeout: 408,
      Conflict: 409,
      Gone: 410,
      LengthRequired: 411,
      PreconditionFailed: 412,
      /**
       * @deprecated Use `ContentTooLarge` instead.
       */
      PayloadTooLarge: 413,
      ContentTooLarge: 413,
      UriTooLong: 414,
      UnsupportedMediaType: 415,
      RangeNotSatisfiable: 416,
      ExpectationFailed: 417,
      ImATeapot: 418,
      MisdirectedRequest: 421,
      /**
       * @deprecated Use `UnprocessableContent` instead.
       */
      UnprocessableEntity: 422,
      UnprocessableContent: 422,
      Locked: 423,
      FailedDependency: 424,
      TooEarly: 425,
      UpgradeRequired: 426,
      PreconditionRequired: 428,
      TooManyRequests: 429,
      RequestHeaderFieldsTooLarge: 431,
      UnavailableForLegalReasons: 451,
      InternalServerError: 500,
      NotImplemented: 501,
      BadGateway: 502,
      ServiceUnavailable: 503,
      GatewayTimeout: 504,
      HttpVersionNotSupported: 505,
      VariantAlsoNegotiates: 506,
      InsufficientStorage: 507,
      LoopDetected: 508,
      NotExtended: 510,
      NetworkAuthenticationRequired: 511,
      WebServerReturnsAnUnknownError: 520,
      WebServerIsDown: 521,
      ConnectionTimedOut: 522,
      OriginIsUnreachable: 523,
      TimeoutOccurred: 524,
      SslHandshakeFailed: 525,
      InvalidSslCertificate: 526
    };
    Object.entries(HttpStatusCode).forEach(([key, value]) => {
      if (HttpStatusCode[value] === void 0) {
        HttpStatusCode[value] = key;
      }
    });
    HttpStatusCode_default = HttpStatusCode;
  }
});

// node_modules/axios/lib/axios.js
function createInstance(defaultConfig2) {
  const context = new Axios_default(defaultConfig2);
  const instance = bind(Axios_default.prototype.request, context);
  utils_default.extend(instance, Axios_default.prototype, context, { allOwnKeys: true });
  utils_default.extend(instance, context, null, { allOwnKeys: true });
  instance.create = function create2(instanceConfig) {
    return createInstance(mergeConfig(defaultConfig2, instanceConfig));
  };
  return instance;
}
var axios, axios_default;
var init_axios = __esm({
  "node_modules/axios/lib/axios.js"() {
    "use strict";
    init_utils();
    init_bind();
    init_Axios();
    init_mergeConfig();
    init_defaults();
    init_formDataToJSON();
    init_CanceledError();
    init_CancelToken();
    init_isCancel();
    init_data();
    init_toFormData();
    init_AxiosError();
    init_spread();
    init_isAxiosError();
    init_AxiosHeaders();
    init_adapters();
    init_HttpStatusCode();
    axios = createInstance(defaults_default);
    axios.Axios = Axios_default;
    axios.CanceledError = CanceledError_default;
    axios.CancelToken = CancelToken_default;
    axios.isCancel = isCancel;
    axios.VERSION = VERSION;
    axios.toFormData = toFormData_default;
    axios.AxiosError = AxiosError_default;
    axios.Cancel = axios.CanceledError;
    axios.all = function all(promises) {
      return Promise.all(promises);
    };
    axios.spread = spread;
    axios.isAxiosError = isAxiosError;
    axios.mergeConfig = mergeConfig;
    axios.AxiosHeaders = AxiosHeaders_default;
    axios.formToJSON = (thing) => formDataToJSON_default(utils_default.isHTMLForm(thing) ? new FormData(thing) : thing);
    axios.getAdapter = adapters_default.getAdapter;
    axios.HttpStatusCode = HttpStatusCode_default;
    axios.default = axios;
    axios_default = axios;
  }
});

// node_modules/axios/index.js
var Axios2, AxiosError2, CanceledError2, isCancel2, CancelToken2, VERSION2, all2, Cancel, isAxiosError2, spread2, toFormData2, AxiosHeaders2, HttpStatusCode2, formToJSON, getAdapter2, mergeConfig2, create;
var init_axios2 = __esm({
  "node_modules/axios/index.js"() {
    init_axios();
    ({
      Axios: Axios2,
      AxiosError: AxiosError2,
      CanceledError: CanceledError2,
      isCancel: isCancel2,
      CancelToken: CancelToken2,
      VERSION: VERSION2,
      all: all2,
      Cancel,
      isAxiosError: isAxiosError2,
      spread: spread2,
      toFormData: toFormData2,
      AxiosHeaders: AxiosHeaders2,
      HttpStatusCode: HttpStatusCode2,
      formToJSON,
      getAdapter: getAdapter2,
      mergeConfig: mergeConfig2,
      create
    } = axios_default);
  }
});

// node_modules/dingtalk-stream/dist/client.mjs
import EventEmitter2 from "events";
var EventAck, defaultConfig, DWClient;
var init_client = __esm({
  "node_modules/dingtalk-stream/dist/client.mjs"() {
    init_constants();
    init_wrapper();
    init_axios2();
    EventAck = /* @__PURE__ */ ((EventAck2) => {
      EventAck2["SUCCESS"] = "SUCCESS";
      EventAck2["LATER"] = "LATER";
      return EventAck2;
    })(EventAck || {});
    defaultConfig = {
      autoReconnect: true,
      keepAlive: false,
      ua: "",
      subscriptions: [
        {
          type: "EVENT",
          topic: "*"
        }
      ]
    };
    DWClient = class extends EventEmitter2 {
      debug = false;
      connected = false;
      registered = false;
      reconnecting = false;
      userDisconnect = false;
      reconnectBaseInterval = 1e3;
      reconnectMaxInterval = 6e4;
      reconnectAttempts = 0;
      heartbeat_interval = 8e3;
      heartbeatIntervallId;
      reconnectTimerId;
      isConnecting = false;
      sslopts = { rejectUnauthorized: true };
      config;
      socket;
      dw_url;
      isAlive = false;
      onEventReceived = (msg) => {
        return {
          status: "SUCCESS"
          /* SUCCESS */
        };
      };
      constructor(opts) {
        super();
        this.config = {
          ...defaultConfig,
          ...opts
        };
        if (!this.config.clientId || !this.config.clientSecret) {
          console.error("clientId or clientSecret is null");
          throw new Error("clientId or clientSecret is null");
        }
        if (this.config.debug !== void 0) {
          this.debug = this.config.debug;
        }
      }
      getConfig() {
        return { ...this.config };
      }
      printDebug(msg) {
        if (this.debug) {
          const date = "[" + (/* @__PURE__ */ new Date()).toISOString() + "]";
          console.info(date, msg);
        }
      }
      registerAllEventListener(onEventReceived) {
        this.onEventReceived = onEventReceived;
        return this;
      }
      registerCallbackListener(eventId, callback) {
        if (!eventId || !callback) {
          console.error(
            "registerCallbackListener: eventId and callback must be defined"
          );
          throw new Error(
            "registerCallbackListener: eventId and callback must be defined"
          );
        }
        if (!this.config.subscriptions.find(
          (x) => x.topic === eventId && x.type === "CALLBACK"
        )) {
          this.config.subscriptions.push({
            type: "CALLBACK",
            topic: eventId
          });
        }
        this.on(eventId, callback);
        return this;
      }
      async getAccessToken() {
        const result = await axios_default.get(
          `${GET_TOKEN_URL}?appkey=${this.config.clientId}&appsecret=${this.config.clientSecret}`
        );
        if (result.status === 200 && result.data.access_token) {
          this.config.access_token = result.data.access_token;
          return result.data.access_token;
        } else {
          throw new Error("getAccessToken: get access_token failed");
        }
      }
      async getEndpoint() {
        this.printDebug("get connect endpoint by config");
        this.printDebug(this.config);
        const res = await axios_default({
          url: GATEWAY_URL,
          method: "POST",
          responseType: "json",
          data: {
            clientId: this.config.clientId,
            clientSecret: this.config.clientSecret,
            ua: this.config.ua,
            subscriptions: this.config.subscriptions
          },
          headers: {
            // 这个接口得加个，否则默认返回的会是xml
            Accept: "application/json"
          }
        });
        this.printDebug("res.data " + JSON.stringify(res.data));
        if (res.data) {
          this.config.endpoint = res.data;
          const { endpoint, ticket } = res.data;
          if (!endpoint || !ticket) {
            this.printDebug("endpoint or ticket is null");
            throw new Error("endpoint or ticket is null");
          }
          this.dw_url = `${endpoint}?ticket=${ticket}`;
          return this;
        } else {
          throw new Error("build: get endpoint failed");
        }
      }
      cleanup() {
        if (this.heartbeatIntervallId !== void 0) {
          clearInterval(this.heartbeatIntervallId);
          this.heartbeatIntervallId = void 0;
        }
        if (this.socket) {
          this.socket.removeAllListeners();
          if (this.socket.readyState === wrapper_default.OPEN || this.socket.readyState === wrapper_default.CONNECTING) {
            this.socket.terminate();
          }
          this.socket = void 0;
        }
      }
      scheduleReconnect() {
        if (!this.config.autoReconnect || this.userDisconnect || this.isConnecting) {
          return;
        }
        const delay = Math.min(
          this.reconnectBaseInterval * Math.pow(2, this.reconnectAttempts) + Math.random() * 1e3,
          this.reconnectMaxInterval
        );
        this.reconnecting = true;
        this.printDebug("Reconnecting in " + (delay / 1e3).toFixed(1) + " seconds... (attempt " + (this.reconnectAttempts + 1) + ")");
        if (this.reconnectTimerId) {
          clearTimeout(this.reconnectTimerId);
        }
        this.reconnectTimerId = setTimeout(() => {
          this.reconnectTimerId = void 0;
          this.connect();
        }, delay);
      }
      _connect() {
        return new Promise((resolve, reject) => {
          this.printDebug("Connecting to dingtalk websocket @ " + this.dw_url);
          try {
            this.socket = new wrapper_default(this.dw_url, this.sslopts);
          } catch (err) {
            this.printDebug("WebSocket constructor error");
            console.warn("ERROR", err);
            reject(err);
            return;
          }
          let settled = false;
          this.socket.on("open", () => {
            this.connected = true;
            this.reconnectAttempts = 0;
            console.info("[" + (/* @__PURE__ */ new Date()).toISOString() + "] connect success");
            if (this.config.keepAlive) {
              this.isAlive = true;
              this.heartbeatIntervallId = setInterval(() => {
                var _a, _b;
                if (this.isAlive === false) {
                  console.error(
                    "TERMINATE SOCKET: Ping Pong does not transfer heartbeat within heartbeat intervall"
                  );
                  return (_a = this.socket) == null ? void 0 : _a.terminate();
                }
                this.isAlive = false;
                (_b = this.socket) == null ? void 0 : _b.ping("", true);
              }, this.heartbeat_interval);
            }
            settled = true;
            resolve();
          });
          this.socket.on("pong", () => {
            this.heartbeat();
          });
          this.socket.on("message", (data) => {
            this.onDownStream(data);
          });
          this.socket.on("close", () => {
            this.printDebug("Socket closed");
            this.connected = false;
            this.registered = false;
            if (this.heartbeatIntervallId !== void 0) {
              clearInterval(this.heartbeatIntervallId);
              this.heartbeatIntervallId = void 0;
            }
            if (settled) {
              this.scheduleReconnect();
            }
          });
          this.socket.on("error", (err) => {
            var _a;
            this.printDebug("SOCKET ERROR");
            console.warn("ERROR", err);
            (_a = this.socket) == null ? void 0 : _a.terminate();
            if (!settled) {
              settled = true;
              reject(err);
            }
          });
        });
      }
      async connect() {
        if (this.isConnecting) {
          this.printDebug("connect() already in progress, skipping");
          return;
        }
        this.userDisconnect = false;
        this.isConnecting = true;
        try {
          this.cleanup();
          await this.getEndpoint();
          if (this.userDisconnect)
            return;
          await this._connect();
        } catch (err) {
          this.printDebug("Connect failed: " + (err instanceof Error ? err.message : String(err)));
          if (!this.userDisconnect) {
            this.reconnectAttempts++;
            this.isConnecting = false;
            this.scheduleReconnect();
          }
          return;
        } finally {
          this.isConnecting = false;
        }
      }
      disconnect() {
        console.info("Disconnecting.");
        this.userDisconnect = true;
        if (this.reconnectTimerId) {
          clearTimeout(this.reconnectTimerId);
          this.reconnectTimerId = void 0;
        }
        this.reconnecting = false;
        this.reconnectAttempts = 0;
        this.cleanup();
        this.connected = false;
        this.registered = false;
      }
      heartbeat() {
        this.isAlive = true;
        this.printDebug("CLIENT-SIDE HEARTBEAT");
      }
      onDownStream(data) {
        this.printDebug("Received message from dingtalk websocket server");
        const msg = JSON.parse(data);
        this.printDebug(msg);
        switch (msg.type) {
          case "SYSTEM":
            this.onSystem(msg);
            break;
          case "EVENT":
            this.onEvent(msg);
            break;
          case "CALLBACK":
            this.onCallback(msg);
            break;
        }
      }
      onSystem(downstream) {
        var _a;
        switch (downstream.headers.topic) {
          case "CONNECTED": {
            this.printDebug("CONNECTED");
            break;
          }
          case "REGISTERED": {
            this.registered = true;
            this.reconnecting = false;
            break;
          }
          case "disconnect": {
            this.connected = false;
            this.registered = false;
            break;
          }
          case "KEEPALIVE": {
            this.heartbeat();
            break;
          }
          case "ping": {
            this.printDebug("PING");
            (_a = this.socket) == null ? void 0 : _a.send(
              JSON.stringify({
                code: 200,
                headers: downstream.headers,
                message: "OK",
                data: downstream.data
              })
            );
            break;
          }
        }
      }
      onEvent(message) {
        var _a;
        this.printDebug("received event, message=" + JSON.stringify(message));
        const ackData = this.onEventReceived(message);
        (_a = this.socket) == null ? void 0 : _a.send(JSON.stringify({
          code: 200,
          headers: {
            contentType: "application/json",
            messageId: message.headers.messageId
          },
          message: "OK",
          data: JSON.stringify(ackData)
        }));
      }
      onCallback(message) {
        this.emit(message.headers.topic, message);
      }
      send(messageId, value) {
        var _a;
        if (!messageId) {
          console.error("send: messageId must be defined");
          throw new Error("send: messageId must be defined");
        }
        const msg = {
          code: 200,
          headers: {
            contentType: "application/json",
            messageId
          },
          message: "OK",
          data: JSON.stringify(value)
        };
        (_a = this.socket) == null ? void 0 : _a.send(JSON.stringify(msg));
      }
      /**
       * 消息响应，避免服务端重试. 
       * stream模式下，服务端推送消息到client后，会监听client响应，如果消息长时间未响应会在一定时间内(60s)重试推消息，可以通过此方法返回消息响应，避免多次接收服务端消息。
       * @param messageId
       * @param result
       * @returns
       * @memberof DWClient
       * @example
       * ```javascript
       * client.socketResponse(res.headers.messageId, result.data);
       * ```
       */
      socketCallBackResponse(messageId, result) {
        this.send(messageId, { response: result });
      }
      sendGraphAPIResponse(messageId, value) {
        var _a;
        if (!messageId) {
          console.error("send: messageId must be defined");
          throw new Error("send: messageId must be defined");
        }
        const msg = {
          code: 200,
          headers: {
            contentType: "application/json",
            messageId
          },
          message: "OK",
          data: JSON.stringify(value)
        };
        (_a = this.socket) == null ? void 0 : _a.send(JSON.stringify(msg));
      }
    };
  }
});

// node_modules/dingtalk-stream/dist/index.mjs
var dist_exports = {};
__export(dist_exports, {
  DWClient: () => DWClient,
  EventAck: () => EventAck,
  GATEWAY_URL: () => GATEWAY_URL,
  GET_TOKEN_URL: () => GET_TOKEN_URL,
  TOPIC_AI_GRAPH_API: () => TOPIC_AI_GRAPH_API,
  TOPIC_CARD: () => TOPIC_CARD,
  TOPIC_ROBOT: () => TOPIC_ROBOT
});
var init_dist = __esm({
  "node_modules/dingtalk-stream/dist/index.mjs"() {
    init_constants();
    init_client();
    init_wrapper();
  }
});

// src/adapters/dingtalk/gateway-real.ts
var gateway_real_exports = {};
__export(gateway_real_exports, {
  createRealGateway: () => createRealGateway,
  requestPinnedDownload: () => requestPinnedDownload
});
import { lookup } from "node:dns/promises";
import { request as httpsRequest } from "node:https";
import { isIP } from "node:net";
function createRealGateway(options = {}) {
  const fetchImpl = options.fetchImpl ?? fetch;
  const resolveHostname = options.resolveHostname ?? (async (hostname) => (await lookup(hostname, { all: true, verbatim: true })).map((result) => result.address));
  let credentials;
  let credentialsVersion = 0;
  let observedRobotCode;
  let token;
  let tokenRequest;
  let stream4;
  let removeAbortListener;
  function setCredentials(next) {
    if (next.clientId.length === 0 || next.clientSecret.length === 0) {
      throw new ChannelError("E_CONNECT_FAILED", "DingTalk credentials are incomplete");
    }
    credentials = { ...next };
    credentialsVersion++;
    observedRobotCode = void 0;
    token = void 0;
    tokenRequest = void 0;
  }
  function requireCredentials() {
    if (credentials === void 0) {
      throw new ChannelError("E_CONNECT_FAILED", "DingTalk credentials are not set");
    }
    return credentials;
  }
  function requireRobotCode() {
    const robotCode = requireCredentials().robotCode?.trim() || observedRobotCode;
    if (robotCode === void 0 || robotCode.length === 0) {
      throw new ChannelError("E_CONNECT_FAILED", "DingTalk robotCode is not set");
    }
    return robotCode;
  }
  async function request(input, init, operation) {
    try {
      return await fetchImpl(input, { ...init, redirect: "error" });
    } catch {
      throw new ChannelError("E_CONNECT_FAILED", `DingTalk ${operation} request failed`);
    }
  }
  async function readJson(response, operation) {
    if (!response.ok) {
      throw new ChannelError("E_CONNECT_FAILED", `DingTalk ${operation} failed with HTTP ${response.status}`);
    }
    try {
      return await response.json();
    } catch {
      throw new ChannelError("E_CONNECT_FAILED", `DingTalk ${operation} returned invalid JSON`);
    }
  }
  async function acquireAccessToken() {
    const current = requireCredentials();
    const version = credentialsVersion;
    const response = await request(
      `${API}/v1.0/oauth2/accessToken`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ appKey: current.clientId, appSecret: current.clientSecret })
      },
      "access token"
    );
    const payload = await readJson(response, "access token");
    if (typeof payload.accessToken !== "string" || payload.accessToken.length === 0 || typeof payload.expireIn !== "number" || !Number.isFinite(payload.expireIn) || payload.expireIn <= 0) {
      throw new ChannelError("E_CONNECT_FAILED", "DingTalk access token response is invalid");
    }
    if (version !== credentialsVersion) {
      throw new ChannelError("E_CONNECT_FAILED", "DingTalk credentials changed during token request");
    }
    token = { value: payload.accessToken, expiresAt: Date.now() + payload.expireIn * 1e3 };
    return token.value;
  }
  async function accessToken() {
    if (token !== void 0 && token.expiresAt > Date.now() + 6e4) return token.value;
    tokenRequest ??= acquireAccessToken();
    const pending = tokenRequest;
    try {
      return await pending;
    } finally {
      if (tokenRequest === pending) tokenRequest = void 0;
    }
  }
  async function api(path, body, method = "POST") {
    const response = await request(
      `${API}${path}`,
      {
        method,
        headers: {
          "content-type": "application/json",
          "x-acs-dingtalk-access-token": await accessToken()
        },
        body: JSON.stringify(body)
      },
      "API"
    );
    return readJson(response, "API");
  }
  async function oapi(path, body) {
    const response = await request(
      `${OAPI}${path}?access_token=${encodeURIComponent(await accessToken())}`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body)
      },
      "directory API"
    );
    const payload = await readJson(response, "directory API");
    if (typeof payload.errcode === "number" && payload.errcode !== 0) {
      throw new ChannelError("E_CONNECT_FAILED", `DingTalk directory API failed with code ${payload.errcode}`);
    }
    return payload;
  }
  function openSpace(target, robotCode) {
    if (target.conversationType === "2") {
      return {
        imGroupOpenSpaceModel: { supportForward: true },
        openSpaceId: `dtv1.card//IM_GROUP.${target.conversationId}`,
        imGroupOpenDeliverModel: { robotCode }
      };
    }
    return {
      imRobotOpenSpaceModel: { supportForward: true },
      openSpaceId: `dtv1.card//IM_ROBOT.${target.userIds?.[0] ?? target.conversationId}`,
      imRobotOpenDeliverModel: { spaceType: "IM_ROBOT", robotCode }
    };
  }
  async function makeStream(current) {
    if (options.streamFactory !== void 0) return options.streamFactory(current);
    try {
      const module = await Promise.resolve().then(() => (init_dist(), dist_exports));
      if (module.DWClient === void 0) throw new Error("DWClient export missing");
      return new module.DWClient({
        clientId: current.clientId,
        clientSecret: current.clientSecret,
        keepAlive: true
      });
    } catch {
      throw new ChannelError("E_CONNECT_FAILED", "DingTalk Stream SDK is unavailable");
    }
  }
  function registerCallback(client, topic, handler) {
    client.registerCallbackListener(topic, async (message) => {
      try {
        if (stream4 !== client) return;
        const payload = JSON.parse(message.data);
        if (typeof payload !== "object" || payload === null || Array.isArray(payload)) return;
        handler(payload);
      } catch {
      } finally {
        try {
          client.socketCallBackResponse?.(message.headers.messageId, {
            message: "success",
            status: "SUCCESS"
          });
        } catch {
        }
      }
    });
  }
  async function stop() {
    removeAbortListener?.();
    removeAbortListener = void 0;
    const current = stream4;
    stream4 = void 0;
    if (current === void 0) return;
    try {
      await current.disconnect();
    } catch {
      throw new ChannelError("E_CONNECT_FAILED", "DingTalk stream disconnect failed");
    }
  }
  return {
    setCredentials,
    async start(handlers, signal) {
      if (signal.aborted) throw new ChannelError("E_CONNECT_FAILED", "DingTalk stream start was aborted");
      const currentCredentials = requireCredentials();
      const version = credentialsVersion;
      await stop();
      const client = await makeStream(currentCredentials);
      stream4 = client;
      registerCallback(client, TOPIC_ROBOT2, (message) => {
        if (version !== credentialsVersion) return;
        if (typeof message.robotCode === "string" && /^[A-Za-z0-9._-]{1,256}$/.test(message.robotCode))
          observedRobotCode = message.robotCode;
        handlers.onMessage(message);
      });
      registerCallback(client, TOPIC_CARD2, handlers.onCard);
      client.onDisconnect?.(() => {
        if (stream4 === client && !signal.aborted) {
          handlers.onDisconnect(new Error("DingTalk stream disconnected"));
        }
      });
      const onAbort = () => {
        if (stream4 === client) void stop().catch(() => void 0);
      };
      signal.addEventListener("abort", onAbort, { once: true });
      removeAbortListener = () => signal.removeEventListener("abort", onAbort);
      try {
        await client.connect();
      } catch {
        if (stream4 === client) await stop().catch(() => void 0);
        throw new ChannelError("E_CONNECT_FAILED", "DingTalk stream connect failed");
      }
      if (signal.aborted || stream4 !== client) {
        if (stream4 === client) await stop().catch(() => void 0);
        throw new ChannelError("E_CONNECT_FAILED", "DingTalk stream start was aborted");
      }
      return { botUserId: currentCredentials.robotCode ?? "" };
    },
    stop,
    async sendMarkdown(target, title, markdown) {
      const robotCode = requireRobotCode();
      const msgParam = JSON.stringify({ title, text: markdown });
      if (target.conversationType === "2") {
        return api("/v1.0/robot/groupMessages/send", {
          robotCode,
          openConversationId: target.conversationId,
          msgKey: "sampleMarkdown",
          msgParam
        });
      }
      return api("/v1.0/robot/oToMessages/batchSend", {
        robotCode,
        userIds: target.userIds ?? [target.conversationId],
        msgKey: "sampleMarkdown",
        msgParam
      });
    },
    async createCard(outTrackId, cardData, target) {
      if (!options.cardTemplateId?.trim()) {
        throw new ChannelError("E_CONFIG_INVALID", "DingTalk card template is not configured");
      }
      const robotCode = requireRobotCode();
      await api("/v1.0/card/instances", {
        cardTemplateId: options.cardTemplateId.trim(),
        outTrackId,
        cardData,
        ...openSpace(target, robotCode)
      });
      await api("/v1.0/card/instances/deliver", {
        outTrackId,
        ...openSpace(target, robotCode)
      });
    },
    async updateCard(outTrackId, cardData) {
      await api("/v1.0/card/instances", { outTrackId, cardData }, "PUT");
    },
    async download(downloadCode, maxBytes) {
      if (!Number.isSafeInteger(maxBytes) || maxBytes < 0) {
        throw new ChannelError("E_CONNECT_FAILED", "DingTalk download size limit is invalid");
      }
      const robotCode = requireRobotCode();
      const payload = await api("/v1.0/robot/messageFiles/download", {
        downloadCode,
        robotCode
      });
      const target = typeof payload.downloadUrl === "string" ? await resolvePublicHttpsUrl(payload.downloadUrl, resolveHostname) : void 0;
      if (target === void 0) {
        rejectedDownloadUrl("MISSING_URL");
      }
      if (options.attachmentRequest !== void 0) {
        return options.attachmentRequest(target, maxBytes);
      }
      if (options.fetchImpl === void 0) return requestPinnedDownload(target, maxBytes);
      const response = await request(
        target.url.href,
        { method: "GET", redirect: "error" },
        "attachment download"
      );
      if (!response.ok) {
        throw new ChannelError(
          "E_CONNECT_FAILED",
          `DingTalk attachment download failed with HTTP ${response.status}`
        );
      }
      const declaredLength = Number(response.headers.get("content-length"));
      if (Number.isFinite(declaredLength) && declaredLength > maxBytes) {
        await response.body?.cancel().catch(() => void 0);
        return { url: target.url.href };
      }
      let bytes;
      try {
        bytes = await readBoundedBody(response, maxBytes);
      } catch {
        throw new ChannelError("E_CONNECT_FAILED", "DingTalk attachment download failed");
      }
      if (bytes === void 0) return { url: target.url.href };
      return {
        bytes,
        mime: response.headers.get("content-type") ?? "application/octet-stream"
      };
    },
    async listDepartments(parentId = 1) {
      const payload = await oapi("/topapi/v2/department/listsub", {
        dept_id: parentId
      });
      return payload.result ?? [];
    },
    async listUsers(deptId, cursor = 0) {
      const payload = await oapi("/topapi/v2/user/list", { dept_id: deptId, cursor, size: 100 });
      const result = payload.result;
      return {
        users: result?.list ?? [],
        ...result?.has_more === true && result.next_cursor !== void 0 ? { nextCursor: result.next_cursor } : {}
      };
    }
  };
}
async function readBoundedBody(response, maxBytes) {
  if (response.body === null) return new Uint8Array();
  const reader = response.body.getReader();
  const chunks = [];
  let total = 0;
  while (true) {
    const part = await reader.read();
    if (part.done) break;
    total += part.value.byteLength;
    if (total > maxBytes) {
      await reader.cancel().catch(() => void 0);
      return void 0;
    }
    chunks.push(part.value);
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}
function rejectedDownloadUrl(reason) {
  throw new ChannelError("E_CONNECT_FAILED", "DingTalk returned an unsafe download URL", {
    downloadUrlReason: reason
  });
}
async function resolvePublicHttpsUrl(value, resolveHostname) {
  if (value.length > 32768) rejectedDownloadUrl("URL_TOO_LONG");
  if ([...value].some((character) => character.charCodeAt(0) <= 32 || character.charCodeAt(0) === 127))
    rejectedDownloadUrl("URL_FORMAT");
  let url2;
  try {
    url2 = new URL(value);
  } catch {
    return rejectedDownloadUrl("URL_FORMAT");
  }
  if (url2.protocol === "http:") {
    if (url2.port) rejectedDownloadUrl("HTTP_CUSTOM_PORT");
    url2.protocol = "https:";
  }
  if (url2.protocol !== "https:") rejectedDownloadUrl("URL_PROTOCOL");
  if (url2.username || url2.password) rejectedDownloadUrl("URL_CREDENTIALS");
  const hostname = url2.hostname.toLowerCase().replace(/^\[|\]$/g, "");
  if (!hostname || hostname === "localhost" || hostname.endsWith(".localhost"))
    rejectedDownloadUrl("LOCAL_HOST");
  const family = isIP(hostname);
  if (family) {
    if (!isPublicAddress(hostname)) rejectedDownloadUrl("NON_PUBLIC_ADDRESS");
    return { url: url2, addresses: [hostname] };
  }
  let addresses;
  try {
    addresses = await resolveHostname(hostname);
  } catch {
    return rejectedDownloadUrl("DNS_FAILED");
  }
  if (!addresses.length) rejectedDownloadUrl("DNS_EMPTY");
  if (!addresses.every(isPublicAddress)) rejectedDownloadUrl("NON_PUBLIC_ADDRESS");
  return { url: url2, addresses };
}
function requestPinnedDownload(target, maxBytes) {
  const resolved = target.addresses.map((address) => ({ address, family: isIP(address) }));
  if (resolved.length === 0 || resolved.some((entry) => !isPublicAddress(entry.address))) {
    return Promise.reject(new ChannelError("E_CONNECT_FAILED", "DingTalk download address is unsafe"));
  }
  const pinnedLookup = (_hostname, lookupOptions, callback) => {
    if (lookupOptions.all) callback(null, resolved);
    else {
      const selected = resolved[0];
      callback(null, selected.address, selected.family);
    }
  };
  return new Promise((resolve, reject) => {
    const request = httpsRequest(
      target.url,
      {
        method: "GET",
        agent: false,
        lookup: pinnedLookup,
        headers: { accept: "*/*" }
      },
      (response) => {
        const status = response.statusCode ?? 0;
        if (status < 200 || status >= 300) {
          response.resume();
          reject(
            new ChannelError("E_CONNECT_FAILED", `DingTalk attachment download failed with HTTP ${status}`)
          );
          return;
        }
        const declaredLength = Number(response.headers["content-length"]);
        if (Number.isFinite(declaredLength) && declaredLength > maxBytes) {
          response.destroy();
          resolve({ url: target.url.href });
          return;
        }
        const chunks = [];
        let total = 0;
        let settled = false;
        response.on("data", (chunk) => {
          if (settled) return;
          total += chunk.byteLength;
          if (total > maxBytes) {
            settled = true;
            response.destroy();
            resolve({ url: target.url.href });
          } else chunks.push(new Uint8Array(chunk));
        });
        response.once("end", () => {
          if (settled) return;
          settled = true;
          const bytes = new Uint8Array(total);
          let offset = 0;
          for (const chunk of chunks) {
            bytes.set(chunk, offset);
            offset += chunk.byteLength;
          }
          const contentType = response.headers["content-type"];
          resolve({
            bytes,
            mime: Array.isArray(contentType) ? contentType[0] ?? "application/octet-stream" : contentType ?? "application/octet-stream"
          });
        });
        response.once("error", () => {
          if (settled) return;
          settled = true;
          reject(new ChannelError("E_CONNECT_FAILED", "DingTalk attachment download failed"));
        });
      }
    );
    request.setTimeout(3e4, () => request.destroy(new Error("attachment timeout")));
    request.once("error", () => {
      reject(new ChannelError("E_CONNECT_FAILED", "DingTalk attachment download failed"));
    });
    request.end();
  });
}
function isPublicAddress(address) {
  const family = isIP(address);
  return family === 4 ? isPublicIpv4(address) : family === 6 ? isPublicIpv6(address) : false;
}
function isPublicIpv4(hostname) {
  const parts = hostname.split(".").map(Number);
  const [a = 0, b = 0] = parts;
  return !(a === 0 || a === 10 || a === 127 || a === 100 && b >= 64 && b <= 127 || a === 169 && b === 254 || a === 172 && b >= 16 && b <= 31 || a === 192 && b === 168 || a === 198 && (b === 18 || b === 19) || a >= 224);
}
function isPublicIpv6(hostname) {
  const words = ipv6Words(hostname);
  if (words === void 0) return false;
  const first = words[0] ?? 0;
  if ((first & 65024) === 64512) return false;
  if ((first & 65472) === 65152 || (first & 65472) === 65216) return false;
  if ((first & 65280) === 65280) return false;
  const firstFiveZero = words.slice(0, 5).every((word) => word === 0);
  if (firstFiveZero && words[5] === 65535) {
    const ipv4 = `${(words[6] ?? 0) >> 8}.${(words[6] ?? 0) & 255}.${(words[7] ?? 0) >> 8}.${(words[7] ?? 0) & 255}`;
    return isPublicIpv4(ipv4);
  }
  if (words.slice(0, 6).every((word) => word === 0)) return false;
  return true;
}
function ipv6Words(value) {
  const normalized = value.toLowerCase().replace(/^\[|\]$/g, "");
  if (normalized.includes("%") || normalized.split("::").length > 2) return void 0;
  const halves = normalized.split("::");
  const parseHalf = (half) => {
    if (half === "") return [];
    const out = [];
    for (const part of half.split(":")) {
      if (!/^[0-9a-f]{1,4}$/.test(part)) return void 0;
      out.push(Number.parseInt(part, 16));
    }
    return out;
  };
  const left = parseHalf(halves[0] ?? "");
  const right = parseHalf(halves[1] ?? "");
  if (left === void 0 || right === void 0) return void 0;
  if (halves.length === 1) return left.length === 8 ? left : void 0;
  const missing = 8 - left.length - right.length;
  if (missing < 1) return void 0;
  return [...left, ...Array.from({ length: missing }, () => 0), ...right];
}
var API, OAPI, TOPIC_ROBOT2, TOPIC_CARD2;
var init_gateway_real = __esm({
  "src/adapters/dingtalk/gateway-real.ts"() {
    "use strict";
    init_errors();
    API = "https://api.dingtalk.com";
    OAPI = "https://oapi.dingtalk.com";
    TOPIC_ROBOT2 = "/v1.0/im/bot/messages/get";
    TOPIC_CARD2 = "/v1.0/card/instances/callback";
  }
});

// src/index.ts
import { fileURLToPath } from "node:url";

// src/adapters/dingtalk/channel.json
var channel_default = {
  id: "dingtalk",
  displayName: "\u9489\u9489",
  version: "0.3.8",
  connection: {
    modes: [
      "stream"
    ],
    default: "stream"
  },
  capabilities: {
    edit: true,
    card: true,
    thread: false,
    attachment: true,
    reactions: false,
    typing: false,
    voice: true
  },
  credentials: {
    required: [
      "clientId",
      "clientSecret"
    ],
    optional: [
      "robotCode",
      "agentId"
    ],
    exposes: [
      "staffId",
      "unionId",
      "conversationId",
      "conversationType",
      "senderNick"
    ]
  },
  limits: {
    textChars: 2e4,
    cardBytes: 65536,
    editWindowMs: 0,
    rate: {
      perChatPerMin: 20,
      perAccountPerSec: 20
    }
  },
  events: {
    supported: [
      "message",
      "cardAction",
      "groupJoin"
    ]
  },
  directory: {
    supported: true,
    unit: "dept"
  },
  displayNames: {
    en: "DingTalk",
    "zh-CN": "\u9489\u9489"
  },
  icon: "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAABAAAAAQACAYAAAB/HSuDAAAACXBIWXMAAAsTAAALEwEAmpwYAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAOdEVYdFNvZnR3YXJlAEZpZ21hnrGWYwAASM9JREFUeAHt3TuM3eeZJ+hXbi+wYGIaUNBOKBlOuEHDFJPpBQoQlRQ6cylYYMCEJMBgNqGKQAeKxKIjBQJ4CRwREJkQuxHL2WwlIgEG7YSi0Ak3MCQx2Q0ESE6EHnTA4avjI5FUFatO1f/yXZ4HIErt8fgiyYfn/X3v5Y049+xZAAAAAE37RQAAAADNEwAAAABABwQAAAAA0AEBAAAAAHRAAAAAAAAdEAAAAABABwQAAAAA0AEBAAAAAHRAAAAAAAAdEAAAAABABwQAAAAA0AEBAAAAAHRAAAAAAAAdEAAAAABABwQAAAAA0AEBAAAAAHRAAAAAAAAdEAAAAABABwQAAAAA0AEBAAAAAHRAAAAAAAAdEAAAAABABwQAAAAA0AEBAAAAAHRAAAAAAAAdEAAAAABABwQAAAAA0AEBAAAAAHRAAAAAAAAdEAAAAABABwQAAAAA0AEBAAAAAHRAAAAAAAAdEAAAAABABwQAAAAA0AEBAAAAAHRAAAAAAAAdEAAAAABABwQAAAAA0AEBAAAAAHRAAAAAAAAdEAAAAABABwQAAAAA0AEBAAAAAHRAAAAAAAAdEAAAAABABwQAAAAA0AEBAAAAAHRAAAAAAAAdEAAAAABABwQAAAAA0AEBAAAAAHRAAAAAAAAdEAAAAABABwQAAAAA0AEBAAAAAHRAAAAAAAAdEAAAAABABwQAAAAA0AEBAAAAAHRAAAAAAAAdEAAAAABABwQAAAAA0AEBAAAAAHRAAAAAAAAdEAAAAABABwQAAAAA0AEBAAAAAHRAAAAAAAAdEAAAAABABwQAAAAA0AEBAAAAAHRAAAAAAAAdEAAAAABABwQAAAAA0AEBAAAAAHRAAAAAAAAdEAAAAABABwQAAAAA0AEBAAAAAHRAAAAAAAAdEAAAAABABwQAAAAA0AEBAAAAAHRAAAAAAAAdEAAAAABABwQAAAAA0IFfBkDFzpyM2Dgd8e7zn2+/GXH82OIf/+qbxa8vnkZsP4q4/yQAAKBrb8S5Z88CoDKb6xEfrC+K/oPIMODqdsTthwEAAF0SAABVOXUi4tOLi5+HkUHA5buLrgAAAOiJAACoRr76Xzsbg7i+swgCAACgF5YAAlW4sjFc8Z8yTPjyk4OPEAAAQO0EAEDxsvjf2ojBZfH/2YdCAAAA+iAAAIo2VvG/JAQAAKAXAgCgWNmmP2bxvyQEAACgBwIAoEhZjF+ZoPh/8d/v3qWI48cCAACaJAAAipQv8lMX43lacMrQAQAApiQAAIqTRfhc7fg5dnBuLQAAoDkCAKAoWfhnET6n62ftAwAAoD0CAKAoufRv7jn8/Pf/9GIAAEBTBABAMfLVvZT2+zMn5+9EAACAIQkAgGJsFbaA79rZxWJAAABogQAAKEJJr/8vchoQAIBWCACAIpT2+r+UwYTTgAAAtEAAAMyu1Nf/pdwFsHE6AACgagIAYHalvv6/KK8COA0IAEDNBADA7N49GcVzGhAAgNoJAIBZnV+r52U9TwPaBwAAQK0EAMCsPliPquS4wpkKOhYAAOBVAgBgNllInzoR1clRAKcBAQCojQAAmM35gjf/v06OLNgHAABAbQQAwCxKP/23nzwLuFnZ+AIAAH0TAACzaGGO/trZOkcYAADokwAAmEVty//2cu+SfQAAANRBAABMLtv/W3k5z/8u2QkAAAClEwAAk8tTei3JZYb2AQAAUDoBADC5dxuY/3/VlQ37AAAAKJsAAJhULv/LtvnW5B4A+wAAACiZAACY1PmKT//txz4AAABKJgAAJvWH09E0+wAAACiVAACYzMbpPlrkswvAPgAAAEojAAAms9H46/+L7AMAAKA0AgBgMq23/78o9wFkCAAAAKUQAACTyO3/vb2I53/nPA8IAAAlEAAAk2h5+//rbG0sggAAAJibAACYxLsdF8GfXlyMBAAAwJwEAMDociN+zwWwfQAAAJRAAACMTgv8IgTJ84AAADAXAQAwup62/7/O5vriFwAAzEEAAIwqN//rAPhJXgXIbgAAAJiaAAAYleL/ZRmI5D6A3k4iAgAwPwEAMKoN7f8/YykgAABzEAAAo/q9dvddZWdEjgMAAMBUBADAaPKl27z73raeBwDn1gIAACYhAABGo/jf3/Wzi6AEAADGJgAARmP+f3+5DPCzDy0FBABgfAIAYDTm/w/GUkAAAKYgAABGkS/aRgAOzlJAAADGJgAARqH4X52lgAAAjEkAAIwiX7RZXS4FFJ4AADAGAQAwincFAIeSoxO5D8BlAAAAhiYAAEbhFfvwLAUEAGAMAgBgcFnAOmt3NBmgXDsbAAAwGAEAMDiv/8PYXF/8AgCAIQgAgMEJAIaTXQAWKgIAMAQBADA4AcCwLAUEAGAIAgBgcG8pVgeV+xQ++9BeBQAAjkYAAAxOB8DwXAYAAOCoBADAoBT/48ldAC4DAABwWAIAYFBm1cflMgAAAIclAAAGpQNgfNkFsHE6AABgJQIAYFACgGl8elG3BQAAqxEAAINyAWAay8sAQgAAAA5KAAAMSkE6neVlAOcBAQA4CAEAMJgsRBWj08qRC5cBAAA4CAEAMBjz//M4vxZxZSMAAOC1BADAYLz+z2drw3lAAABeTwAADMb8/7ycBwQA4HUEAMBgBADzy/OARjEAANiNAAAYjABgfjmGkZcB/LUAAOBVAgBgML+yA6AIWfx/9qGdDAAAvEwAAAzmt16di7EMAQAAYEkAAAxGB0BZchdA7gQAAIAkAAAGo+W8POfXIq5sBAAACACAYVg6V66tDSEAAAACAGAgXv/LliHAubUAAKBjAgBgEAKA8t2+uNgLAABAnwQAwCAEAHXIywDGNQAA+iQAAAYhAKhD/nUSAgAA9EkAAAxCAFCPLP6FAAAA/REAAIMQANQli/97l/x1AwDoiQAAoFO5EDBDAAAA+iAAAAbhJblOZ05GfHoxAADogAAAGIQAoF7n1yKubAQAAI0TAAAQWxtCAACA1gkAAPiBEAAAoG0CAAB+lCHAubUAAKBBAgBgEG+5Kd+M2xeFAAAALRIAAPAz188uzgQCANAOAQAAP5NXHT77UAgAANASAQAAu8oQ4N6liLeNdwAANEEAAMCesvjPTgAhAABA/QQAwCD+9n3QKCEAAEAbBADAIL4TADRNCAAAUD8BAAAHsgwBcjcAAAD1EQAAcGBCAACAegkAAFhJngYUAgAA1EcAAAzCDoC+CAEAAOojAAAGIQDoT4YA184GAACVEAAAcGjn1yI+vRgAAFRAAAAMQgdAv4QAAAB1EAAAgxAA9E0IAABQPgEAAIMQAgAAlE0AAAziq28CfggBrmwEAAAFEgAAgzACwNLWhhAAAKBEAgBgEAIAXiQEAAAojwAAGIQAgFcJAQAAyiIAAAaRAYAQgFcJAQAAyiEAAAbzNwEAuxACAACUQQAADOZbAQB7EAIAAMxPAAAM5munAHkNIQAAwLwEAMBg7ABgP0IAAID5CACAwXylA4ADyBDg04sBAMDEBADAYAQAHNT5NSEAAMDUBADAYIwAsAohAADAtAQAwGAePw1YiRAAAGA6AgBgMEYAOAwhAADANAQAwKCcAuQwMgT4/I8Rx48FAAAjEQAAg/rWHgAO6dSJiM8+FAIAAIxFAAAM6gt7ADgCIQAAwHgEAMCg7AHgqDIEyHGAt98MAAAGJAAABiUAYAhZ/GcngBAAAGA4AgBgUE4BMhQhAADAsAQAwKB0ADAkIQAAwHAEAMCgvvveKUCGJQQAABiGAAAYnDEAhpbFfy4GzAWBAAAcjgAAGJwxAMaQpwGzE2DjdAAAcAgCAGBwOgAYS4YA9y5FnFsLAABWJAAABicAYGy3L0Zc2QgAAFYgAAAGZwSAKWxtCAEAAFYhAAAG5xIAUxECAAAcnAAAGMX9JwGTyBDg2tkAAGAfAgBgFPYAMKXN9cVywFwSCADA7gQAwCgEAEwtzwPmmUAhAADA7gQAwCgEAMzh1ImIz/8Y8fabAQDAKwQAwCgsAmQuWfxnJ4AQAADgZQIAYDQWATKXZQiQHQEAACwIAIDRGANgTssQIHcDAAAgAABGpAOAueVCwLwOkFcCAAB6JwAARpMdALkLAOZ27WzElY0AAOiaAAAYlTEASrG1IQQAAPomAABG9cAYAAXJEODTi4vRAACA3ggAgFHZA0Bpzq85EwgA9EkAAIzKCAAlyvOAQgAAoDcCAGBUuQRQFwAlWp4JFAIAAL0QAACj+0IXAIXK4v/zP0ZsnA4AgOYJAIDRbT8KKFYuBLx3yYUAAKB9AgBgdLkHIEcBoGTOBAIArRMAAKPL4t8yQGqwPBMIANAiAQAwiT8bA6ASeSYw9wJYDggAtEYAAExCBwA1cSYQAGiRAACYRJ4CtAeAmizPBGYYAADQAgEAMBljANRmeSZwcz0AAKonAAAmk10AUKNrZ10IAADqJwAAJrOtA4CK5YWADAKOHwsAgCoJAIDJ5A4AXQDULEcBLAcEAGolAAAmZQ8AtXMhAAColQAAmJQxAFqwXA64cToAAKohAAAm9dU3EV9/E1C93AVw75LlgABAPQQAwOR0AdCS5XJAAIDSCQCAyQkAaE0uB8yRAHsBAICSCQCAyeUlgLwIAC2xHBAAKJ0AAJjFnYcBzbEcEAAomQAAmIUxAFplOSAAUCoBADALYwC0znJAAKA0AgBgNsYAaF0uB/zyE3sBAIAyCACA2RgDoAdZ/OdywFwSCAAwJwEAMBtjAPRiuRwwOwIAAOYiAABmdWMnoBu5E8ByQABgLgIAYFbZBQA9yeWAORJgLwAAMDUBADCrDACEAPTmzEkhAAAwPQEAMLs/WwZIh+wFAACmJgAAZnfbOUA6dfyYvQAAwHQEAMDs8hKAMQB6Zi8AADAFAQBQhKvbAV2zFwAAGJsAAChCdgBkJwD0zF4AAGBMAgCgGDd2ArpnLwAAMBYBAFCMbdcA4Ee5FyC7AYwEAABDEQAAxXj81DJAeNGpE4u9APkTAOCoBABAUYwBwMuWewGMBAAARyUAAIpiGSDsLkcCcjdA7ggAADgMAQBQlCz+dQHA7vI6gL0AAMBhCQCA4lwXAMCeliMB59cCAGAlAgCgONkFYBkg7C3HAD69aC8AALAaAQBQpKvbAezDqUAAYBUCAKBI2QGgCwD2tzwVeOZkAAC8lgAAKNafHwVwANkBkCGAkQAA4HUEAECxbj90EhBWkSMB9y4ZCQAAdicAAIrlJCCsbuP0ohtACAAAvEoAABQtTwLqAoDVZPH/5SdGAgCAlwkAgKJl8W8XABxOjgTkuUDdAABAEgAAxctdAMDhnF8zEgAALAgAgOI5CQhHYyQAAEgCAKAKV7cDOCIjAQDQNwEAUAVdADAMIwEA0C8BAFANXQAwDCMBANAnAQBQDV0AMCwjAQDQFwEAUBVdADCs5UjAqRMBADROAABURRcADC87AD7/o5EAAGidAACoji4AGEeOBFgQCADtEgAA1dEFAOM5c3IRAmycDgCgMQIAoEq6AGA82QFw75KRAABojQAAqJIuABhfjgTkuUAjAQDQBgEAUC1dADC+5YLAvBYAANRNAABUSxcATOP4sYhPLy5+5R8DAHUSAABV0wUA08kugOwGMBIAAHUSAABV0wUA08riP/cCWBAIAPURAADV0wUA08sFgXkuUDcAANRDAABULzsA7jwMYGJnTi5CAAsCAaAOAgCgCVvbEd99H8DEsgMglwNeO2tBIACUTgAANOGrbyJu7AQwk811CwIBoHQCAKAZ13d0AcCcLAgEgLIJAIBmZPFvISDMLxcE6gYAgPIIAICmZBfA46cBzOzUiUUIYEEgAJRDAAA05/LdAAqQSwFzQWD+0g0AAPMTAADNybOA+QsoQ3YB5LnAPBsIAMxHAAA06cItCwGhJNkBkCGABYEAMB8BANAkZwGhTLkgMC8FGAkAgOkJAIBm5ULADAKAsjgXCADzEAAAzcoRgBwFAMqkGwAApiUAAJpmISCULYv/PBe4uR4AwMgEAEDzLASEsuW5wGtnnQsEgLEJAIDmWQgIdVieC8yfAMDw3ohzz54FQAeyzfjUiQAqcPthxNVtizwBYEg6AIBuXL4bQCV0AwDA8P4hTm1tBUAH8iXx18ci/vl3AVQgdwNsnF78/MtfI/7jPwMAOAIjAEBXspDIUQCLxqAuGeDlQk9XPQDg8IwAAF3JawBZRAB1ydAuRwLyWgAAcDgCAKA7+YLoKgDUaXM94stPdPEAwGEIAIAubdkuDtXK4j9DgCsbAQCsQAAAdMkoANRva0M3AACsQgAAdMsoANRPNwAAHJwrAMBK8sv2mZMRp05E/OrYT//4198sCuraNnS7CgDtyLGe9z423gMAexEAAPvKIvmD9cU97iz8Xydb67cfRVytaMY+A43cLg60IXd85GcQAPAyAQDwWrlxO1trjx9b/f9vTV/Cr59dhBxAG3QDAMDPCQCAXWXBf+/S4nX8KLIbIJftZWdAyfK/b3YB7NfhANRFNwAA/EQAAPxMzsNnMTzUXPzjp4uXuNJDgPzvm/sADtPtAJRLNwAALLgCALxk6OI/5at6/muWXlhnceClENrjUgAALOgAAF6SL+BjtcHX0gmQYcVRRx+AMmXQ9/7NxecRAPRGBwDwo3wdG3MGvpZOgCwOtApDm5ajPtfOGvcBoD8CAOAH+aV4a4L22AwB8ot3ybJDIRcXAu3KCycZBOj2AaAn/xCntrYC6N6nFyNO/iYmkSFABg5/fhTFyg6AN0JxAC3LDoDza4vPoy+elj+eBABHpQMA+OHL78bpmFR+6c7QoWR5Puz+kwAal59HOZ6UPwGgZQIAYJLW/93UEALkKIB9ANC+DELz8yh/DXkFBQBKIgAA4g8Tv/6/qPQQIIt/+wCgH7oBAGiZAAA6lzPuc2/CLj0EyDGAy3cD6IRuAABaJQCAzpWy5K70EOD6TsSdhwF0JD+X8lJAXgwAgBYIAKBz7xa05b70EGDzrn0A0JvskMrTpV9+ohsAgPoJAKBzc7f/v2r54lbaf66UJ8Le+9ipMOhRFv8ZAlyZaWkqAAxBAACdO3UiipP/mXIJV4khQHYAvH8zgE7l1RTdAADUSgAAHSuxwF4qOQSwFBD6tuwGyNGAkj9HAeBVAgDoWOkvWCWHALkU8MZOAB3L5YA5suRkIAC1EABAx2p4uSo5BMilgI+fBtAxJwMBqIkAADpWS+tqySFA7gNwGQDILoD8nNINAEDJBADQsZpmVzMEyFbb0l7Ysvh3GQBIy26ADAJ0AwBQIgEAdKy2L6j5n7fEL9YuAwAvOnPSyUAAyiQAAKpSagjgMgDwquXJwBLPrQLQJwEAdKzWFtVSQ4C8DHB1OwB+lJ9TOb5kSSAAJRAAAFUqNQTY2nYeEPg5SwIBKIEAADr2VuWvUaWGAHkeMEcCAF60XBJ475JuAADmIQAAqrZsry1txtZ5QGAvG6ctCQRgHgIAoHp5zjA7AUoKAfIsYJ4HFAIAe7EkEICpCQCgY79tqAU1Q4DsBDhX0HxtFv8ZAmQYALAbSwIBmJIAAGjK7YtCAKA+lgQCMAUBANCcDAFKmq19/DTi8t0AeK3lksASl5sC0AYBAHTsV8eiWTlbW1IIcPuhEAA4mDMnLQkEYBz/EKe2tgLo0sf/RzQtv0SnB4Wc5Pu3v0a8ET/95wJ4nfysyJGAv32/6CQCgKMSAEDHtjp4Xcov0Lkg8P/59yjC/SdCAODg8vMrzwbmSMAXT+0TAeBojAAAzdtcj7h3afFFugRb2xF3HgbAgWUnQF4LyM8zADisN+Lcs2cBdOnZ7ehKttCWtJE/Q4l82QNYxfK6SP4EgFXoAAC6cerE4gWtlO3aF26Z6wVWl59huSQwLwa4FgDAKgQAQFfyy3IpJ7ayEyFf8YQAwGHkWEB+nuVPADgIAQDQnWUIkB0Bc8sQ4P2bWnmBw8nPs+wEyI4A3QAA7EcAAHRpGQKUMINvnhc4quVYwLWzggAA9iYAALqVVwFyEd+VAs4hCgGAIeSVAGMBAOxFAAAdc096YWtDCAC0YzkWkAGnbgAAXiQAgI79TQDwowwBsnV2bkIAYCg54pRjARlwZscTAAgAAP5u2To79xdlIQAwpAw48wSqsQAABAAALzhzcvFFee622Sz+8zqAMQ1gCMYCAEgCAOjYt4rLXS0vBMz9Jfnx00UngBAAGIqxAIC+CQCgYwrLvWXxn50Ac58JFAIAYzAWANAnAQDAHko5EygEAMZgLACgPwIA6NjXlswdSAlnApchgMWAwNCMBQD0QwAAcAAZAuQr2ZxfjoUAwJiMBQC0TwAAHdNSvpp8JZv7QoATgcCYlmMBJVxDAWB4AgDomABgdcsLAadOxGyEAMDY8jMuxwKunRUEALREAAAdU0AezvJCwOZ6zEYIAEwhP+cy9DQWANAGAQDAIeXL2JzLAYUAwBSWYwHZEaAbAKBuAgDomMLx6OZeDrgMAXJBIMCYsvjPECDDAEEAQJ0EANAxAcAw5l4OKAQAppTjADkWMOcYFACHIwCAjlkCOJzlcsAzJ2MW+dfynY8i7jwMgNHlZ16OQWVHQIagANRBAAAdy6JRCDCcZQgw516A87cibuwEwCTycy/HoIwFANThH+LU1lYA3fo/35tvfr1V2QWQf07/8teI//jPmNx///eIN2K+bgSgP3k2cDkS8OBJAFAoHQDQuS/tARhFfhGecy/A1nbE1e0AmFQuRs2xAGcDAcokAIDOfS0AGM1yJGCu+dgMAS7cCoBJLc8G5miAsQCAsggAoHMuAYxrOR87116A2w8XywH9dQamluGns4EAZREAQOcUhtPItti5XsPyPGCeCfTXGpjD8mygsQCA+QkAoHOuAEwnX8PyS/AcIUAW/xkCZBgAMLXlWEB2BFhQCjAfAQB0TkE4rfwSnF+Al9uyp5QhQI4DOBMIzGW5G8VYAMA8nAGEzmUHwNaMd+t79S//NN+pQGcCgbk5GwgwDx0AgEsAM5nzVGBeCLh8NwBm5WwgwLQEAIAxgBll8Z8hwBwjAdd3XAgA5rfcDzBXIArQEwEAoACcWY4CXDu7+JV/PCUXAoBS5FiAs4EA4xIAADoACjHXSIDlgEBJlmcDr9hPAzA4SwCBH16dzV+WIf9azLEYKxcRWg4IlCI/C/OzKH9v+tv3gmqAobwR5549C6Br+UXr2z8Fhbn9MOLq9vTt+fmlWwsuUJLtR4vFpcaVAI5GAAD8IAOAqefP2V9+2b1wK+L+xGeylre6hQBASeYKRgFaYQQA+MH//ruIk78JCvPieMaUIwHffb/YCfDr5//+//y7AChCLgrcOL0YV/q3vwYAK7IEEPiB15SyLW9lT/0iv3l30XabgQBACfJzMK+m5Gei/TUAqxEAAD+wYKl8+aU3v/BOvRn7+s7iSoCQCChJfibmvpJ7l4wrARyUHQDAD5bFJXXInQC5G2DKojzHEa4/f3U758UNKJD9AAD7swMA+EG2eOf5uf/1fwkqkIFNzsFOeR4rTwXmJu789/yXfwqAotgPALA/HQDAj3Lruxvw9Znj1cuVAKBk+XmYn4v5+QjAT+wAAH70hT0AVcolWFmMT7kMK79c//ZfF5cCAEqz3A8gqAR4mREA4Ef/+KtF+yT1yfn8/GuXX3QzyJlqa/9///eIr79ZtN7mfwaAkuRnYo63Tf3ZCFAqIwDAjywCbMMcra/L1zYjJECpsvjPqyb5+QjQKwEA8JJv/+QltxVz7AbY2pj+TCHAKuwHAHomAABecvuiM28tmasbwNwtULr8fHzvY2cDgb5YAgi85LFFgE1Ztubnr6kK8uWCQG22QMmWY29Tfj4CzE0HAPCSXOb2+R+DBs0x/6obAKjF1vPPxjsPdQQAbRMAAD9jD0Db5mh7tRsAqIH9AEDrjAAAP/PgSdCwZdvrtbPTBT35spZjAV7WgJItx6byM/K8fThAg/4hTm1tBcAL/vFXEf/yT0Hj/vl3Ef/1v0T87ftpdj/kCMKNnWw9cy4QKFuGoxunF4HAF08Xn18ALTACAPyMPQD9mfpkoN0AQE3mOKsKMAYBALArewD6lK36Uy4J3FyP+GBdEACUL4v/ZRAAUCsjAMCu/rffLDoB6Eu25ufc61RjAf/214g/P4r49TF/vwFly1B86s9IgKEJAIBdLecf6c/Us6/5r7/9PAT4+ptFCKDzBCjZ8jMyg4D8jDQWANTECACwq/yCk2MAMPVtbCcDgZrYDwDURAAA7CmXtNnWTpr6NnZ2H1w/G/EHXShAJQQBQA2MAAB7+u2bAgAWpm55zbGA/+svxgKAeuRnVX5O5qnT3G8CUCIBAPBaWfDBUhbi+ffEVPsBcsnWjZ3FF2phFFC6/Iz8l3+yKBAolxEA4LW++iTiLSfa2MOULa8ZOuR+gHNCKaAS+dn4/k1BAFAOHQDAa2XR9c+/C9jVsuU1X70ePIlRLa8F5NnAfGEzFgCULj+n/tt703VNAexHBwDwWtl2ncsAYT9TLwrMFtu8FvC2DhWgEhYFAnMTAAD7ynOAXls5qKmDgBwL+GDd36NAHfIzMnebXN8JgMkZAQD29ZtfGQPg4JYXA7J7JLf4j/3Sdf9JxP/9l4hfH1uMJACUzKJAYE46AIB9GQPgKCwKBNhbfjZeuLUIMwHGJgAADsQYAEc1ZRCQnQDXz0a863QgUAn7AYApGAEADsQYAEeVRfnm+jTbsP//vy2+TOcIQv77Cq+A0i0/I/Pz6v/9/1wMAMYhAAAO5D/+czGvCEc1ZRCQs7W5bEsQANQiw/bco2I/ADAGIwDAgX31ScRbTq4xsCnbXp0OBGqSAcD7N40FAMPRAQAcWL6enjFTzcB0BADs7h9/tfh8TA8sCQQGoAMAOLAslnIZIIxJRwDAz+Vn4nsf6wYAjkYAAKwkzwHqAmAKeRIrg4ApTmMJAoBabG0vPhsBDsMIALCSXEr0X/9LwOiyGM/CPJdh/Y//HHcZltEAoBYZwufn1F/+6lIAsDodAMBKsjD68hMFEtPLttd89coRgbHpCABKl5+JuSDQpQBgFToAgJXkOcDf/GpxpgimlKFTdgNkcZ6dKN99P97r14sdAb8+JggAypOfif/tvcUfWxAIHJQOAGBl2Xr4+R8DZjfVwsBsud3aiHjX/gugQNefB5aX7wbAvgQAwKFYBkhJMgi483D8hYHZCZBBwLm1AChKdi7lSIArAcDrCACAQ8niP0MAKMlUewIEAUCJnAoE9iMAAA7t2z9ZBkiZ8svv9qPFHP+YX4QzCMi9BB+s2xMAlEEIALyOAAA4tHwBzU3pULKpxgNcDgBKkQtSMwRwIQB4lQAAODQnAanJcjwgOwPGvJ2d4zGb6xF/OB0AsxECALsRAABHcv3sov0ZapFfijMEGPt6wHJPQAYBQjJgDkIA4FUCAOBILAOkZjkWkOMBYy4NzCAg/3diPACYQ4YA73xkJwCwIAAAjsxJQGqXX4wzDMilgWO+lOXCwBwPeNf/XoAJWQwILAkAgCPTBUBLMgDIICADgbG+LC/HAzII0BUATCE/z7ITYMwdKED5BADAIHQB0KKxLwjkboDsCjAeAEwhP8uyEwDolwAAGIQuAFqWL2d/fhRxfWe8roD831CeEjy3FgCjyc+xy3cD6JQAABiMLgB6sFwcONaIgKWBwNgyAMggAOiPAAAYjC4AepMjAtkZkGcFx6ArABiDywDQLwEAMChdAPRoeUVgrH0BugKAoeXC09wHYCkg9EUAAAxKFwC9G/ukoK4AYCj2AUB/BADA4HQBwMKYywOXXQGb6xG/PxEAh5JdAGNdOgHKIwAABqcLAH4uuwFyRCD3BQwdBpw6sQgC3j1pRABYTX4e5T4AowDQBwEAMApdALC3ZRiQr25DjwnkeIBdAcAqrm5HbG0H0AEBADAKXQBwMMsxgbwoMGQYcPuiPQHAwbgKAP0QAACj0QUAqxnymsDxYxFffrL4CbCf/MzJfQBA2wQAwGh0AcDhLcOA7A7IvQGH+td4HgC8ZRQAOCALAaF9AgBgVNqQYRgZAmQYkF/OD9qm++2fdAAAB6cLANonAABGpQ0Zhpdf0h88WYQCe+0NyGWAn14MgJXoAoC2CQCA0W1tLLaSA8N7cVRg+aV9eQlA8AasShcAtE0AAIwui5DP/+gsGQDUQBcAtOsXATCyPC90+W4AABXQtQftEgAAk9h+5DUBAGqQV3yMEEGbBADAZC7cCgCgApvrATRIAABMJpeVXd0OAKBwHwgAoEkCAGBS13cOfsMcAJhHjgDkKADQFgEAMKlcCGgUAADKd24tgMYIAIDJ5TLAXAoIAJRr47RlgNAaAQAwi+wCyG4AAKBMWfyfOhFAQwQAwCyy+LcQEADKll0AQDsEAMBsciFgjgMAAGWyBwDaIgAAZmUUAADKlWMAb78ZQCMEAMCs8iSgUQAAKJcxAGiHAACYXY4CuAoAAGX6vUWA0AwBAFCEHAXIbgAAoCxnTgbQCAEAUITcA5AhAABQltwBYA8AtEEAABQjLwLc2AkAoDCnjAFAEwQAQFG2tiMePw0AoCACAGiDAAAoSo4CvH/TaUAAKMlbRgCgCQIAoDhOAwJAWSwChDYIAIAi5WlA+wAAoAzHjwXQAAEAUKzcB+A0IADMLwMAlwCgfgIAoFi5B+C9j+0DAIAS6AKA+gkAgKJlB8CFWwEAzMwlAKifAAAo3vYjSwEBYG46AKB+AgCgCrkP4P6TAABmIgCA+gkAgGq8f9NSQACYy1uWAEL1BABANSwFBACAwxMAAFXJDoDsBAAApvVbHQBQPQEAUJ3cBXD5bgAAACsQAABVur7jMgAAAKxCAABUKy8D3HkYAMAELAGE+gkAgKqdvxXx+GkAAAD7EAAA1cvLAM4DAgDA6wkAgOotzwMKAQAAYG8CAKAJWfwLAQBgPF/7PRaqJwAAmrEMAbIjAAAAeJkAAGiKEAAAAHYnAACak1cBhAAAAPAyAQDQJCEAAAzrSzsAoHoCAKBZQgAAGM7f/H4K1RMAAE0TAgDAMPxeCvUTAADNEwIAwNH5fRTqJwAAuiAEAICjyd9LgboJAIBu5BeXdz5anAoEAFYjRIf6CQCArmTxn50AQgAAWI0OAKifAADojhAAAFaj+Ic2CACALi1DAF9oAGB/XwvNoQkCAKBbyxDgzsMAAF5DYA5tEAAAXcuFRudvRVzdDgBgD/efBNAAAQDAc1vbQgAA2IsOAGiDAADg7zIEeP+mM0cA8KIHT/zeCK0QAAC8YPtRxDsfuRAAAEte/6EdAgCAVyyXA5p3BIBFOA60QQAAsItlCGAvAAA9y98PBeLQDgEAwGvkXoALt8w+AtAnxT+0RQAAsI/bD+0FAKBPf9b+D00RAAAcQBb/GQLc2AkA6EL+3mf+H9oiAAA4oBwD2LwbcfmukQAA2qf9H9ojAABY0fUdIwEAtO/OwwAaIwAAOIQs/n/7r64EANAm2/+hTQIAgCPIKwF5LlA3AAAtEXBDmwQAAEeULyQZAmiVBKAVXv+hTQIAgAFkB8D5WxEXbukGAKBuef7W72XQJgEAwIDyS5NuAABqpv0f2iUAABjYshvAbgAAauP1H9omAAAYSc5PuhQAQE10sEHbBAAAI8tLARkE+FIFQMkePLH8D1onAACYgCWBAJRuS8caNE8AADChnK1cjgV8930AQBG8/kMf3ohzz54FAJN7+83nry0bEefWAgBmleG0DjVonw4AgJksxwLsBwBgTjb/Qz90AAAUIjsCbl+MePdkAMBkvP5DP3QAABQiv3yd+TjivY8Xs5gAMLYbO4p/6IkOAIBCnTm52BGgIwCAMWThn6GzAAD6oQMAoFC5jVlHAABjyYs0in/oiw4AoGnHj0WcX4v4/YnFH6evv1l84Xn8tK6TRzoCABhK/h74zkcBdEYAADQpi/0rz4vlzfXX//O++34RAvz50WILcg2cDwTgqCz+gz4JAIDmZIH82YeLn6vIL0LZDikIAKBl+Xvd1nYAHRIAAM3J4v/MEdrkBQEAtCp/j8vXf6BPAgCgKTnv/+nFGMQyCMgRgRraJJdBQO4IWLX7AYA+aP2HvgkAgKZ8/seIUydiUPlFKbsB7jysJwjIDojcgSAIAGBJ6z8gAACakcXul5/EaLL4z26Ams4mZUeEIAAArf9AEgAAzdg4HXHvUkwiOwJqCgKcEATom9Z/IP0iABoxdOv/6+TLenYb5L6BGl7Xs3PhzMeLL4B3KlluCMAwagqsgXHpAACacfvifJvwl6MB+bMG9gQA9OHB3wNggCQAAJqxfSniD6djVrWdEEwZBGyuz//nDoBh5e9J733s9R/4iREAgAHla3qOBeR4QI4J1DIesHFzMR6gTRSgHZfv+kwHXqYDAGjGnCMAe6nxckDKhYr5q7Q/nwAcjJN/wG4EAEAzSgwAXpRjAbmAr5Y9AWm5KyBHBH4/4ZJFAA7v8dOIdz4KgJ8RAADNyDN3udSudPnF7MZOXXsCUoYBy1OCFgcClMncP/A6AgCgGTlzn/P3tah1PCBlV0D++RYGAJQlX/4zaAbYjQAAaEbOrN+7FFWqcTxgSRgAUIZc+nd9JwD2JAAAmpHFZ27fr9nyjGAGATW2b2YYkEFMnhQUBgBMx9I/4CAEAEAzjh+L+PZP0YyauwLSqRM/BQLZHQDAOB48/33izMcBsC8BANCUrz6JeKuxl+fauwJShjMvhgG6AwCGYekfsAoBANCU7UuL9vNWbT9adAXkz5otzwtmIJDnBQUCAKtT/AOrEgAATbl+NuKD9Wje8oJAnhNsYdtzjgssRwbyZ4YCAOztu+8Xxb+N/8AqBABAU2o7BTiEDAMyCMiugFZegZYjA8tQIAOB/McAWLhwa7ErBmAVAgCgKS1cAjiK7ApYLg5srSX01ImfRgeWXQJCAaBHNv4DhyUAAJqTlwAUhm2HAUv513k5PpDhgGAAaJ3iHzgKAQDQnNYXAR5GD2HAi14NBpa/8kKEcAColeIfOCoBANCczfWIa2eDPeTCqLwZnbOjPS6PWoYDu/0UEAClyhD3/K0AOBIBANCcLOY+/2NwAMtrAn9+tPiZW6X5qWMgw4BXf7746y3nC4EJ5JLX928GwJEJAIAm2QNwOMswIDsD8o/Z36uhwKu/MpAykgIcVn4e57k/AS0wBAEA0KTbFyPOrQVHkF82MwTIcYH86db04WX3QI6mfLAeAAem+AeGJgAAmnT+efH/6cVgQC8GAjoEDmdrI+LKRgDsS/EPjEEAADQpW69zDIBxZQjwxdOfrgvoEtif8RRgP4p/YCwCAKBZn30YceZkMKH8sppfXIUCe/P3JfA6+bmZxX8PJ1uB6QkAgGY5B1iO5YWBB38PBXoOBr78ZLETAOBVin9gbAIAoFnGAMqXIUAGA9kxsAwGlr9aa33Nvx8zkDpvOSWwC8U/MAUBANA07db1ygBgGQTkz6+/efkfW/4q/ctyvvZvnF5cAPDyD+xG8Q9M5ZcB0LC8aS8AqFO+mJ86cfB//ovBQHrxj79+5Uv1i/9vq/zneXV531tv/vz//cV/noIf2I/iH5iSDgCgaVmI5cy1resAlEbxD0ztFwHQsOXiOQAoieIfmIMAAGje9Z0AgGIo/oG5CACA5i1P0AHA3BT/wJwEAEAXbugCAGBmin9gbgIAoAs5BqALAIC55O9B799U/APzEgAAXcgvXnceBgBMLn8Pypf/x08DYFYCAKAb248CACal+AdKIgAAupHLAO87CQjARBT/QGkEAEBXrm4HAExC8Q+URgAAdEUXAABTuHBL8Q+URwAAdEcXAABjyuL/tsWzQIEEAEB3dAEAMBbFP1AyAQDQJV0AAAxN8Q+UTgAAdEkXAABDUvwDNRAAAN3SBQDAEPL3E8U/UAMBANAtXQAAHFUW/1sCZaASAgCga7oAADgsxT9QGwEA0LXsALijbROAFSn+gRoJAIDu5Re4774PADgQxT9QKwEA0L2vvom4sRMAsC/FP1AzAQDAc9d3FkEAAOxF8Q/UTgAAEIsRgLzhDAC7UfwDLRAAAPxdLgTcfhQA8BLFP9AKAQDACy7ftRAQgJ8o/oGWCAAAXpB7AK76ogdALM7EKv6BlggAAF6RCwFzHACAfmXxf95uGKAxAgCAXeRCQKMAAH1S/AOtEgAA7MIoAECfFP9AywQAAHswCgDQl7wEo/gHWiYAAHgNowAAfXj8dPGZD9AyAQDAa+QogC+EAG3L4v+9jwW+QPsEAAD7yJbQGzsBQIMU/0BPBAAAB5B3oPNLIgDtUPwDvREAABxAfjl8/6YviQCtUPwDPRIAAByQfQAAbcjPc6Eu0CMBAMAKch/A1e0AoFJZ/OfLf/4E6I0AAGBFuQ/g/pMAoDKKf6B3AgCAQ8jWUV8gAeqh+AcQAAAcSs6NWh4FUAfFP8CCAADgkJZLpAAol+If4CcCAIAjyF0Al+8GAAVS/AO8TAAAcETXd1wGACiN4h/g5wQAAAPIywA3dgKAAij+AXYnAAAYyObdiDsPA4AZKf4B9iYAABjQ+VsRj58GADPIyyzOtALsTQAAMLB8eRICAExreZ7V5y/A3gQAAAPzJRRgWj53AQ5GAAAwAl9GAabh8xbg4AQAACPxpRRgXD5nAVYjAAAYkS+nAOO5fNfnK8AqBAAAIxMCAAzvwq2I206vAqxEAAAwgQwB3vko4o4vqwBHpvgHOBwBAMCEzj//0np1OwA4JMU/wOEJAAAmtrUtBAA4DMU/wNEIAABmkCFALq8C4GAyOFX8AxyNAABgJtd3FnsBvvomAHiNLP63dE4BHJkAAGBGeRkgLwQIAQB2p/gHGI4AAGBmWfy7EADwc4p/gGEJAAAKkGcCXQgA+IniH2B4b8S5Z88CgGKcOhFx71LE228GQJcU/wDj0AEAUJjlXoDtRwHQHcU/wHgEAAAFyr0A7980EgD0RfEPMC4jAACFy1GAzz40EgC0TfEPMD4dAACFy26A3/6rbgCgXYp/gGnoAACoiG4AoDV5AjWvoAAwPh0AABXRDQC0RPEPMC0dAACVyi6A62cj/nA6AKqj+AeYng4AgEplN8DGzYgLtxZ/DFALxT/APHQAADRiayPiykYAFE3xDzAfAQBAQ3IsIIOAc2sBUJzHTyPe+SgAmIkAAKBBggCgNFn8v/dxxHffBwAzEQAANEwQAJRA8Q9QBgEAQAcEAcBcFP8A5RAAAHRkGQS8e3LxxwBjUvwDlEUAANCp82uLqwGCAGAMeZ40i39nSgHKIQAA6NyZk4sw4A+nI44fC4AjU/wDlEkAAMCPMgjYOL0IAwAOQ/EPUC4BAAA/k2MB2RkgDABWofgHKJsAAIDXyrGAZRhgeeDh5RK0r79ZLEXL4ij/PPrzSUsU/wDlEwAAsJJTJxa/MhD4/QkF7KteLPTzV/7f95/sXhTln7t7lxZ/PqFmin+AOggAADiSLGKzgM0ugfyZoUDLywSzoP/b9z8V9y/+zOJn1XNn+eftsw8DqqX4B6iHAACAwWUAcOrE7j/z11uFdQ18/ffCZVnA569X//jF/3tI+efj2z8FVEnxD1AXAQAAs1iGAcsRguX/vfz1ooOOGbxahLxasH/1zcs/xyjoVyUAoFaKf4D6/DIAYAYvvq73zPw/NVL8A9TpFwEAzObTiwFVyeBO8Q9QJwEAAMzkyoYrCtRF8Q9QNwEAAMwgC/+tjYBqLIv/vHgBQJ0EAAAwsSz+nf6jJop/gDYIAABgYlta/6mI4h+gHQIAAJjQ5nrEubWAKij+AdoiAACAieSr/7WzAdW4fFfxD9ASAQAATMDcP7W5cCvi9sMAoCECAACYQL78m/unFop/gDYJAABgZFc2IjZOB1QhC3/FP0CbBAAAMKIzJxdb/6EWN3YCgEYJAABgJNny/+nFgKpY+gfQLgEAAIwkl/6Z+6c2/p4FaJcAAABGYOkftfpgPQBolAAAAAaWS/82FVFUKv/ezdGVUycCgMa8EeeePQsAYBBZNH3+xwAa9dU3i18PniyuJeQfA9RCAAAAA8mWf3P/0Jf7z4OAC7cEAUAdjAAAwEDuXVL8Q2/y1OeXnyxGfwBKJwAAgAHk0j8z09CvrQ0hAFA+AQAAHJGlf0DKECA7AgBKJQAAgCPIV/8tr37A3+UFBYBSCQAA4JBy3j/n/gGW8nNBFwBQKgEAABzC8WM2/gO7O7cWAEUSAADAIeTcv+If2I0OAKBUAgAAWJGlf8DrCAeBUgkAAGAFG6ct/QMA6iQAAIADylc9G74BgFoJAADgALL4z6V/ufwP4HW++iYAiiQAAIADyJd/c73AQXzxNACKJAAAgH3k0j9bvYGD2n4UAEUSAADAa+S2f0v/gIPK9v/bDwOgSAIAANjDqRMR184GwIHdUfwDBRMAAMAuct7/3qUAOLB8/d/aDoBiCQAA4BW56T83/lv6BxxUFv/vfRwARRMAAMArcumf4h84qGXx7/wfUDoBAAC8IIv/XPwHcBA3diLe+UjxD9ThlwEA/GDjtI3/wMFkwX/hVsT9JwFQDQEAAMSi5f/TiwGwr3z1z2V/330fAFURAADQvSz+c+lfLv8D2ItXf6B2AgAAupfn/iz9A17Hqz/QAgEAAF27djbi1IkA2NXjp4tX//wJUDsBAADdsvEf2Eu+9C9f/QFaIQAAoEtnTtr4D+zuwZOI87ec9gPaIwAAoDs2/gO7yVf/q89f/K/vBECTBAAAdCU3/efGf0v/gBd59Qd6IAAAoCv58q/4B5a8+gM9EQAA0I1c+rdxOgB+4NUf6I0AAIAu5LZ/S/+A5NUf6JUAAIDmZcv/FcU/8Nz2o4gLtxYhAEBvBAAANC2L/1z6l8v/gH5lwZ+FfwYAAL0SAADQtHuXLP2D3t3Yidja9uoPIAAAoFnXzkacOhFAp3K5X776338SAIQAAIBG5cx/Lv4D+uTVH+DnBAAANCdf/W38hz559QfYmwAAgKbkvH/O/QP98eoP8HoCAACakZv+c+O/pX/QF6/+AAcjAACgGTn3r/iHvnj1Bzg4AQAATbD0D/ri1R9gdQIAAKp35qSlf9ATr/4AhyMAAKBq2fL/6cUAOuDVH+BoBAAAVMvSP+iHV3+AoxMAAFAtS/+gfY+fRly+69UfYAgCAACqZOkftO/q9uLVH4BhCAAAqM6pE5b+Qcvy1T9n/fMnAMMRAABQlWz5v3cpgEZ59QcYjwAAgKpk8W/uH9rj1R9gfAIAAKqRc//Z/g+0xas/wDQEAABU4fyauX9ojVd/gGkJAAAoXrb8X1H8Q1O8+gNMTwAAQPE++9DcP7TCqz/AfAQAABQtX/4V/9AGr/4A8xIAAFCsLPzN/UP9vPoDlEEAAECxFP9QP6/+AOUQAABQrHdPBlApr/4A5REAAFCkjdNm/6FWN3YWr/7ffR8AFEQAAECRznj9h+p89c3i1f/+kwCgQAIAAIr0+xMBVMSrP0D5BAAAFOm32v+hCl79AeohAAAA4FC8+gPURQAAQJG+fV5QvBVAibz6A9TpFwEABfrC6TAoUr76v/OR4h+gRjoAAChS3g4/F0ApvPoD1E8HAABFuv3QXDGUwqs/QBsEAAAUKYv/LDqA+eSr/3sfR2zeFcgBtEAAAECxru8sChBgetuPvPoDtEYAAECx8sXx/ZteHmFKy//d+d8eQHsEAAAULZcB5iukTgAY34Pnr/2//dfF6z8A7REAAFC85RzynYcBjCBf+i/fjTjzsVd/gJa9EeeePQsAqMTbb0ZsbUT84XTE8WMBHFG++p+/pcsGoAcCAACqdebk4tepE4swIMOBUv3qmMCCsuRL/9XtxbJNAPogAACACWUIkIFF/to4HfHuyYDJefUH6JMAAABmtBxpOLcWMImc9ffqD9AnAQAAFCCDgM8+LHuMgbrlRY0LtxY/AeiTKwAAUIBsxc5zh4ozxnBjx99fAAgAAKAYuZQtzx2ay2YoyxOam3cDAAQAAFCSDAGyTRuOavnqf/9JAMAPfhkAQFGyYMtfZ1wI4BDy1T9DJIU/AK/SAQAABfrzo4CVbT/y6g/A3gQAAFCgbQEAK1iOjrx/c/HHALAbIwAAUCBFHAf14Plr//lblkcCsD8BAAAUSADAfvLvkavbEdd3AgAORAAAAAU6fixgT4+fLtr9vfoDsAoBAAAU6O03A3aV5/027wYArEwAAAAFOnUi4CXO+wFwVAIAACjQmZMBP8pX/61tuyEAOBoBAAAU6A+nA7z6AzAoAQAAFOb8miWARGw/WhT/Xv0BGIoAAAAKc2Uj6JjzfgCMRQAAAAXJ138XAPr14MnzvwduOe8HwDgEAABQEK///bp816s/AOMSAABAIbz+9ylf+9+/GfH4aQDAqAQAAFCALPy9/vfHeT8ApiQAAIACfLDu9b8nWfDnq7/zfgBMSQAAADPLwn9zPeiERX8AzEUAAAAz29L63wXn/QCYmwAAAGa0cTri3FrQuFzwly3/Xv0BmJMAAABmdO1s0Lhc9Ld5NwBgdgIAAJiJs39ty9f+C7cs+gOgHAIAAJiBs39t2360KP6d9wOgJAIAAJiBs39tsugPgJIJAABgYs7+tcl5PwBKJwAAgIk5+9cei/4AqIEAAAAmlIv/nP1rh0V/ANREAAAAE7L4rx13Hi5e/S36A6AWAgAAmMimxX9NsOgPgFoJAABgAln4f2DxX/UeP414/6ZFfwDUSQAAABPIxX9e/+tm0R8AtRMAAMDIsvC3+K9eFv0B0AoBAACM7NOLQaW2Hy2Kf4v+AGiBAAAARpRn/86cDCpj0R8ALRIAAMCInP2rj0V/ALRKAAAAI7li8V91ctHf1raWfwDaJAAAgBFk4b/l9b8aWfDnq79FfwC0TAAAACNQ/NfjwfOi//wtLf8AtE8AAAADO3XC2b9aXL5r0R8A/RAAAMDA7l0KCpev/XneT8s/AD0RAADAgPLsn8V/ZbvzMGLzrkV/APRHAAAAA8nC39m/cmXBf3Vbyz8A/RIAAMBAtpz9K9bjp4st/xb9AdAzAQAADCALf4v/ynTj+Yv/1raWfwAQAADAAK6dDQqTBX8u+tt+FABACAAA4Mhy8d/G6aAgD548/+tyS8s/ALxIAAAAR2TxX1my5T+3/AMALxMAAMARXLH4rxj52p8t//efBACwCwEAABxSFv5bXv+LoOUfAPYnAACAQ1L8l+Hy3YjrOwEA7EMAAACHkEv/nP2bl5Z/AFiNAAAADsHZv3nlab8s/vPUHwBwMAIAAFjR5rrFf3PS8g8AhyMAAIAVZOH/wXowg2z5f/9mxOOnAQAcggAAAFaw5ezfLO48jNi8q+UfAI5CAAAAB5SFv8V/08qC/+q2ln8AGIIAAAAO6LMPgwlp+QeAYQkAAOAAzq9p/Z+Sln8AGJ4AAAD2kYX/lY1gAlr+AWA8AgAA2IfFf9PQ8g8A4xIAAMBrWPw3DS3/ADA+AQAAvMa9S8GItPwDwHQEAACwh1z8d+pEMBIt/wAwLQEAAOzC4r9xafkHgOkJAABgFx+sW/w3lst3tfwDwBwEAADwiiz8N9eDgWn5B4B5CQAA4BXXzgYD234UceGWln8AmJMAAABekIv/Nk4HA9LyDwBlEAAAwN8dP2bx35Cy5T9f/e8/CQCgAAIAAPg7i/+G8+B50X/+1iIEAADKIAAAgFgU/lte/wdxdfv5n8vtAAAKIwAAgFD8DyEX/OWWfy3/AFAmAQAA3cvFf+fWgiPI035Z/Gv5B4ByCQAA6J7Ff0dzYydi824AAIUTAADQtSz+Lf47nGz5zxN/tx8GAFABAQAA3bL47/Cy1f+9j7X8A0BNfhEA0CnF/+Fky/87Hyn+AaA2OgAA6JLFf4eTLf/XdwIAqJAAAIAuWfy3mnztzy3/ue0fAKiTAACA7lj8t5oHTyI2bi6W/gEA9RIAANAVi/9Wc3X7+Z+v7QAAGiAAAKAriv+Dydf+bPm//yQAgEYIAADohsV/B5Nz/ln82/IPAG0RAADQDYv/9pcn/rLl37w/ALRHAABAFyz+e70s+HPe34k/AGiXAACA5ln893pO/AFAHwQAADTv2tlgD078AUA/BAAANC0X/22cDnbhxB8A9EUAAECzjh+z+G83+dp/4VbE9qMAADoiAACgWR+sW/z3Kif+AKBfAgAAmmTx38/deRixede8PwD0SgAAQJPuXQpecPmuE38A0DsBAADNycV/p04EsWj1z3n/+08CAOicAACApmTrv8V/C+b9AYAXCQAAaIrFfws3dhbz/gAASwIAAJqRhf/menQtF/xd3TbvDwD8nAAAgGZ89mF0LVv9s+U/W/8BAF4lAACgCbn4r+fW/wdPIjZuOvEHAOxNAABA9Xpf/GfeHwA4CAEAANXb2ujz9T9f+y8/L/xvPwwAgH0JAACoWhb+59aiO+b9AYBVCQAAqFqPi//M+wMAhyEAAKBaefKvt9Z/8/4AwGEJAACoUhb+H6xHN8z7AwBHJQAAoEo9Lf4z7w8ADEEAAEB1Nk73s/jPvD8AMBQBAADVuXY2umDeHwAYkgAAgKpc6aD137w/ADAGAQAA1cjCP2f/W2beHwAYiwAAgGq0XvznvP/5W4sQAABgaAIAAKpwfq3txX/m/QGAsQkAACje8WOL2f9W5bz/9Z0AABiVAACA4rW6+M+8PwAwJQEAAEXLwn9zPZqTRX8W/+b9AYCpCAAAKNpnH0Zzct5/a3tx7g8AYCoCAACKlYv/Wmv9N+8PAMxFAABAkbLwb2nxX772Z8v//ScBADALAQAARdpqaPFfzvm/97F5fwBgXgIAAIpz6kTEubVowvajiAu3zPsDAPMTAABQnHuXoglXtxfL/gAASiAAAKAoVxpo/c/X/lz2d/thAAAUQwAAQDGy8N+qfPFfzvnnsr/HTwMAoCgCAACKUXvxn0V/Fv+W/QEAJRIAAFCE82t1L/67sROxeTcAAIolAABgdtn6f6Xi1/+c97++EwAARRMAADC7D9brXPyXy/6y5f/+kwAAKJ4AAIBZZeG/uR7VyTn/9z427w8A1EMAAMCsPvswqvPg+Yv/xs1FBwAAQC0EAADMZrPC1n/L/gCAWgkAAJhFFv4fVNb6b9kfAFAzAQAAs9jaqOf137I/AKAFAgAAJnd+LeLcWlTBsj8AoBUCAAAmdfxYxJWNqML2o4gLtyz7AwDaIAAAYFIfVLL47+p2xNZ2AAA0QwAAwGSy8N+q4PXfsj8AoEUCAAAm89mHUbSc889lf4+fBgBAcwQAAExis/DWf8v+AIDWCQAAGF0W/jn7X6oHTyI2blr2BwC0TQAAwOhy7r/U1/8bOxGbdwMAoHkCAABGdX4t4txaFMmyPwCgJwIAAEZz/FjElQK3/merfy77u/8kAAC6IQAAYDRXCmz9t+wPAOiVAACAUWThv1nY4j/L/gCAngkAABjFZx9GUSz7AwB6JwAAYHCltf5b9gcAIAAAYGBZ+G8VsvgvW/0v3IrYfhQAAN0TAAAwqGtnowiW/QEAvOwXAQADOb8WsXE6Zvf4qeIfAOBVOgAAGES2/l8poPX/zsPFsj+b/gEAXiYAAGAQWwUs/ru6/fw/x3YAALALAQAAR5at/+fWYla57O/2wwAAYA8CAACObM7W/2z1z3n/nPsHAGBvAgAAjiRf/+dq/bfpHwDg4AQAABzJXFv/Hzx5/u9907I/AICDEgAAcCTvnozJ3dhZbPoHAODgfhEAcEinTkQcPxaTyk3/in8AgNXpAADg0KYs/rPV//Jdm/4BAA5LAABA8XLJ3/s3bfoHADgKAQAAhzbF9n2b/gEAhmEHAACHlkX51yMW5rnp/52PFP8AAEMQAABwJGPN5Oem/zMfO/MHADAUAQAAR3J9Z/gi3aZ/AIDhCQAAOJIs/rNgH+pf68KtiK2B/vUAAPiJJYAAHFl2AeRJwCsbcWg2/QMAjEsAAMAglq/2hwkBsujP4t+yPwCA8bwR5549CwAYyNtvRty+GPHuyf3/udnyn8v+xtgjAADAywQAAIwig4DN9YhTJyJ+f2IxIpDybGC++N9/srggoPAHAJiGAAAAAAA64AoAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAHBAAAAADQAQEAAAAAdEAAAAAAAB0QAAAAAEAH/idQxQ8GCs7w+QAAAABJRU5ErkJggg=="
};

// src/adapters/dingtalk/index.ts
import { createHash as createHash2, randomUUID } from "node:crypto";
import { fromAcpOptionKind } from "@agnes/protocol";

// src/adapter.ts
var adapter_exports = {};
__reExport(adapter_exports, channel_contract_star);
import * as channel_contract_star from "@agnes/plugin-runtime/channel-contract";

// src/decision.ts
function decisionRef(key) {
  try {
    const parts = key.split("/").map(decodeURIComponent);
    if (parts.length !== 3) return void 0;
    const [sessionId, id, rev] = parts;
    const revision = Number(rev);
    if (!sessionId || !id || sessionId.length > 512 || id.length > 256 || !Number.isSafeInteger(revision) || revision < 1)
      return void 0;
    return { sessionId, id, revision };
  } catch {
    return void 0;
  }
}

// src/degrade.ts
function approvalKey(block) {
  if (block.decisionKey) return decisionRef(block.decisionKey)?.id.slice(0, 6);
  if (block.requestSeq !== void 0) return `#${block.requestSeq}`;
  return void 0;
}
function approvalAsText(block) {
  const lines = [`\u3010${block.title}\u3011`, block.summary, `\u98CE\u9669\uFF1A${block.risk}`];
  if (block.expiresAt) lines.push(`\u622A\u6B62\uFF1A${block.expiresAt}`);
  const key = approvalKey(block);
  if (key) lines.push(`\u56DE\u590D\u300C\u540C\u610F ${key}\u300D\u6216\u300C\u62D2\u7EDD ${key}\u300D`);
  return lines.join("\n");
}
function cardAsText(block) {
  const lines = [`\u3010${block.title}\u3011`, block.body];
  for (const [key, value] of block.fields ?? []) lines.push(`${key}: ${value}`);
  if (block.actions?.length) {
    block.actions.forEach((action, index) => {
      lines.push(`${index + 1}. ${action.label}`);
    });
    lines.push(`\u56DE\u590D ${block.actions.map((_, index) => index + 1).join(" / ")} \u9009\u62E9`);
  }
  return lines.join("\n");
}
function tableAsText(block) {
  const lines = block.title ? [`\u3010${block.title}\u3011`] : [];
  lines.push(block.columns.join(" | "));
  for (const row of block.rows) lines.push(row.join(" | "));
  return lines.join("\n");
}
function degrade(message, capabilities, options = {}) {
  const degraded = /* @__PURE__ */ new Set();
  const blocks = message.blocks.map((block) => {
    if ((block.kind === "card" || block.kind === "table" || block.kind === "approval") && !capabilities.card) {
      degraded.add("card");
      const markdown = block.kind === "card" ? cardAsText(block) : block.kind === "table" ? tableAsText(block) : approvalAsText(block);
      return { kind: "text", markdown };
    }
    if (block.kind === "file" && !capabilities.attachment) {
      degraded.add("attachment");
      return block.url ? { kind: "link", title: block.name, url: block.url } : { kind: "text", markdown: `${block.name}\uFF08\u65E0\u6CD5\u53D1\u9001\uFF09` };
    }
    return block;
  });
  if (options.updating && !capabilities.edit) {
    degraded.add("edit");
    const first = blocks[0];
    if (first?.kind === "text") {
      blocks[0] = { kind: "text", markdown: `\uFF08\u66F4\u65B0\uFF09${first.markdown}` };
    } else {
      blocks.unshift({ kind: "text", markdown: "\uFF08\u66F4\u65B0\uFF09" });
    }
  }
  return { msg: { ...message, blocks }, degraded: [...degraded] };
}

// src/adapters/dingtalk/index.ts
init_errors();

// src/manifest.ts
init_errors();
import { readFile } from "node:fs/promises";
import { validateChannelManifest } from "@agnes/protocol";
function checkManifest(input) {
  const result = validateChannelManifest(input);
  if (!result.ok) {
    const first = result.errors[0];
    throw new ChannelError(
      "E_MANIFEST_INVALID",
      `schema: ${first?.path ?? ""} ${first?.message ?? "invalid"}`,
      { errors: result.errors }
    );
  }
  const manifest = result.value;
  if (!manifest.connection.modes.includes(manifest.connection.default)) {
    throw new ChannelError(
      "E_MANIFEST_INVALID",
      `connection.default ${manifest.connection.default} not in modes ${manifest.connection.modes.join(",")}`
    );
  }
  const overlap = manifest.credentials.required.filter((key) => manifest.credentials.optional.includes(key));
  if (overlap.length > 0) {
    throw new ChannelError(
      "E_MANIFEST_INVALID",
      `credentials required/optional overlap: ${overlap.join(",")}`
    );
  }
  if (manifest.events.supported.includes("cardAction") && !manifest.capabilities.card) {
    throw new ChannelError(
      "E_MANIFEST_INVALID",
      "events.supported has cardAction but capabilities.card is false"
    );
  }
  return manifest;
}
async function loadManifest(path) {
  let text;
  try {
    text = await readFile(path, "utf8");
  } catch (error) {
    throw new ChannelError("E_MANIFEST_INVALID", `cannot read ${path}: ${errorName(error)}`);
  }
  let input;
  try {
    input = JSON.parse(text);
  } catch (error) {
    throw new ChannelError("E_MANIFEST_INVALID", `${path} is not JSON: ${errorName(error)}`);
  }
  return checkManifest(input);
}
function errorName(error) {
  return error instanceof Error ? error.name : "unknown error";
}

// src/transport/backoff.ts
function* backoffDelays(options = {}) {
  const baseMs = options.baseMs ?? 1e3;
  const maxMs = options.maxMs ?? 6e4;
  const jitter = options.jitter ?? Math.random;
  let exponent = 0;
  while (true) {
    const raw = Math.min(maxMs, baseMs * 2 ** exponent);
    exponent++;
    yield Math.round(raw * (0.8 + 0.4 * jitter()));
  }
}

// src/transport/throttle.ts
var TokenBucket = class {
  tokensPerSecond;
  capacity;
  now;
  sleep;
  tokens;
  refilledAt;
  tail = Promise.resolve();
  constructor(options) {
    if (!Number.isFinite(options.tokensPerSecond) || options.tokensPerSecond <= 0) {
      throw new RangeError("tokensPerSecond must be greater than zero");
    }
    if (!Number.isFinite(options.capacity) || options.capacity < 1) {
      throw new RangeError("capacity must be at least one");
    }
    this.tokensPerSecond = options.tokensPerSecond;
    this.capacity = options.capacity;
    this.now = options.now ?? Date.now;
    this.sleep = options.sleep ?? ((delayMs) => new Promise((resolve) => setTimeout(resolve, delayMs)));
    this.tokens = options.capacity;
    this.refilledAt = this.now();
  }
  take(signal) {
    const turn = this.tail.then(() => this.acquire(signal));
    this.tail = turn.catch(() => void 0);
    return turn;
  }
  async acquire(signal) {
    while (true) {
      signal?.throwIfAborted();
      const now = this.now();
      const elapsedSeconds = Math.max(0, now - this.refilledAt) / 1e3;
      this.tokens = Math.min(this.capacity, this.tokens + elapsedSeconds * this.tokensPerSecond);
      this.refilledAt = now;
      if (this.tokens >= 1) {
        this.tokens -= 1;
        return;
      }
      const waitMs = Math.max(1, Math.ceil((1 - this.tokens) / this.tokensPerSecond * 1e3));
      await abortable(this.sleep(waitMs), signal);
    }
  }
};
async function abortable(promise, signal) {
  if (signal === void 0) return promise;
  signal.throwIfAborted();
  return new Promise((resolve, reject) => {
    const aborted = () => reject(signal.reason);
    signal.addEventListener("abort", aborted, { once: true });
    void promise.then(resolve, reject).finally(() => signal.removeEventListener("abort", aborted));
  });
}

// src/adapters/dingtalk/cards.ts
import { createHash } from "node:crypto";
var APPROVAL_LABEL = {
  allow_once: { text: "\u540C\u610F", color: "blue" },
  allow_always: { text: "\u672C\u4F1A\u8BDD\u90FD\u540C\u610F", color: "blue" },
  reject_once: { text: "\u62D2\u7EDD", color: "red" },
  reject_always: { text: "\u672C\u4F1A\u8BDD\u90FD\u62D2\u7EDD", color: "red" }
};
function cardBizIdFor(sessionKey, nodeId) {
  return createHash("sha256").update(`${sessionKey}#${nodeId}`).digest("hex").slice(0, 32);
}
function approvalValue(key, kind) {
  return `appr:${key.decisionKey ?? `#${key.requestSeq ?? 0}`}:${kind}`;
}
function slotValue(requestSeq, actionId) {
  return `slot:${requestSeq}:${actionId}`;
}
function buildCard(blocks, ids, maxBytes) {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1) {
    throw new RangeError("maxBytes must be a positive integer");
  }
  let title = "";
  const markdown = [];
  const buttons = [];
  for (const block of blocks) {
    switch (block.kind) {
      case "text":
        markdown.push(block.markdown);
        break;
      case "link":
        markdown.push(`[${escapeMarkdownLabel(block.title)}](${block.url})`);
        break;
      case "file":
        markdown.push(
          block.url === void 0 ? `${block.name}\uFF08\u65E0\u53EF\u53D1\u9001\u94FE\u63A5\uFF09` : `[${escapeMarkdownLabel(block.name)}](${block.url})`
        );
        break;
      case "table":
        title ||= block.title ?? "";
        if (block.title !== void 0) markdown.push(`**${block.title}**`);
        markdown.push(markdownTable(block.columns, block.rows));
        break;
      case "card":
        title ||= block.title;
        if (block.body.length > 0) markdown.push(block.body);
        for (const [key, value] of block.fields ?? []) markdown.push(`- ${key}: ${value}`);
        for (const action of block.actions ?? []) {
          buttons.push({
            text: action.label,
            value: slotValue(block.requestSeq ?? 0, action.id),
            color: action.style === "danger" ? "red" : "blue"
          });
        }
        break;
      case "approval":
        title ||= block.title;
        markdown.push(block.summary, `\u98CE\u9669\uFF1A${block.risk}`);
        if (block.expiresAt !== void 0) markdown.push(`\u622A\u6B62\uFF1A${block.expiresAt}`);
        for (const option of block.options) {
          buttons.push({
            ...APPROVAL_LABEL[option],
            value: approvalValue(
              {
                ...block.decisionKey === void 0 ? {} : { decisionKey: block.decisionKey },
                ...block.requestSeq === void 0 ? {} : { requestSeq: block.requestSeq }
              },
              option
            )
          });
        }
        break;
    }
  }
  const make = (body) => ({
    outTrackId: ids.cardBizId,
    cardParamMap: {
      title: title || " ",
      markdown: body,
      buttons: JSON.stringify(buttons)
    }
  });
  const full = markdown.join("\n\n");
  let cardData = make(full);
  let bytes = byteLength(cardData);
  if (bytes <= maxBytes) return { cardData, bytes };
  const suffix = "\uFF08\u5DF2\u622A\u65AD\uFF09";
  const characters = [...full];
  let low = 0;
  let high = characters.length;
  while (low < high) {
    const middle = Math.ceil((low + high) / 2);
    if (byteLength(make(`${characters.slice(0, middle).join("")}${suffix}`)) <= maxBytes) low = middle;
    else high = middle - 1;
  }
  cardData = make(`${characters.slice(0, low).join("")}${suffix}`);
  bytes = byteLength(cardData);
  if (bytes > maxBytes) {
    throw new RangeError("DingTalk card byte limit is too small for the card envelope");
  }
  return { cardData, bytes };
}
function markdownTable(columns, rows) {
  const cells = (row) => row.map(escapeTableCell).join(" | ");
  return [
    `| ${cells(columns)} |`,
    `| ${columns.map(() => "---").join(" | ")} |`,
    ...rows.map((row) => `| ${cells(row)} |`)
  ].join("\n");
}
function escapeTableCell(value) {
  return value.replace(/\|/g, "\\|").replace(/\r?\n/g, "<br>");
}
function escapeMarkdownLabel(value) {
  return value.replace(/[\\[\]]/g, "\\$&");
}
function byteLength(value) {
  return Buffer.byteLength(JSON.stringify(value), "utf8");
}

// src/adapters/dingtalk/events.ts
init_errors();
var MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024;
var MESSAGE_TYPES = /* @__PURE__ */ new Set([
  "text",
  "picture",
  "file",
  "audio",
  "video",
  "richText"
]);
function requiredString(value, field, maxLength) {
  if (typeof value !== "string" || value.length === 0 || value.length > maxLength) {
    throw new TypeError(`invalid DingTalk ${field}`);
  }
  return value;
}
function optionalString(value, field, maxLength) {
  if (value === void 0) return void 0;
  return requiredString(value, field, maxLength);
}
function contentOf(message) {
  let content = message.content;
  if (typeof content === "string") {
    if (content.length > 256 * 1024) return {};
    try {
      content = JSON.parse(content);
    } catch {
      return {};
    }
  }
  return content && typeof content === "object" && !Array.isArray(content) ? content : {};
}
function attachmentFailureText(error) {
  if (error instanceof RangeError) return "\u9644\u4EF6\u5408\u8BA1\u8D85\u8FC7 20 MiB\uFF08ATTACHMENT_LIMIT\uFF09";
  if (error instanceof ChannelError) {
    const http3 = /DingTalk (API|access token|attachment download) failed with HTTP ([1-5][0-9]{2})$/.exec(
      error.message
    );
    if (http3)
      return (http3[1] === "attachment download" ? "\u6587\u4EF6\u4E0B\u8F7D\u5931\u8D25" : "\u83B7\u53D6\u4E0B\u8F7D\u5730\u5740\u5931\u8D25") + "\uFF08HTTP " + http3[2] + "\uFF09";
    if (/robotCode is not set$/.test(error.message)) return "\u673A\u5668\u4EBA\u6807\u8BC6\u7F3A\u5931\uFF08BOT_ID_MISSING\uFF09";
    if (/unsafe download URL|download address is unsafe/.test(error.message)) {
      const reasons = {
        MISSING_URL: "\u9489\u9489\u63A5\u53E3\u672A\u8FD4\u56DE\u4E0B\u8F7D\u5730\u5740",
        URL_TOO_LONG: "\u4E0B\u8F7D\u5730\u5740\u8D85\u8FC7 32768 \u5B57\u7B26",
        URL_FORMAT: "\u4E0B\u8F7D\u5730\u5740\u683C\u5F0F\u65E0\u6548",
        HTTP_CUSTOM_PORT: "HTTP \u4E0B\u8F7D\u5730\u5740\u4F7F\u7528\u4E86\u975E\u9ED8\u8BA4\u7AEF\u53E3",
        URL_PROTOCOL: "\u4E0B\u8F7D\u5730\u5740\u4E0D\u662F HTTP/HTTPS",
        URL_CREDENTIALS: "\u4E0B\u8F7D\u5730\u5740\u542B\u7528\u6237\u51ED\u636E",
        LOCAL_HOST: "\u4E0B\u8F7D\u5730\u5740\u6307\u5411\u672C\u673A",
        NON_PUBLIC_ADDRESS: "\u4E0B\u8F7D\u57DF\u540D\u89E3\u6790\u5230\u5185\u7F51\u6216\u4FDD\u7559 IP\uFF08\u53EF\u80FD\u53D7\u4EE3\u7406 DNS \u5F71\u54CD\uFF09",
        DNS_FAILED: "\u4E0B\u8F7D\u57DF\u540D DNS \u89E3\u6790\u5931\u8D25",
        DNS_EMPTY: "\u4E0B\u8F7D\u57DF\u540D\u6CA1\u6709\u53EF\u7528 DNS \u5730\u5740"
      };
      const reason = error.detail?.downloadUrlReason;
      return typeof reason === "string" && Object.hasOwn(reasons, reason) ? reasons[reason] + "\uFF08" + reason + "\uFF09" : "\u4E0B\u8F7D\u5730\u5740\u672A\u901A\u8FC7\u5B89\u5168\u6821\u9A8C\uFF08UNSAFE_DOWNLOAD_URL\uFF09";
    }
    if (/returned invalid JSON$/.test(error.message)) return "\u4E0B\u8F7D\u63A5\u53E3\u8FD4\u56DE\u683C\u5F0F\u65E0\u6548\uFF08INVALID_RESPONSE\uFF09";
    if (/request failed$/.test(error.message)) return "\u8BF7\u6C42\u4E0B\u8F7D\u63A5\u53E3\u7684\u7F51\u7EDC\u8FDE\u63A5\u5931\u8D25\uFF08API_NETWORK\uFF09";
    if (/attachment download failed$/.test(error.message)) return "\u6587\u4EF6\u4E0B\u8F7D\u7F51\u7EDC\u8FDE\u63A5\u5931\u8D25\uFF08FILE_NETWORK\uFF09";
  }
  return "\u9644\u4EF6\u4E0B\u8F7D\u5931\u8D25\uFF08DOWNLOAD_FAILED\uFF09";
}
function textOf(message) {
  if (message.msgtype === "text") {
    return typeof message.text?.content === "string" ? message.text.content : "";
  }
  const content = contentOf(message);
  if (message.msgtype === "audio") return typeof content.recognition === "string" ? content.recognition : "";
  if (message.msgtype !== "richText") return "";
  const richText = content.richText;
  if (!Array.isArray(richText)) return "";
  return richText.map(
    (part) => typeof part === "object" && part !== null && typeof part.text === "string" ? part.text ?? "" : ""
  ).join("");
}
function checkDiscriminants(message) {
  if (message.conversationType !== "1" && message.conversationType !== "2") {
    throw new TypeError("invalid DingTalk conversationType");
  }
  if (!MESSAGE_TYPES.has(message.msgtype)) throw new TypeError("invalid DingTalk msgtype");
}
function isSafeAttachmentUrl(value) {
  if (value.length > 4096) return false;
  if ([...value].some((character) => (character.codePointAt(0) ?? 0) <= 32)) return false;
  try {
    const url2 = new URL(value);
    return url2.protocol === "https:" && url2.username === "" && url2.password === "";
  } catch {
    return false;
  }
}
function defaultName(message) {
  if (message.msgtype === "picture") return `${message.msgId}.png`;
  if (message.msgtype === "audio") return `${message.msgId}.amr`;
  if (message.msgtype === "video") return `${message.msgId}.mp4`;
  return message.msgId;
}
function safeAttachmentName(value, fallback) {
  const candidate = typeof value === "string" ? value.replaceAll("\\", "/").split("/").at(-1) : void 0;
  const clean = (input) => [...input].filter((character) => {
    const code = character.codePointAt(0) ?? 0;
    return code > 31 && code !== 127;
  }).join("").trim();
  return [...clean(candidate ?? "") || clean(fallback) || "attachment"].slice(0, 256).join("");
}
function safeAttachmentMime(value, fallback = "application/octet-stream") {
  return typeof value === "string" && value.length <= 128 && /^[\w!#$&^_.+-]+\/[\w!#$&^_.+-]+$/.test(value) ? value : fallback;
}
function toEvent(message, botUserId, attachments) {
  checkDiscriminants(message);
  const messageId = requiredString(message.msgId, "msgId", 512);
  const conversationId = requiredString(message.conversationId, "conversationId", 256);
  const staffId = requiredString(message.senderStaffId, "senderStaffId", 256);
  const senderNick = optionalString(message.senderNick, "senderNick", 256);
  const senderCorpId = optionalString(message.senderCorpId, "senderCorpId", 1024);
  const unionId = optionalString(message.senderId, "senderId", 256);
  const robotCode = optionalString(message.robotCode, "robotCode", 128);
  if (!Number.isFinite(message.createAt)) throw new TypeError("invalid DingTalk createAt");
  const at = new Date(message.createAt).toISOString();
  const raw = {
    staffId,
    conversationId,
    conversationType: message.conversationType
  };
  if (unionId !== void 0) raw.unionId = unionId;
  if (senderNick !== void 0) raw.senderNick = senderNick;
  if (senderCorpId !== void 0) raw.senderCorpId = senderCorpId;
  return {
    kind: "message",
    eventId: messageId,
    messageId,
    accountId: robotCode ?? "dingtalk",
    at,
    chat: { id: conversationId, type: message.conversationType === "1" ? "dm" : "group" },
    sender: {
      userId: staffId,
      ...unionId === void 0 ? {} : { unionId },
      ...senderNick === void 0 ? {} : { displayName: senderNick },
      raw
    },
    text: textOf(message),
    attachments: attachments.map((attachment) => ({ ...attachment })),
    mentions: {
      bot: message.isInAtList === true || botUserId.length > 0 && (message.atUsers ?? []).some((candidate) => candidate.dingtalkId === botUserId),
      replyToBot: false,
      quoteBot: false
    }
  };
}
async function attachmentsOf(message, download, onFailure) {
  checkDiscriminants(message);
  const content = contentOf(message);
  const parts = message.msgtype === "richText" ? (Array.isArray(content.richText) ? content.richText : []).filter(
    (part) => part && typeof part === "object" && part.type === "picture"
  ) : ["picture", "file", "audio", "video"].includes(message.msgtype) ? [content] : [];
  const attachments = [];
  let remaining = MAX_ATTACHMENT_BYTES;
  for (const [index, part] of parts.slice(0, 50).entries()) {
    const name = safeAttachmentName(
      "fileName" in part ? part.fileName : void 0,
      message.msgtype === "richText" ? message.msgId + "-" + (index + 1) + ".png" : defaultName(message)
    );
    const code = part.downloadCode || ("pictureDownloadCode" in part ? part.pictureDownloadCode : void 0);
    try {
      if (typeof code !== "string" || !code.length) {
        onFailure?.(name, "\u6D88\u606F\u7F3A\u5C11\u9644\u4EF6\u4E0B\u8F7D\u7801\uFF08DOWNLOAD_CODE_MISSING\uFF09");
        continue;
      }
      if (remaining <= 0) throw new RangeError("DingTalk attachment exceeds the size limit");
      const result = await download(code, remaining);
      if ("url" in result) {
        if (!isSafeAttachmentUrl(result.url)) throw new TypeError("DingTalk attachment URL must be HTTPS");
        throw new RangeError("DingTalk attachment exceeds the size limit");
      }
      if (!(result.bytes instanceof Uint8Array) || result.bytes.length > remaining)
        throw new RangeError("DingTalk attachment exceeds the size limit");
      remaining -= result.bytes.length;
      const fallback = message.msgtype === "audio" ? "audio/amr" : message.msgtype === "video" ? "video/mp4" : "application/octet-stream";
      attachments.push({
        name,
        mime: safeAttachmentMime(result.mime?.split(";")[0]?.trim(), fallback),
        bytes: new Uint8Array(result.bytes),
        sizeBytes: result.bytes.length
      });
    } catch (error) {
      if (!onFailure) throw error;
      onFailure(name, attachmentFailureText(error));
    }
  }
  if (parts.length > 50) onFailure?.("\u8D85\u8FC7 50 \u4E2A\u7684\u9644\u4EF6", "\u5355\u6761\u6D88\u606F\u6700\u591A\u540C\u6B65 50 \u4E2A\u9644\u4EF6\uFF08ATTACHMENT_COUNT\uFF09");
  return attachments;
}

// src/adapters/dingtalk/index.ts
var MAX_TRACKED_CHATS = 1024;
var MAX_RECONNECT_QUEUE = 100;
var MAX_CALLBACK_VALUE_CHARS = 512;
var MAX_CALLBACK_ID_CHARS = 256;
var MAX_ACTION_ID_CHARS = 128;
var DingtalkAdapter = class {
  constructor(manifest, gateway, options) {
    this.manifest = manifest;
    this.gateway = gateway;
    this.options = options;
    const perSecond = options.rate?.perAccountPerSec ?? manifest.limits.rate?.perAccountPerSec ?? 20;
    if (!Number.isFinite(perSecond) || perSecond <= 0) {
      throw new ChannelError("E_CONFIG_INVALID", "DingTalk per-account rate must be greater than zero");
    }
    this.accountBucket = new TokenBucket({
      tokensPerSecond: perSecond,
      capacity: Math.max(1, perSecond)
    });
  }
  manifest;
  gateway;
  options;
  botUserId = "";
  accountId = "dingtalk";
  onEvent;
  log;
  connectionEpoch = 0;
  inboundLanes = /* @__PURE__ */ new Map();
  removeAbortListener;
  cancelReconnectDelay;
  connected = false;
  closed = true;
  reconnecting = false;
  reconnectRequested = false;
  everConnected = false;
  flushingOutbound = false;
  outboundQueue = [];
  buckets = /* @__PURE__ */ new Map();
  chatTargets = /* @__PURE__ */ new Map();
  cardChats = /* @__PURE__ */ new Map();
  chatTails = /* @__PURE__ */ new Map();
  accountBucket;
  outboundAbort = new AbortController();
  parkedOutbound = 0;
  async connect(options) {
    requireCredential(options.credentials, "clientId");
    requireCredential(options.credentials, "clientSecret");
    if (options.signal.aborted) {
      throw new ChannelError("E_CONNECT_FAILED", "connection signal is already aborted");
    }
    if (this.outboundAbort.signal.aborted) this.outboundAbort = new AbortController();
    const credentialGateway = this.gateway;
    credentialGateway.setCredentials?.({
      clientId: options.credentials.clientId,
      clientSecret: options.credentials.clientSecret,
      ...options.credentials.robotCode === void 0 ? {} : { robotCode: options.credentials.robotCode }
    });
    this.accountId = options.credentials.robotCode ?? "dingtalk";
    this.cardChats.clear();
    this.closed = false;
    this.connected = false;
    this.reconnectRequested = false;
    this.onEvent = options.onEvent;
    this.log = options.log;
    try {
      await this.startGateway(options);
    } catch (error) {
      await this.gateway.stop().catch(() => void 0);
      this.closed = true;
      this.connected = false;
      this.reconnectRequested = false;
      this.cancelReconnectDelay?.();
      this.cancelReconnectDelay = void 0;
      this.onEvent = void 0;
      this.log = void 0;
      this.botUserId = "";
      this.rejectOutboundQueue("connection failed before queued sends could be delivered");
      this.removeAbortListener?.();
      this.removeAbortListener = void 0;
      this.connectionEpoch++;
      this.outboundAbort.abort(new ChannelError("E_CONNECT_FAILED", "DingTalk adapter connection failed"));
      throw error;
    }
  }
  async disconnect() {
    this.closed = true;
    this.connected = false;
    this.reconnectRequested = false;
    this.cancelReconnectDelay?.();
    this.cancelReconnectDelay = void 0;
    this.removeAbortListener?.();
    this.removeAbortListener = void 0;
    this.connectionEpoch++;
    this.outboundAbort.abort(new ChannelError("E_CONNECT_FAILED", "DingTalk adapter disconnected"));
    try {
      await this.gateway.stop();
    } finally {
      this.rejectOutboundQueue("adapter disconnected before queued sends could be delivered");
      this.onEvent = void 0;
      this.log = void 0;
      this.botUserId = "";
    }
  }
  capabilities() {
    return {
      ...this.manifest.capabilities,
      ...!this.options.cardTemplateId?.trim() ? { card: false, edit: false } : {}
    };
  }
  handleRaw(message, epoch = this.connectionEpoch) {
    const conversationId = typeof message.conversationId === "string" ? message.conversationId : "";
    const lane = `${epoch}:${conversationId}`;
    const previous = this.inboundLanes.get(lane) ?? Promise.resolve();
    const current = previous.catch(() => void 0).then(() => this.deliverRaw(message, epoch));
    this.inboundLanes.set(lane, current);
    void current.finally(() => {
      if (this.inboundLanes.get(lane) === current) this.inboundLanes.delete(lane);
    }).catch(() => void 0);
  }
  toSession(event, context) {
    if (event.kind !== "message") return null;
    const text = (event.mentions.bot ? event.text.replace(/^@\S+\s*/, "") : event.text).trim();
    const commandMatch = /^\/(help|status|new|cancel|preset)(?:\s+(.*))?$/.exec(text);
    const content = text.length > 0 ? [{ type: "text", text }] : [];
    for (const attachment of event.attachments) {
      const resource = attachmentContent(attachment);
      if (resource !== void 0) content.push(resource);
    }
    if (content.length === 0) return null;
    return {
      sessionKey: (0, adapter_exports.sessionKeyFor)({
        tenant: context.tenant,
        agent: context.agent,
        channel: this.manifest.id,
        chat: event.chat
      }),
      credential: this.credentialOf(event),
      eventId: event.eventId,
      messageId: event.messageId,
      content,
      kind: commandMatch === null ? "message" : "command",
      ...commandMatch === null ? {} : {
        command: {
          name: commandMatch[1],
          args: (commandMatch[2] ?? "").split(/\s+/).filter(Boolean)
        }
      }
    };
  }
  resolveScope(event) {
    return { scope: event.chat.id, isDirectMessage: event.chat.type === "dm" };
  }
  credentialOf(event) {
    const credential = {
      kind: "channel",
      channel: this.manifest.id,
      accountId: event.accountId,
      userId: event.sender.userId,
      chatId: event.chat.id,
      chatType: event.chat.type,
      ...event.sender.unionId === void 0 ? {} : { unionId: event.sender.unionId },
      ...event.sender.displayName === void 0 ? {} : { displayName: event.sender.displayName },
      raw: event.sender.raw
    };
    return (0, adapter_exports.whitelistCredential)(credential, this.manifest.credentials.exposes).cred;
  }
  render(_node, _context) {
    return null;
  }
  async send(target, message) {
    return this.scheduleOutbound(
      target.chatId,
      () => this.whenConnected(target.chatId, () => this.sendNow(target, message))
    );
  }
  async update(reference, message) {
    return this.scheduleOutbound(
      reference.chatId,
      () => this.whenConnected(reference.chatId, () => this.updateNow(reference, message))
    );
  }
  async sendNow(_target, _message) {
    const target = this.dingtalkTarget(_target);
    if (_message.ephemeral === true || !this.capabilities().card) {
      const { processQueryKey } = await this.gateway.sendMarkdown(
        target,
        "Agnes Harness",
        markdownForMessage(_message, this.capabilities())
      );
      return { chatId: _target.chatId, messageId: processQueryKey };
    }
    const cardBizId = cardBizIdFor(_target.chatId, _target.deliveryKey ?? randomUUID());
    const { cardData } = buildCard(_message.blocks, { cardBizId }, this.manifest.limits.cardBytes);
    await this.gateway.createCard(cardBizId, cardData, target);
    this.rememberCard(cardBizId, _target.chatId, target.conversationType);
    return { chatId: _target.chatId, messageId: cardBizId, cardBizId };
  }
  async updateNow(_reference, _message) {
    if (_reference.cardBizId === void 0) {
      throw new ChannelError("E_NOT_IMPLEMENTED", "DingTalk markdown messages cannot be updated");
    }
    const { cardData } = buildCard(
      _message.blocks,
      { cardBizId: _reference.cardBizId },
      this.manifest.limits.cardBytes
    );
    await this.gateway.updateCard(_reference.cardBizId, cardData);
  }
  renderApproval(request, decisionKey) {
    return {
      blocks: [
        {
          kind: "approval",
          title: "\u9700\u8981\u5BA1\u6279",
          summary: request.summary,
          risk: request.risk,
          options: request.options,
          ...(decisionKey ?? request.decisionKey) === void 0 ? {} : { decisionKey: decisionKey ?? request.decisionKey },
          ...request.requestSeq === void 0 ? {} : { requestSeq: request.requestSeq },
          ...request.expiresAt === void 0 ? {} : { expiresAt: request.expiresAt }
        }
      ]
    };
  }
  handleCard(callback, epoch = this.connectionEpoch) {
    if (this.closed || epoch !== this.connectionEpoch) return;
    try {
      const raw = callback;
      const cardBizId = boundedString(raw.outTrackId, MAX_CALLBACK_ID_CHARS);
      const userId = boundedString(raw.userId, MAX_CALLBACK_ID_CHARS);
      const privateData = raw.cardPrivateData;
      if (cardBizId === null || userId === null || !isRecord(privateData)) return;
      const actionIds = privateData.actionIds;
      if (!Array.isArray(actionIds) || actionIds.length !== 1) return;
      const value = boundedString(actionIds[0], MAX_CALLBACK_VALUE_CHARS);
      if (value === null) return;
      const remembered = this.cardChats.get(cardBizId);
      const suppliedChat = optionalBoundedString(raw.conversationId, MAX_CALLBACK_ID_CHARS);
      if (suppliedChat === false || remembered !== void 0 && suppliedChat && suppliedChat !== remembered.chatId)
        return;
      const chatId = suppliedChat || remembered?.chatId;
      if (chatId === void 0) return;
      const suppliedType = raw.conversationType;
      if (suppliedType !== void 0 && suppliedType !== "1" && suppliedType !== "2") return;
      if (remembered !== void 0 && suppliedType !== void 0 && suppliedType !== remembered.conversationType)
        return;
      const conversationType = suppliedType ?? remembered?.conversationType ?? "2";
      const chatType = conversationType === "1" ? "dm" : "group";
      const eventId = createHash2("sha256").update(JSON.stringify([cardBizId, value, userId, chatId, conversationType])).digest("hex");
      this.onEvent?.({
        kind: "cardAction",
        eventId: `dingtalk-card:${eventId}`,
        accountId: this.accountId,
        at: (/* @__PURE__ */ new Date()).toISOString(),
        chat: { id: chatId, type: chatType },
        sender: {
          userId,
          raw: { staffId: userId, conversationId: chatId, conversationType }
        },
        cardBizId,
        value
      });
    } catch {
      this.safeWarn("Dingtalk card callback was invalid and has been dropped");
    }
  }
  onApprovalAction(event) {
    if (event.kind !== "cardAction" || event.value.length > MAX_CALLBACK_VALUE_CHARS) return null;
    const match = /^appr:([^:\s]+):(allow_once|allow_always|reject_once|reject_always)$/.exec(event.value);
    if (match === null) return null;
    const key = match[1];
    if (key.length === 0 || key.length > MAX_ACTION_ID_CHARS) return null;
    const option = match[2];
    const verdict = fromAcpOptionKind(option);
    if (verdict === "cancelled" || verdict === "unavailable") return null;
    if (key.startsWith("#")) {
      const requestSeq = Number(key.slice(1));
      if (!Number.isSafeInteger(requestSeq) || requestSeq < 1) return null;
      return { requestSeq, verdict, approverCredential: this.credentialOf(event) };
    }
    return { decisionKey: key, verdict, approverCredential: this.credentialOf(event) };
  }
  onSlotAction(event) {
    if (event.kind !== "cardAction" || event.value.length > MAX_CALLBACK_VALUE_CHARS) return null;
    const match = /^slot:(\d+):(.+)$/.exec(event.value);
    if (match === null) return null;
    const requestSeq = Number(match[1]);
    const actionId = match[2];
    if (!Number.isSafeInteger(requestSeq) || requestSeq < 1 || actionId.length > MAX_ACTION_ID_CHARS)
      return null;
    return { requestSeq, actionId, credential: this.credentialOf(event) };
  }
  handleDisconnect(_error, options, epoch = this.connectionEpoch) {
    if (this.closed || options.signal.aborted || epoch !== this.connectionEpoch) return;
    this.connected = false;
    this.connectionEpoch++;
    this.removeAbortListener?.();
    this.removeAbortListener = void 0;
    this.safeWarn("Dingtalk stream disconnected; reconnecting");
    this.reconnectRequested = true;
    this.startReconnectDriver(options);
  }
  async startGateway(options) {
    if (this.closed || options.signal.aborted) {
      throw new ChannelError("E_CONNECT_FAILED", "connection was aborted during startup");
    }
    const epoch = ++this.connectionEpoch;
    this.removeAbortListener?.();
    const onAbort = () => {
      if (this.connectionEpoch !== epoch) return;
      this.closed = true;
      this.connected = false;
      this.reconnectRequested = false;
      this.connectionEpoch++;
      this.outboundAbort.abort(new ChannelError("E_CONNECT_FAILED", "DingTalk adapter connection aborted"));
      this.cancelReconnectDelay?.();
      this.cancelReconnectDelay = void 0;
      this.onEvent = void 0;
      this.log = void 0;
      this.botUserId = "";
      this.rejectOutboundQueue("connection aborted before queued sends could be delivered");
    };
    const removeAbortListener = () => options.signal.removeEventListener("abort", onAbort);
    this.removeAbortListener = removeAbortListener;
    options.signal.addEventListener("abort", onAbort, { once: true });
    try {
      const { botUserId } = await this.gateway.start(
        {
          onMessage: (message) => this.handleRaw(message, epoch),
          onCard: (callback) => this.handleCard(callback, epoch),
          onDisconnect: (error) => this.handleDisconnect(error, options, epoch)
        },
        options.signal
      );
      if (epoch !== this.connectionEpoch || options.signal.aborted || this.closed) {
        throw new ChannelError("E_CONNECT_FAILED", "connection was aborted during startup");
      }
      this.botUserId = botUserId;
      this.connected = true;
      this.everConnected = true;
      this.startOutboundFlush();
      return epoch;
    } catch (error) {
      if (this.removeAbortListener === removeAbortListener) {
        removeAbortListener();
        this.removeAbortListener = void 0;
      }
      if (this.connectionEpoch === epoch) this.connectionEpoch++;
      throw error;
    }
  }
  startReconnectDriver(options) {
    if (this.reconnecting) return;
    this.reconnecting = true;
    void this.runReconnectDriver(options).catch(() => void 0).finally(() => {
      this.reconnecting = false;
      if (this.reconnectRequested && !this.closed && !options.signal.aborted) {
        this.startReconnectDriver(options);
      }
    });
  }
  async runReconnectDriver(options) {
    while (this.reconnectRequested && !this.closed && !options.signal.aborted) {
      this.reconnectRequested = false;
      await this.reconnect(options);
    }
  }
  async reconnect(options) {
    const delays = backoffDelays(this.options.backoff);
    for (let attempt = 1; !this.closed && !options.signal.aborted; attempt++) {
      try {
        const epoch = await this.startGateway(options);
        if (epoch !== this.connectionEpoch || !this.connected) continue;
        this.safeInfo("Dingtalk stream reconnected", { attempt });
        return;
      } catch {
        await this.gateway.stop().catch(() => void 0);
        if (this.closed || options.signal.aborted) return;
        this.safeWarn("Dingtalk reconnect attempt failed", { attempt });
        await this.waitForReconnect(delays.next().value, options.signal);
      }
    }
  }
  waitForReconnect(delayMs, signal) {
    if (this.closed || signal.aborted) return Promise.resolve();
    return new Promise((resolve) => {
      let timer;
      const finish = () => {
        if (timer !== void 0) clearTimeout(timer);
        signal.removeEventListener("abort", finish);
        if (this.cancelReconnectDelay === finish) this.cancelReconnectDelay = void 0;
        resolve();
      };
      timer = setTimeout(finish, delayMs);
      this.cancelReconnectDelay = finish;
      signal.addEventListener("abort", finish, { once: true });
    });
  }
  bucket(chatId) {
    let bucket = this.buckets.get(chatId);
    if (bucket !== void 0) {
      this.buckets.delete(chatId);
      this.buckets.set(chatId, bucket);
      return bucket;
    }
    this.trimChatState(this.buckets, 1);
    const perMinute = this.options.rate?.perChatPerMin ?? this.manifest.limits.rate?.perChatPerMin ?? 20;
    if (!Number.isFinite(perMinute) || perMinute <= 0) {
      throw new ChannelError("E_CONFIG_INVALID", "DingTalk per-chat rate must be greater than zero");
    }
    bucket = new TokenBucket({
      tokensPerSecond: perMinute / 60,
      capacity: Math.max(1, Math.min(perMinute, 5))
    });
    this.buckets.set(chatId, bucket);
    return bucket;
  }
  async dispatchOutbound(chatId, operation) {
    const signal = this.outboundAbort.signal;
    await this.bucket(chatId).take(signal);
    await this.accountBucket.take(signal);
    signal.throwIfAborted();
    if (this.everConnected && !this.connected) {
      throw new ChannelError("E_CONNECT_FAILED", "DingTalk adapter is not connected");
    }
    return operation();
  }
  scheduleOutbound(chatId, operation) {
    if (!this.chatTails.has(chatId) && this.chatTails.size >= MAX_TRACKED_CHATS) {
      return Promise.reject(new ChannelError("E_CONNECT_FAILED", "DingTalk active chat capacity reached"));
    }
    const parked = !this.connected && !this.closed;
    if (parked && this.parkedOutbound >= MAX_RECONNECT_QUEUE) {
      return Promise.reject(
        new ChannelError("E_CONNECT_FAILED", "send queue full while DingTalk is reconnecting")
      );
    }
    if (parked) this.parkedOutbound++;
    const previous = this.chatTails.get(chatId) ?? Promise.resolve();
    const current = previous.catch(() => void 0).then(operation);
    this.chatTails.set(chatId, current);
    void current.finally(() => {
      if (parked) this.parkedOutbound--;
      if (this.chatTails.get(chatId) === current) this.chatTails.delete(chatId);
    }).catch(() => void 0);
    return current;
  }
  whenConnected(chatId, operation) {
    if (this.connected && !this.flushingOutbound) {
      const epoch = this.connectionEpoch;
      return this.dispatchOutbound(chatId, operation).catch((error) => {
        if (!this.closed && this.connectionEpoch !== epoch) {
          return this.queueOutbound(chatId, operation);
        }
        throw error;
      });
    }
    if (this.closed) {
      if (!this.everConnected) return this.dispatchOutbound(chatId, operation);
      return Promise.reject(new ChannelError("E_CONNECT_FAILED", "DingTalk adapter is disconnected"));
    }
    return this.queueOutbound(chatId, operation);
  }
  queueOutbound(chatId, operation) {
    if (this.outboundQueue.length >= 100) {
      return Promise.reject(
        new ChannelError("E_CONNECT_FAILED", "send queue full while DingTalk is reconnecting")
      );
    }
    return new Promise((resolve, reject) => {
      const queued = {
        run: async () => {
          const epoch = this.connectionEpoch;
          try {
            resolve(await this.dispatchOutbound(chatId, operation));
          } catch (error) {
            if (!this.closed && this.connectionEpoch !== epoch) {
              this.outboundQueue.unshift(queued);
              return;
            }
            reject(error);
          }
        },
        reject
      };
      this.outboundQueue.push(queued);
      if (this.connected) this.startOutboundFlush();
    });
  }
  startOutboundFlush() {
    if (this.flushingOutbound) return;
    this.flushingOutbound = true;
    void (async () => {
      while (this.connected) {
        const queued = this.outboundQueue.shift();
        if (queued === void 0) break;
        await queued.run();
      }
    })().finally(() => {
      this.flushingOutbound = false;
      if (this.connected && this.outboundQueue.length > 0) this.startOutboundFlush();
    });
  }
  rejectOutboundQueue(message) {
    const error = new ChannelError("E_CONNECT_FAILED", message);
    for (const queued of this.outboundQueue.splice(0)) queued.reject(error);
  }
  async deliverRaw(message, epoch) {
    let attachments = [];
    const failed = [];
    try {
      attachments = await attachmentsOf(
        message,
        (downloadCode, maxBytes) => this.gateway.download(downloadCode, maxBytes),
        (name, reason) => failed.push(JSON.stringify(name) + "\uFF1A" + (reason ?? "\u9644\u4EF6\u4E0B\u8F7D\u5931\u8D25\uFF08DOWNLOAD_FAILED\uFF09"))
      );
    } catch (error) {
      failed.push(attachmentFailureText(error));
      this.safeWarn("Dingtalk attachment download failed; continuing without attachment");
    }
    try {
      const event = toEvent(message, this.botUserId, attachments);
      if (failed.length) {
        this.safeWarn("Dingtalk attachment import incomplete");
        event.text += "\n[\u9489\u9489\u9644\u4EF6\u672A\u80FD\u540C\u6B65\uFF1A" + failed.join("\uFF1B") + "\u3002\u8BF7\u6839\u636E\u9519\u8BEF\u539F\u56E0\u68C0\u67E5\u673A\u5668\u4EBA\u6743\u9650\u6216\u91CD\u65B0\u53D1\u9001\u3002]";
      }
      if (epoch === this.connectionEpoch) {
        this.rememberTarget(message);
        this.onEvent?.(event);
      }
    } catch {
      this.safeWarn("Dingtalk inbound message was invalid and has been dropped");
    }
  }
  dingtalkTarget(target) {
    const remembered = this.chatTargets.get(target.chatId);
    if (remembered !== void 0) {
      this.chatTargets.delete(target.chatId);
      this.chatTargets.set(target.chatId, remembered);
    }
    const conversationType = remembered?.conversationType ?? "2";
    return {
      conversationId: target.chatId,
      conversationType,
      ...conversationType === "1" && remembered?.userId !== void 0 ? { userIds: [remembered.userId] } : {}
    };
  }
  rememberTarget(message) {
    this.chatTargets.delete(message.conversationId);
    this.chatTargets.set(message.conversationId, {
      conversationType: message.conversationType,
      ...message.conversationType === "1" ? { userId: message.senderStaffId } : {}
    });
    this.trimChatState(this.chatTargets);
  }
  rememberCard(cardBizId, chatId, conversationType) {
    this.cardChats.delete(cardBizId);
    this.cardChats.set(cardBizId, { chatId, conversationType });
    while (this.cardChats.size > MAX_TRACKED_CHATS) {
      const oldest = this.cardChats.keys().next().value;
      if (oldest === void 0) break;
      this.cardChats.delete(oldest);
    }
  }
  trimChatState(map, reserve = 0) {
    while (map.size + reserve > MAX_TRACKED_CHATS) {
      const candidate = [...map.keys()].find((key) => !this.chatTails.has(key));
      if (candidate === void 0) {
        throw new ChannelError("E_CONNECT_FAILED", "DingTalk active chat capacity reached");
      }
      map.delete(candidate);
    }
  }
  safeInfo(message, meta) {
    try {
      this.log?.info(message, meta);
    } catch {
    }
  }
  safeWarn(message, meta) {
    try {
      this.log?.warn(message, meta);
    } catch {
    }
  }
};
async function createDingtalkAdapter(manifestPath, options = {}) {
  const manifest = await loadManifest(manifestPath);
  const gateway = options.gateway ?? (await Promise.resolve().then(() => (init_gateway_real(), gateway_real_exports))).createRealGateway({
    ...options.cardTemplateId === void 0 ? {} : { cardTemplateId: options.cardTemplateId }
  });
  return new DingtalkAdapter(manifest, gateway, options);
}
function requireCredential(credentials, key) {
  if (typeof credentials[key] !== "string" || credentials[key]?.length === 0) {
    throw new ChannelError("E_CONNECT_FAILED", `missing ${key}`, { key });
  }
}
function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function boundedString(value, maxChars) {
  return typeof value === "string" && value.trim().length > 0 && value.length <= maxChars ? value : null;
}
function optionalBoundedString(value, maxChars) {
  if (value === void 0) return void 0;
  return boundedString(value, maxChars) ?? false;
}
function attachmentContent(attachment) {
  if (!(attachment.bytes instanceof Uint8Array) || attachment.bytes.length > MAX_ATTACHMENT_BYTES)
    return void 0;
  const bytes = attachment.bytes;
  const data = Buffer.from(bytes).toString("base64");
  const imageMime = bytes.length >= 8 && [137, 80, 78, 71, 13, 10, 26, 10].every((byte, index) => bytes[index] === byte) ? "image/png" : bytes[0] === 255 && bytes[1] === 216 && bytes[2] === 255 ? "image/jpeg" : void 0;
  if (imageMime) return { type: "image", data, mimeType: imageMime };
  return {
    type: "file",
    data,
    name: safeAttachmentName(attachment.name, "attachment"),
    mimeType: safeAttachmentMime(attachment.mime)
  };
}
function markdownForMessage(message, capabilities) {
  return degrade(message, { ...capabilities, card: false, attachment: false }).msg.blocks.map((block) => {
    switch (block.kind) {
      case "text":
        return block.markdown;
      case "link":
        return `[${block.title}](${block.url})`;
      case "file":
        return block.url === void 0 ? `${block.name}\uFF08\u65E0\u53EF\u53D1\u9001\u94FE\u63A5\uFF09` : `[${block.name}](${block.url})`;
      case "table":
      case "card":
      case "approval":
        return "";
    }
    return "";
  }).filter((part) => part.length > 0).join("\n\n");
}

// src/index.ts
var dingtalkProvider = {
  id: "dingtalk",
  version: "0.3.8",
  manifest: channel_default,
  configSchema: { type: "object", additionalProperties: false, properties: {
    cardTemplateId: { type: "string", title: "\u4E92\u52A8\u5361\u7247\u6A21\u677F ID\uFF08\u7559\u7A7A\u4F7F\u7528\u666E\u901A\u6D88\u606F\uFF09 / Card template ID (blank for plain messages)" }
  } },
  async create({ config }) {
    const options = config ?? {};
    if (typeof options !== "object" || Array.isArray(options) || Object.keys(options).some((key) => key !== "cardTemplateId") || options.cardTemplateId !== void 0 && typeof options.cardTemplateId !== "string")
      throw new Error("Invalid DingTalk instance config");
    const cardTemplateId = typeof options.cardTemplateId === "string" ? options.cardTemplateId.trim() : "";
    return createDingtalkAdapter(fileURLToPath(new URL("./adapters/dingtalk/channel.json", import.meta.url)), {
      ...cardTemplateId ? { cardTemplateId } : {}
    });
  }
};
var main = {
  inject: ["profileRuntime", "services", "providers"],
  apply(ctx, config = {}) {
    if (!ctx.profileRuntime.registerChannel) throw new Error("DingTalk requires AGH Extension API 5.8 or later");
    ctx.profileRuntime.registerChannel(dingtalkProvider, { instances: config.instances ?? [] });
  }
};
export {
  dingtalkProvider,
  main
};
/*! Bundled license information:

mime-db/index.js:
  (*!
   * mime-db
   * Copyright(c) 2014 Jonathan Ong
   * Copyright(c) 2015-2022 Douglas Christopher Wilson
   * MIT Licensed
   *)

mime-types/index.js:
  (*!
   * mime-types
   * Copyright(c) 2014 Jonathan Ong
   * Copyright(c) 2015 Douglas Christopher Wilson
   * MIT Licensed
   *)
*/
