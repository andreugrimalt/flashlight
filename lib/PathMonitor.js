var fbutil = require('./fbutil');
var DynamicPathMonitor = require('./DynamicPathMonitor');
var traverse = require('traverse');
var deepAssign = require('deep-assign');
var unflatten = require('flat').unflatten;

function IndexProcessor(esc, pageSize, frequency, maxIterations) {
  this.queue = [];
  this.pageSize = pageSize;
  this.frequency = frequency;
  this.maxIterations = maxIterations;
  this.esc = esc;
  this._init();
}

IndexProcessor.prototype = {
  _init: function() {
    console.log('using bulk insert mode');
    setInterval(this._bulkInsert.bind(this), this.frequency);
    this.items = [];
    this.iterations = 0;
    this.inserting = false;
  },

  _bufferData: function* (length) {
    if (this.queue.length > 0 && ! this.inserting) {
      this.inserting = true;
      for (let item in this.queue) {
        let item = this.queue.shift();
        let config = deepAssign(this._generatedConfig(item.data), item.toArray || {});
        item.data = this._flatten(item.data, config);

        this.items.push({
          index: {
            _index: item.index,
            _type: item.type,
            _id: item.key
          }
        });

        this.items.push(item.data);
        if(this.items.length / 2 >= this.pageSize) { break; }
      }
      if (this.items.length / 2 >= length ||
          this.iterations > this.maxIterations && this.items.length / 2 > 0) {

        yield this.items;
        this.items = [];
        this.iterations = 0;
      }
      this.iterations++;
      this.inserting = false;
    }
  },

  _bulkInsert: function() {
    const that = this;  
    for(let data of this._bufferData(this.pageSize)) {
      this.esc.bulk({
        body: data
      }, function (error, response) {
        if (error) {
            console.log(error.red);
        } else {
            console.log(`bulk ${that.pageSize}`);
        }
      }.bind(this));
    };
  },

  _isDictionary: function (obj) {
    if(!obj) return false;
    if(Array.isArray(obj)) return false;
    if(obj.constructor != Object) return false;
    return true;
  },

  _generatedConfig: function(obj, config) {
    configs = [];
    traverse(obj).forEach(function (x) {
      if (this.key !== undefined && this.key.match(/^[0-9A-Za-z\-_]{20}$/)) {
        this.path.pop();
        configs.push(
          unflatten({[this.path.join('.')]: true},
                    {object: true}));
      }
    });
    if (configs.length > 0) { return deepAssign(...configs); }
    return [];
  },

  _convert: function(data) {
    if (data !== undefined) {
      // we need to replace the current keys contents
      let keys = Object.keys(data);
      if (keys.every(k => { return data[k] === true; })) {
        data = keys.map(k => { return {id: k}; });
      }
      if (keys.every(k => { return this._isDictionary(data[k]); })) {
        data = keys.map(k => { return deepAssign(data[k], {id: k})})
      }
    }
    return data;
  },

  _flatten: function(data, configKeys) {
    if (data !== undefined) {
      let keys = Object.keys(data);
      for (let k of Object.keys(configKeys)) {
        // we've found a key to convert to a ES structure
        if (configKeys[k] === true) {
          data[k] = this._convert(data[k]);
        }
        // we need to traverse down into the config to run data transformations
        if (this._isDictionary(configKeys[k])) {
          this._flatten(data[k], configKeys[k]);
        }
      }
    }
    return data;
  },
}

function PathMonitor(esc, path, processor) {
   this.ref = fbutil.fbRef(path.path);
   console.log('Indexing %s/%s using path "%s"'.grey, path.index, path.type, fbutil.pathName(this.ref));
   this.esc = esc;
   this.seconds = 0;

   this.processor = processor;
   this.index = path.index;
   this.type  = path.type;
   this.filter = path.filter || function() { return true; };
   this.parse  = path.parser || function(data) { return parseKeys(data, path.fields, path.omit) };
   this.toArray = path.toArray || {};

   this._init();
}

PathMonitor.prototype = {
  _init: function() {
    this.addMonitor = this.ref.on('child_added', this._process.bind(this, this._childAdded));
    this.changeMonitor = this.ref.on('child_changed', this._process.bind(this, this._childChanged));
    this.removeMonitor = this.ref.on('child_removed', this._process.bind(this, this._childRemoved));
  },

  _index: function (key, data, callback) {
    this.esc.index({
      index: this.index,
      type: this.type,
      id: key,
      body: data
    }, function (error, response) {
      if (callback) {
        callback(error, response);
      }
    }.bind(this));
  },

  _stop: function() {
    this.ref.off('child_added', this.addMonitor);
    this.ref.off('child_changed', this.changeMonitor);
    this.ref.off('child_removed', this.removeMonitor);
  },

  _process: function(fn, snap) {
    var dat = snap.val();
    if( this.filter(dat) ) {
      fn.call(this, snap.key, this.parse(dat));
    }
  },

  _firebaseArrayToArrayOfObjects: function(data, type, field) {
    if (data[field]) {
      const elements = Object.keys(data[field]).map(elementKey => {
        const element = data[field][elementKey];
        element.id = elementKey;
        return element;
      });
      data[field] = elements;
      return data;
    } else {
      return data;
    }
  },

  // TODO: Quite a hack!
  _replaceStringInData: function(data, target, replacement) {
      return JSON.parse(JSON.stringify(data).replace(target, replacement));
  },

  _childAdded: function(key, data) {
    var name = nameFor(this, key);
    data = this._replaceStringInData(data, 'lng', 'lon');
    this.processor.queue.push({
      name: name,
      data: data,
      key: key,
      type: this.type,
      index: this.index,
      toArray: this.toArray
    });
  },

  _childChanged: function(key, data) {
    var name = nameFor(this, key);
    this._index(key, data, function (error, response) {
      if (error) {
        console.error('failed to update %s: %s'.red, name, error);
      } else {
        console.log('updated'.green, name);
      }
    }.bind(this));
  },

  _childRemoved: function(key, data) {
    var name = nameFor(this, key);
    this.esc.delete({
      index: this.index,
      type: this.type,
      id: key
    }, function(error, data) {
      if( error ) {
        console.error('failed to delete %s: %s'.red, name, error);
      } else {
        console.log('deleted'.cyan, name);
      }
    }.bind(this));
  }
};

function nameFor(path, key) {
  return path.index + '/' + path.type + '/' + key;
}

function parseKeys(data, fields, omit) {
  if (!data || typeof(data)!=='object') {
    return data;
  }
  var out = data;
  // restrict to specified fields list
  if( Array.isArray(fields) && fields.length) {
    out = {};
    fields.forEach(function(f) {
      if( data.hasOwnProperty(f) ) {
        out[f] = data[f];
      }
    })
  }
  // remove omitted fields
  if( Array.isArray(omit) && omit.length) {
    omit.forEach(function(f) {
      if( out.hasOwnProperty(f) ) {
        delete out[f];
      }
    })
  }
  return out;
}

exports.process = function(esc, paths, dynamicPathUrl) {
  const processor = new IndexProcessor(esc, 1000, 100, 20);
  paths && paths.forEach(function(pathProps) {
    new PathMonitor(esc, pathProps, processor);
  });
  if (dynamicPathUrl) {
    new DynamicPathMonitor(fbutil.fbRef(dynamicPathUrl), function(pathProps) {
      return new PathMonitor(esc, pathProps, processor);
    });
  }
};
