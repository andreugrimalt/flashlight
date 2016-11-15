var fbutil = require('./fbutil');
var DynamicPathMonitor = require('./DynamicPathMonitor');
var BulkIndexer = require('./BulkIndexer');
var Pager = require('./Pager');
var traverse = require('traverse');
var deepAssign = require('deep-assign');
var unflatten = require('flat').unflatten;

function PathMonitor(esc, config, indexer) {
  this.esc = esc;
  this.config = config;
  this.ref = fbutil.fbRef(this.config.path);
  this.indexer = indexer;
  console.log('Indexing %s/%s using path "%s"'.grey, this.config.index, this.config.type, fbutil.pathName(this.ref));

  this.pager = new Pager(this.ref, 1000, (e) => {
    this.indexer.insert(Object.keys(e.value).map((k) => {
      return {
        action: 'add',
        key: k,
        data: e.value[k]
      }
    }));
  }, (done) => {
    console.log('adding listeners starting from key %s', this.pager.lastKey);
    this._addListeners();
  });

  this.filter = this.config.filter || function() { return true; };
  this.parse  = this.config.parser || function(data) { return parseKeys(data, this.config.fields, this.config.omit) };
}

PathMonitor.prototype = {
  _addListeners: function() {
    this.addMonitor = this.ref.orderByKey().startAt(this.pager.lastKey).on('child_added', this._process.bind(this, this._childAdded));
    this.changeMonitor = this.ref.orderByKey().startAt(this.pager.lastKey).on('child_changed', this._process.bind(this, this._childChanged));

    this.removeMonitor = this.ref.orderByKey().startAt(this.pager.lastKey).on('child_removed', this._process.bind(this, this._childRemoved));
  },

  _index: function (key, data, callback) {
    this.esc.index({
      index: this.config.index,
      type: this.config.type,
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

  // TODO: Quite a hack!
  _replaceStringInData: function(data, target) {
     return traverse(data).forEach(function (x) {
        if (this.key === target) {
            this.delete();
            this.parent.update(
                deepAssign({lon: this.node}, this.parent.node)
            );
        }
      });
  },

  _childAdded: function(key, data) {
    var name = nameFor(this, key);
    this.indexer.insert([{
        action: 'add',
        key: key,
        data: data
      }]);
  },

  _childChanged: function(key, data) {
    var name = nameFor(this, key);
    this.indexer.insert([{
        action: 'update',
        key: key,
        data: data
      }]);
  },

  _childRemoved: function(key, data) {
    var name = nameFor(this, key);
    this.indexer.insert([{
        action: 'delete',
        key: key,
        data: data
      }]);
  }
};

function nameFor(path, key) {
  return path.config.index + '/' + path.config.type + '/' + key;
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
  new DynamicPathMonitor(fbutil.fbRef(dynamicPathUrl), function(pathProps) {
    return new PathMonitor(esc, pathProps, new BulkIndexer(esc, pathProps));
  });

};
