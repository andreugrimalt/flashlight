var traverse = require('traverse');
var deepAssign = require('deep-assign');
var unflatten = require('flat').unflatten;
var deepEqual = require('deep-equal');
var deepAssign = require('deep-assign');

function BulkIndexer(esc, config) {
  this.esc = esc;
  this.config = config;
  this.count = 0;
  this._addMapping();
  this._init();
}

BulkIndexer.prototype = {
  _init: function() {
    console.log('using bulk insert mode');
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

  _addMapping: function() {
    if (this.config.mapping !== undefined) {
      this.esc.indices.exists({
        index: this.index
      }).then((response) => {
        if (!response) {
          this._createIndex(this.index);
        }
      });
    }
  },

  _createIndex: function(name) {
    this.esc.indices.create({
      index: name,
      body: this.config.mapping 
    }, (error) => {
      if (error) {
        console.log('failed to create index: %s'.green, this.index);
      } else {
        console.log('creating index: %s'.green, this.index);
      }
    });
  },


  insert: function(data) {
    this.esc.bulk({
      body: this.transform(data)
    }, function (error, response) {
      if (error) {
        console.log(error.red);
      } else if(response.errors) {
        console.log(JSON.stringify(response).red);
      } else {
        this.count += data.length;
        console.log(`bulk inserted ${this.count} items`);
      }
    }.bind(this));
  },

  transform: function(data) {
    const transformers = {
      'add': (item) => {
        let config = deepAssign(this._generatedConfig(item.data), this.config.toArray || {});
        item.data = this._flatten(item.data, config);
        item.data = this._replaceStringInData(item.data, 'lng');
        return [{
          index: {
            _index: this.config.index,
            _type: this.config.type,
            _id: item.key
          }
        }, item.data];
      },
      'delete': (item) => {
        return[{
          delete: {
            _index: item.index,
            _type: item.type,
            _id: item.key
          }
        }];
      },
      'update': (item) => {
        let config = deepAssign(this._generatedConfig(item.data), item.toArray || {});
        item.data = this._flatten(item.data, config);
        return [{
          update: {
            _index: item.index,
            _type: item.type,
            _id: item.key
          }
        }, {doc: item.data}];
      },
    };
    let transformed = [];
    for (let item of data) {
      transformed = transformed.concat(transformers[item.action](item));
    }
    return transformed;
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

module.exports = BulkIndexer;
