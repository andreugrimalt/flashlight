var EventEmitter = require('events');
class em extends EventEmitter {}

function Pager(ref, pageSize, pageCb, doneCb) {
  this.ref = ref;
  this.pageSize = pageSize;
  this.pageCb = pageCb;
  this.doneCb = doneCb;
  this.count = 0;
  this.done = false;
  this._init();
  this.em = new em();
  this.em.on('ready', () => {
    console.log('ready to page');
    this.page();
  });
  this.em.on('done', (e) => {
    this.doneCb(e);
  });
  this.em.on('page', (e) => {
    this.pageCb(e);
    if (e.cursor) {
      this.firstKey = e.cursor
      setImmediate(this.page.bind(this), 50);
    }
  });
}

Pager.prototype = {
  _init: function() {
    Promise.all([this._getFirstKey(this.ref), this._getLastKey(this.ref)]).then((results) => {
      this.firstKey = results[0];
      this.lastKey = results[1];
      this.em.emit('ready');
    }).catch((err) => {
      console.log(err);
    });
  },

  _getLastKey: function(ref) {
    return new Promise((resolve, reject) => {
      ref.orderByKey().limitToLast(1).on('value', (snap) => {
        resolve(Object.keys(snap.val())[0]);
      });
    });
  },

  _getFirstKey: function(ref) {
    return new Promise((resolve, reject) => {
      ref.orderByKey().limitToFirst(1).on('value', (snap) => {
        if (snap.val()) {
          resolve(Object.keys(snap.val())[0]);
        } else {
          reject('no key');
        }
      });
    });
  },

  page: function() {
    this.ref.orderByKey().startAt(this.firstKey).limitToFirst(this.pageSize + 1).on('value', (snap) => {
      let cursor;
      let done = false;
      let items = snap.val();
      let keys = Object.keys(items);
      if (keys.length > this.pageSize) {
        if (!keys.find((k) => {k === this.lastKey})) {
          cursor = keys.pop();
          delete items[cursor];
        }
      } else {
        done = true;
      }
      this.count += keys.length;
      this.em.emit('page', {
        value: items,
        cursor: cursor,
        keys: keys
      });
      if (done) {
        this.em.emit('done', true);
      }
      snap = null;
    });
  }
}

module.exports = Pager;
