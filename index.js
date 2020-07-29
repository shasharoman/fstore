const fs = require('fs');
const _ = require('lodash');

const parse = {
    number: value => Number(value),
    string: value => value.toString()
};

module.exports = class FStore {
    constructor(path, column) {
        this.split = ' ';

        this.path = path;
        this.prevPath = _.isString(path) ? path : path();

        this.column = _.map(column, item => item.split(':'));
        this.parse = _.mapValues(_.fromPairs(this.column), item => parse[item]);
    }

    insert(row) {
        let path = _.isString(this.path) ? this.path : this.path();
        if (this.prevPath !== path) {
            this.prevPath = path;
            this.wStream.close();
            this.wStream = null;
        }
        this.wStream = this.wStream || fs.createWriteStream(path, {
            flags: 'a'
        });

        row = _.map(this.column, item => {
            let [key, type] = item;
            if (type && typeof row[key] !== type) {
                throw new Error(`expect ${key} type to be ${type}, but receive ${typeof row[key]}`);
            }

            return row[key];
        });

        this.wStream.write(_.map(row, encodeURI).join(this.split) + '\n');
    }

    onrow(fn) {
        let rl = require('readline').createInterface({
            input: fs.createReadStream(this.path),
            output: null
        });
        let keys = _.map(this.column, item => item[0]);

        let resumeTimer = null;
        rl.on('line', line => {
            try {
                let row = _.zipObject(keys, line.split(this.split));
                row = _.mapValues(row, (value, key) => this.parse[key] ? this.parse[key](value) : value);
                let delay = fn(row);
                if (delay) {
                    rl.pause();

                    if (resumeTimer) {
                        clearTimeout(resumeTimer);
                    }
                    resumeTimer = setTimeout(() => {
                        rl.resume();
                    }, delay);
                }
            }
            catch (err) {
                logger.error(err);
                rl.close();
            }
        });
        rl.on('close', () => fn(null));
    }
};