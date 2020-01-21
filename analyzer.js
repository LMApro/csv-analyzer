const csvParser = require('csv-parser');
const fs = require('fs');
const moment = require('moment');

const date = process.argv[2];
let file = `reconnected-${date}.csv`
let data = [];
let dataMap = {};

function compare(sign, item1, item2) {
    if (sign === '=') return item1 === item2;
    if (sign === '<') return item1 < item2;
    if (sign === '>') return item1 > item2;
}

function reconnectDelaysByCid(data, options) {
    let {threshold, times, thresholdSign, timesSign} = options;
    let lastTime = {};
    let reconnectDelaysByCid = data.reduce((accumulate, cur) => {
        let t = moment(cur['@timestamp'].substr(cur['@timestamp'].indexOf(',') + 2), 'HH:mm:ss.SSS');
        if (!accumulate[cur.cid]) {
            accumulate[cur.cid] = [];
        } else {
            accumulate[cur.cid].push(moment(t).diff(lastTime[cur.cid], 'seconds'));
        }
        lastTime[cur.cid] = t;
        return accumulate;
    }, {});

    let filtered = {};

    for (let cid in reconnectDelaysByCid) {
        reconnectDelaysByCid[cid].sum = reconnectDelaysByCid[cid].reduce((acc,current) => {
            return acc + current;
        }, 0);
        reconnectDelaysByCid[cid].avg = reconnectDelaysByCid[cid].sum / reconnectDelaysByCid[cid].length;
        if (compare(thresholdSign, reconnectDelaysByCid[cid].avg, threshold) && compare(timesSign, reconnectDelaysByCid[cid].length, times)) {
            filtered[cid] = reconnectDelaysByCid[cid]
        }
    }

    return filtered
}

fs.createReadStream(`./${file}`).pipe(csvParser())
    .on('data', (row) => {
        dataMap[row.cid] = row;
        data.push(row);
    })
    .on('end', () => {
        let filtered = reconnectDelaysByCid(data, {
            thresholdSign: '<',
            threshold: 5*60,
            timesSign: '>',
            times: 5,
        });
        for (let cid in filtered) {
            console.log(dataMap[cid], filtered[cid]);
        }
    })