const csvParser = require('csv-parser');
const fs = require('fs');
const moment = require('moment');

const date = process.argv[2];
let file = `reconnected-${date}.csv`
let data = [];

function reconnectDelaysByCid(data, threshold, times) {
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
        if (reconnectDelaysByCid[cid].avg < threshold && reconnectDelaysByCid[cid].length > times) {
            filtered[cid] = reconnectDelaysByCid[cid]
        }
    }

    return filtered
}

fs.createReadStream(`./${file}`).pipe(csvParser())
    .on('data', (row) => {
        data.push(row);
    })
    .on('end', () => {
        let filtered = reconnectDelaysByCid(data, 5*60, 5);
        console.log(Object.keys(filtered));
        console.log(Object.keys(filtered).length);
    })