const csvParser = require('csv-parser');
const fs = require('fs');
const moment = require('moment');

const date = '2020-01-17';
let file = `reconnected-${date}.csv`
let data = [];

function reconnectDelaysByCid(data, top) {
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

    let c = 0, filtered = {};
    for (let cid in reconnectDelaysByCid) {
        if (reconnectDelaysByCid[cid].length > top) {
            c++;
            filtered[cid] = data[cid];
        }
    }
    return filtered
}



// let s = 'January 17th 2020, 00:06:53.587';
// let t = s.substr(s.indexOf(',') + 2);
// console.log(moment(t, 'HH:mm:ss.SSS'));
fs.createReadStream(`./${file}`).pipe(csvParser())
    .on('data', (row) => {
        data.push(row);
    })
    .on('end', () => {
        let filtered = reconnectDelaysByCid(data, 300);
        console.log(Object.keys(filtered).length);
    })