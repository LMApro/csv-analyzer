const csvParser = require('csv-parser');
const fs = require('fs');
const moment = require('moment');

const date = process.argv[2];
let file = `reconnected-${date}.csv`
let data = [];
let dataMap = {};

let debug = false

function findMaxCounts(data, max) {
    let filtered = {};
    for (let i = 0; i < data.length; i++) {
        let record = data[i];
        if (!filtered[record.cid]) {
            filtered[record.cid] = {...record, count: 0};
        }
        filtered[record.cid].count ++

    }

    return filtered;
}

function reconnectDelaysByCid(data) {
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
        if (
            // reconnectDelaysByCid[cid].avg > 10*30 && 
            reconnectDelaysByCid[cid].length <= 5 
        ) {
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
        // let filtered = reconnectDelaysByCid(data);
        // for (let cid in filtered) {
        //     // console.log(dataMap[cid], filtered[cid]);
        // }
        // console.log('Total:', Object.keys(dataMap).length, 'Filtered:', Object.keys(filtered).length, 'Ratio:', (Object.keys(filtered).length*100 / Object.keys(dataMap).length).toFixed(1) + '%');
        // debug && console.log(Object.keys(filtered));

        let filtered = findMaxCounts(data);
        console.log(Object.values(filtered).sort((a,b) => b.count - a.count));
    })