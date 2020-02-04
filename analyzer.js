const csvParser = require('csv-parser');
const fs = require('fs');
const moment = require('moment');

const statistic = require('./statistic-lib');

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

    return Object.values(filtered).sort((a,b) => b.count - a.count).slice(0, max);
}

function getDataByCid(data, cid) {
    return data.filter(item => item.cid == cid).map(i => i['@timestamp'])
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

    return Object.values(reconnectDelaysByCid);
}

function filterByReconnectDelay(data, avg, times) {
    return data.filter(item => {
        return statistic.mean(item) > avg && statistic.count(item) > times
    })
}

fs.createReadStream(`./${file}`).pipe(csvParser())
    .on('data', (row) => {
        dataMap[row.cid] = row;
        data.push(row);
    })
    .on('end', () => {
        let delays = reconnectDelaysByCid(data);
        let filtered = filterByReconnectDelay(delays, 5*30, 10)
        // console.log(getDataByCid(data, '4021beca-ab78-48e2-87e9-dca22e3a73a6'));
        console.log('Total:', Object.keys(dataMap).length, 'Filtered:', filtered.length, 'Ratio:', (filtered.length*100 / Object.keys(dataMap).length).toFixed(1) + '%');
        console.log(filtered);

        // let filtered = findMaxCounts(data, 20);
        // console.log(filtered);
    })