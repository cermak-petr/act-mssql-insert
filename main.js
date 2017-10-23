const sql = require('mssql');
const Apify = require('apify');
const _ = require('underscore');
const Promise = require('bluebird');

function isString(value){
    return (typeof value === 'string' || value instanceof String);
}

function getAllKeys(results, start, length){
    
    const keys = {};
    function saveKeys(result){
        for(const key in result){
            if(!keys[key]){keys[key] = true;}
        }
    }
    
    const end = Math.min(start + length, results.length);
    for(let i = start; i < end; i++){saveKeys(results[i]);}
    
    return Object.keys(keys);
}

function createInsert(results, start, length, table, staticParam){
    
    const keys = getAllKeys(results, start, length);
    const spKeys = staticParam ? Object.keys(staticParam).join(', ') : null;
    const spValues = staticParam ? Object.values(staticParam).join(', ') : null;
    const keyString = keys.join(', ') + (spKeys ? ', ' + spKeys : '');
    
    let valueStrings = '';
    function addValueString(result){
        valueStrings += valueStrings.length > 0 ? ', (' : '(';
        _.each(keys, function(key, index){
            let val;
            if(result[key]){
                if(typeof result[key] === 'number'){val = result[key];}
                else{val = "'" + result[key].replace(/'/g, "''") + "'";}
                /*if(isString(result[key])){
                    if(result[key].trim().match(/^(\d+\.)?(\d+)$/)){
                        val = result[key].trim();
                    }
                    else{val = "'" + result[key] + "'";}
                }
                else{val = result[key];}*/
            }
            else{val = 'NULL';}
            valueStrings += index > 0 ? (', ' + val) : val;
        });
        if(spValues){valueStrings += ', ' + spValues;}
        valueStrings += ')';
    }
    
    const end = Math.min(start + length, results.length);
    for(let i = start; i < end; i++){
        addValueString(results[i]);
    }
    
    return `INSERT INTO ${table} (${keyString}) VALUES ${valueStrings};`;
}

Apify.main(async () => {
    Apify.setPromisesDependency(Promise);
    const rowSplit = process.env.MULTIROW ? parseInt(process.env.MULTIROW) : 10;
    
    const input = await Apify.getValue('INPUT');
    console.dir(input);
    if(!input.data){
        return console.log('missing "data" attribute in INPUT');
    }
    if(!input._id){
        return console.log('missing "_id" attribute in INPUT');
    }
    const data = input.data ? (typeof input.data === 'string' ? JSON.parse(input.data) : input.data) : {};
    if(!data.connection){
        return console.log('missing "connection" attribute in INPUT.data');
    }
    if(!data.table){
        return console.log('missing "table" attribute in INPUT.data');
    }
    
    Apify.client.setOptions({executionId: input._id});
    
    async function processResults(pool, lastResults){
        const results = _.chain(lastResults.items).pluck('pageFunctionResult').flatten().value();
        for(let i = 0; i < results.length; i += rowSplit){
            const insert = createInsert(results, i, rowSplit, data.table, data.staticParam);
            try{
                const records = await pool.request().query(insert);
                console.dir(records);
            }
            catch(e){console.log(e);}
        }
    }
    
    try{
        const pool = await sql.connect(data.connection);
        
        const limit = 15000;
        let total = -1, offset = 0;
        while(total === -1 || offset + limit <= total){
            const lastResults = await Apify.client.crawlers.getExecutionResults({limit: limit, offset: offset});
            await processResults(pool, lastResults);
            total = lastResults.total;
            offset += limit;
        }
    }
    catch(e){console.log(e);}
});
