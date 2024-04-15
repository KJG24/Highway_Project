const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocument, PutCommand } = require('@aws-sdk/lib-dynamodb');
const fs = require('fs');
const csv = require('csv-parser');
const csvFile = './freeway_detectors.csv'

const client = new DynamoDBClient({ region: 'us-east-2' }); //Will need to update if server is in another region

const docClient = DynamoDBDocument.from(client);



const tableName = 'freeway_data';

fs.createReadStream(csvFile)
    .pipe(csv()) // Initialize the csv-parser without specifying column headers
    .on('data', function(row) {
        const item = {
            detectorid: {S: row['detectorid'].toString()},
            highwayid: {N: parseFloat(row['highwayid'])},
            milepost: {N: parseFloat(row['milepost'])},
            locationtext: {S: row['locationtext'].toString()},
            detectorclass: {N: parseFloat(row['detectorclass'])},
            lanenumber: {N: parseFloat(row['lanenumber'])},
            stationid: {N: parseFloat(row['stationid'])}
        };
        console.log('Row:', item);
        insertData(row);
    })
    .on('error', function(err) {
        // This function is called if there is an error reading the CSV file
        console.error('Error reading CSV file:', err);
    });

async function insertData(row) {
    const command = new PutCommand({
        TableName: tableName,
        Item: {
            detectorid: row['detectorid'].toString(),
            highwayid: parseFloat(row['highwayid']),
            milepost: parseFloat(row['milepost']),
            locationtext: row['locationtext'].toString(),
            detectorclass: parseFloat(row['detectorclass']),
            lanenumber: parseFloat(row['lanenumber']),
            stationid: parseFloat(row['stationid']),
        },
    });
    
    const response = await docClient.send(command);
    console.log(response);
};
