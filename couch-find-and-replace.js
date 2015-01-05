/*

This script performs a bulk find and replace with a preview mode.

You must have already imported all data (see the "import" directory for details) and set up the views used by the search (see the "couchapp" directory for details).

 */
"use strict";

var argv = require('yargs')
    .usage('Usage: node couch-find-and-replace.js --url http://[username:password@]couch-db-host:port/db/ --field FIELDNAME --find REGEXP --replace REGEXP --commit')
    .demand(['url', 'field'])
    .describe('url','The URL for your couchdb instance, including database')
    .describe('field','The field to update.')
    .describe('limit.field','Limit the find by one or more additional FIELD values.  For example, "limit.status=active".')
    .describe('find','The regular expression to look for.  If this is not specified, we will look for records where the value is undefined.')
    .describe('replace','The pattern to replace matches with.  If this is not specified, matching records will have their value for FIELD cleared.')
    .describe('commit','By default, no changes will be made.  You must pass this argument to write changes.')
    .describe('save','If you pass this argument, the list of records will be saved a file in /tmp.')
    .argv;

var field          = argv.field;
var find_regexp    = argv.find ? new RegExp(argv.find, "gi") : "^.*$";
var replace_regexp = argv.replace !== undefined ? argv.replace : "^$";

// We use when.js to throttle our async requests
var when    = require("when");

var request = require('request');

var preview = (argv.commit === undefined);

function getData() {
    var defer = when.defer();

    var searchUrl = argv.url + "/_all_docs?include_docs=true";
    request.get(searchUrl, function(e,r,b) {
        if (r.body) {
            var data = JSON.parse(r.body);
            var docs = data.rows;
            var parsedRows = Object.keys(docs).map(function(key){
                var record = docs[key];
                return record.doc;
            });
            defer.resolve(JSON.stringify({ rows: parsedRows }));
        }
        else {
            defer.reject("No data returned....");
        }
    });

    return defer.promise;
}

// Start with the list of records and then resolve with the individual records
function filterData(dataString) {
    var data            = JSON.parse(dataString);
    var records         = data.rows;
    var filteredRecords = [];

    records.forEach(function(record){
        var includeRecord = false;
        if (record._id.indexOf("_design") === -1) {
            if ((!argv.find && ! record[field]) || (argv.find && record[field] && find_regexp.test(record[field]))) {
                includeRecord = true;
            }
            if (argv.limit) {
                Object.keys(argv.limit).forEach(function(limitField){
                    var value = argv.limit[limitField];
                    if (record[limitField] !== value) {
                        includeRecord = false;
                    }
                });
            }
        }
        if (includeRecord) {
            filteredRecords.push(record);
        }
    });

    return when(JSON.stringify({ rows: filteredRecords }));
}


function updateRecords(dataString) {
    var defer = when.defer();
    var updatedDocs = [];

    var data = JSON.parse(dataString);
    var records = data.rows;
    records.forEach(function(record) {
        var newRecord = JSON.parse(JSON.stringify(record));
        if (!record[field]) {
            newRecord[field] = argv.replace;
        }
        else {
            newRecord[field] = record[field].replace(find_regexp, replace_regexp);
        }

        // If we have an "updated" field and the user is not explicitly setting it to something else, set it to the current date...
        if (newRecord["updated"] && field !== "updated") {
            newRecord["updated"] = (new Date()).toISOString();
        }

        updatedDocs.push(newRecord);
    });

    if (!preview) {
        var updateOptions = {
            url:  argv.url + "/_bulk_docs",
            json: true,
            body: { docs: updatedDocs}
        };

        request.post(updateOptions, function(e, r, b) {
            if (e) {
                console.error(e);
                defer.reject("Error updating record: " + e);
                return;
            }
            if (b)  {
                var errors = [];
                b.forEach(function(row){
                    if (row.reason) {
                        var error = "Error updating record '" + row.reason.current.uniqueId + "':";
                        row.reason.errors.forEach(function(error) {
                            Object.keys(error).forEach(function(field) {
                                error += "\n  '" + field + "': " + error[field];
                            });
                        });
                    }
                });
                if (errors.length > 0) {
                    defer.reject(errors);
                }
                else {
                    defer.resolve(JSON.stringify(b));
                }
            }
        });
    }
    else {
        console.log("Running in preview mode, no changes will be saved...");
        return when(JSON.stringify({ rows: updatedDocs }));
    }
}

function saveMatchesToFile(dataString) {
    if (argv.save) {
        var defer = when.defer();

        var fs = require("fs");
        var timestamp = (new Date()).getTime();

        var filename = "/tmp/" + "couch-find-and-replace-" + timestamp;
        debugger;
        fs.writeFile(filename, JSON.stringify(JSON.parse(dataString),null,2), function(err) {
            if (err) {
                defer.reject(err);
            }
            else {
                console.log("Saved results to file '" + filename + "'...");
                defer.resolve(dataString);
            }
        });

        return defer.promise;
    }
    else {
        return when(dataString);
    }
}

function displayStats(dataString) {
    var data    = JSON.parse(dataString);
    var records = data.rows;

    var condition = argv.find ? "matches '" + argv.find + "'" : "is empty";
    var message = "Found " + records.length + " records whose '" + field + "' " + condition + "...";

    if (argv.limit) {
        message += "\nResults were limited using the following conditions:";
        Object.keys(argv.limit).forEach(function(field){
            var value = argv.limit[field];
            message += "\n\t" + field + " = " + value;
        });
    }

    console.log(message);

    return when(dataString);
}

getData().then(filterData).then(displayStats).then(updateRecords).then(saveMatchesToFile);
