var format = function(f) {
    var formatRegExp = /%[sdj%]/g;

    if (typeof f !== 'string') {
        var objects = [];
        for (var i = 0; i < arguments.length; i++) {
            objects.push(inspect(arguments[i]));
        }
        return objects.join(' ');
    }

    var i = 1;
    var args = arguments;
    var len = args.length;
    var str = String(f).replace(formatRegExp, function(x) {
        if (x === '%%') return '%';
        if (i >= len) return x;
        switch (x) {
            case '%s': return String(args[i++]);
            case '%d': return Number(args[i++]);
            case '%j':
                try {
                    return JSON.stringify(args[i++]);
                } catch (_) {
                    return '[Circular]';
                }
            default:
                return x;
        }
    });
    for (var x = args[i]; i < len; x = args[++i]) {
        if (isNull(x) || !isObject(x)) {
            str += ' ' + x;
        } else {
            str += ' ' + inspect(x);
        }
    }
    return str;
};


module.exports = function (connection) {

    var _self = this;

    var ResultBuilder = function (end, wholeData, dataSize) {
        var _self = this;

        var executed = 0;

        this.success = function (kName, parentData) {
            return function (error, data) {
                executed ++;

                if (data && data.length) {
                    if (!Array.isArray(parentData[kName])) {
                        parentData [kName] = data.length == 1 ? data[0] : data;
                    } else {
                        if (!Array.isArray(parentData [kName])) {
                            parentData[kName] = [parentData[kName]];
                        }

                        Array.prototype.push.apply(parentData[kName], data);
                    }
                }

                if (dataSize == executed) {
                    end ({}, wholeData);
                }
            }
        }

    }

    _self.insert = function (json, table, success) {
        if (Array.isArray(json)) {
            if (!json.length) {
                success();
                return;
            }

            var resultBuilder = new ResultBuilder(success, {}, json.length);

            for (var i = 0; i < json.length; i++){
                _self.insert (json[i], table, resultBuilder.success({}, ''));
            }
        } else {
            var sql = 'INSERT INTO %s (%s) VALUES (%s)';
            var columns = '';
            var values = '';
            var objects = [];

            var keys = Object.keys(json);

            for (var jKey in keys) {
                if (typeof json[keys[jKey]] == 'object') {
                    objects.push({
                        table: keys[jKey],
                        json: json[keys[jKey]]
                    });
                } else {
                    columns += '`' + keys[jKey] + '`, ';
                    values += '"' + json[keys[jKey]] + '", ';
                }
            }

            connection.query({
                sql: format (
                    sql,
                    table,
                    columns.substring (0, columns.length - 2),
                    values.substring (0, values.length - 2)
                ),
                timeout: 40000,
                values: []
            }, function (error, results, fields) {
                if (!objects.length) {
                    success();
                } else {
                    var resultBuilder = new ResultBuilder(success, {}, objects.length);

                    for (var i = 0; i < objects.length; i++) {
                        if (Array.isArray(objects[i].json)) {
                            for (var j = 0; j < objects[i].json.length; j++) {
                                objects[i].json[j][table + '_id'] = results.insertId;
                            }
                        } else {
                            objects[i].json[table + '_id'] = results.insertId;
                        }

                        _self.insert(objects[i].json, objects[i].table, resultBuilder.success('', {}));
                    }
                }
            });
        }
    }

    _self.select = function (matching, map, table, callback) {
        var sql = 'SELECT * FROM %s WHERE %s';
        var conditions = '';

        var mKeys = Object.keys(matching);

        for (var mKey in mKeys) {
            conditions += mKeys[mKey] + ' = ' + matching[mKeys[mKey]] + ', ';
        }

        conditions = conditions.substr(0, conditions.length - 2);

        connection.query(format(
            sql,
            table,
            conditions.length ? conditions : '1'
        ), function(err, rows, fields) {
            var mKeys = Object.keys(map);

            if (!mKeys.length || !rows) {
                callback (err, rows, fields);
                return;
            }

            var resultBuilder = new ResultBuilder(callback, rows, rows.length * mKeys.length);

            for (var i = 0; i < rows.length; i++) {
                for (var mKey in mKeys) {
                    var _map = {};

                    if (map[mKeys[mKey]].matching) {
                        var matchingKeys = Object.keys (map[mKeys[mKey]].matching);

                        for (var matchingKey in matchingKeys) {
                            _map[matchingKeys [matchingKey]] = rows [i][map[mKeys[mKey]].matching [matchingKeys [matchingKey]]];
                        }
                    }

                    _self.select(
                        _map ? _map : {},
                        map[mKeys[mKey]].map ? map[mKeys[mKey]].map : {},
                        mKeys[mKey],
                        resultBuilder.success(mKeys[mKey], rows [i])
                    )
                }
            }
        });
    }

}