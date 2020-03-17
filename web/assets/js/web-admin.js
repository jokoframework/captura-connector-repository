
function CRWebAdmin() {
    var that = {};
    var columnsLoaded = false;

    var $tree;
    var $columnTypesSelect;

    var CONNECTION_TREE_URL = '/ajax/connectionTree';

    var SAVE_CONNECTION_URL = '/ajax/saveConnection';
    var UPDATE_CONNECTION_URL = '/ajax/updateConnection';

    var SAVE_EXTRACTION_UNIT_URL = '/ajax/saveExtractionUnit';
    var UPDATE_EXTRACTION_UNIT_URL = '/ajax/updateExtractionUnit';

    var TEST_CONNECTION_URL = '/ajax/testConnection';
    var GET_COLUMNS_URL = '/ajax/getColumns';
    var GET_COLUMN_TYPES_URL = 'ajax/getColumnTypes';

    function fillConnectionForm(connection) {
        $('#connection-title').text(connection.id);
        $('#connection-id').val(connection.id);
        $('#connection-url').val(connection.url);
        $('#connection-driver').val(connection.driver);
        $('#connection-user').val(connection.user);
    }

    function fillExtractionUnitForm(selectedNode) {
        $('#eu-columns tbody').empty();

        if (selectedNode.columns) {
            fillColumnList(selectedNode.columns);
        }

        $('#eu-id-connection').val(selectedNode.connectionId);
        $('#eu-id').val(selectedNode.name);
        $('#eu-description').val(selectedNode.description);
        $('#eu-sql').val(selectedNode.sql);
        $('#eu-frequency').val(selectedNode.frequencyInSeconds);
        $('#eu-batch-size').val(selectedNode.insertBatchSize);
        $('#eu-stop-on-error').prop('checked', selectedNode.stopOnError);
        $('#eu-active').prop('checked', selectedNode.active);
    }

    function getExtractionUnitFromForm() {
        var columns = [];

        $.each($('#eu-columns tbody tr'), function(i, el) {
            var rows = $(el).children();
            var column = {};
            column.sourceColumn = $(rows[0]).html();
            column.targetColumn = column.sourceColumn;
            column.pkmember = $(rows[1]).children('[type=checkbox]').prop('checked');
            column.triggerUpdate = $(rows[2]).children('[type=checkbox]').prop('checked');
            column.javaClass = $(rows[3]).val();
            column.length = $(rows[4]).val();
            column.targetType = $(rows[5]).children('select').val();
            console.log(rows[4]);
            columns[columns.length] = column;
        });

        console.log(columns);

        var eu = {
            connectionId: $('#eu-id-connection').val(),
            label: $('#eu-id').val(),
            description: $('#eu-description').val(),
            sql: $('#eu-sql').val(),
            frequencyInSeconds: $('#eu-frequency').val(),
            insertBatchSize: $('#eu-batch-size').val(),
            stopOnError: $('#eu-stop-on-error').prop('checked'),
            active: $('#eu-active').prop('checked'),
            columns: columns
        };

        return eu;
    }

    function getConnectionFromForm() {
        return {
            id: $('#connection-id').val(),
            url: $('#connection-url').val(),
            driver: $('#connection-driver').val(),
            user: $('#connection-user').val(),
            pass: $('#connection-password').val()
        }
    }

    function selectConnection(name) {
        var node = $tree.tree('getNodeByName', name);
        $tree.tree('selectNode', node);
        $('#extraction-unit-form').hide();
        $('#connection-form').show();
        fillConnectionForm(node.connection);
    }

    function selectExtractionUnit(name) {
        var node = $tree.tree('getNodeByName', name);
        $tree.tree('selectNode', node);
        $('#connection-form').hide();
        $('#extraction-unit-form').show();
        fillExtractionUnitForm(node);
    }

    function initTree() {
        $.getJSON(CONNECTION_TREE_URL, function (data) {
            $tree = $('#tree').tree({
                data: data,
                autoOpen: true,
            });

            // select the first connection of the tree
            var name = data[0].label;
            selectConnection(name);
        });

        $('#tree').bind('tree.click', function (event) {
            /*
             * Level 1 is the root node, and a connection node
             * Extraction Units are always children of connection nodes
             */
            var node = event.node;

            if (node) {
                if (node.getLevel() === 1) {
                    $('#extraction-unit-form').hide();
                    $('#connection-form').show();
                    fillConnectionForm(node.connection);
                } else {
                    $('#connection-form').hide();
                    $('#extraction-unit-form').show();
                    columnsLoaded = false;
                    fillExtractionUnitForm(node);
                }
            }

        });
    }

    function sendJsonPostRequest(url, data, onSuccess, onError) {
        var request = $.ajax({
            url: url,
            data: JSON.stringify(data),
            type: 'POST',
            contentType: 'application/json'
        });

        request.done(function (response) {
            alert('Data succesfully sent!');
            if (onSuccess) {
                onSuccess(response);
            }
        });

        request.fail(function (response) {
            if (response.status === 0) {
                alert('Connection Failed. Verify Network.');
            } else {
                alert(response.responseText);
            }
            if (onError) {
                onError(response);
            }
        });
    }

    function reloadTree(onFinished) {
        $tree.tree('loadDataFromUrl', CONNECTION_TREE_URL, null, onFinished);
    }

    function saveConnection() {
        var connection = getConnectionFromForm();

        var onSuccess = function () {
            reloadTree(function () {
                var node = $tree.tree('getNodeById', connection.id);
                $tree.tree('selectNode', node);
                $('#connection-title').text(connection.id);
            });
        };

        sendJsonPostRequest(SAVE_CONNECTION_URL, connection, onSuccess);
    }

    function updateConnection() {
        /* 
         * The old connection is sent just in case the id changed
         * and we need the old id to recognize the object that
         * needs to be changed
         */
        var connection = getConnectionFromForm();
        var selectedNode = $tree.tree('getSelectedNode');
        var oldConnection = selectedNode.connection;
        var data = [oldConnection, connection];

        var onSuccess = function () {
            reloadTree(function () {
                var node = $tree.tree('getNodeById', connection.id);
                $tree.tree('selectNode', node);
                $('#connection-title').text(connection.id);
            });
        };

        sendJsonPostRequest(UPDATE_CONNECTION_URL, data, onSuccess);
    }

    function saveExtractionUnit() {
        var extractionUnit = getExtractionUnitFromForm();

        var onSuccess = function () {
            reloadTree(function () {
                var newNode = $tree.tree('getNodeByName', extractionUnit.label);
                $tree.tree('selectNode', newNode);
            });
        }

        sendJsonPostRequest(SAVE_EXTRACTION_UNIT_URL, extractionUnit, onSuccess);
    }

    function updateExtractionUnit() {
        /* 
         * The old connection is sent just in case the id changed
         * and we need the old id to recognize the object that
         * needs to be changed
         */
        var extractionUnit = getExtractionUnitFromForm();
        var selectedNode = $tree.tree('getSelectedNode');

        var oldExtractionUnit = {
            connectionId: selectedNode.connectionId,
            label: selectedNode.name,
            description: selectedNode.description,
            sql: selectedNode.sql,
            frequencyInSeconds: selectedNode.frequencyInSeconds.toString(),
            insertBatchSize: selectedNode.insertBatchSize.toString(),
            stopOnError: selectedNode.stopOnError,
            active: selectedNode.active,
            columns: selectedNode.columns
        };

        var data = [oldExtractionUnit, extractionUnit];

        var onSuccess = function () {
            reloadTree(function () {
                var newNode = $tree.tree('getNodeByName', extractionUnit.label);
                $tree.tree('selectNode', newNode);
            });
        }

        sendJsonPostRequest(UPDATE_EXTRACTION_UNIT_URL, data, onSuccess);
    }

    function testConnection() {
        var connection = getConnectionFromForm();
        var onSuccess = function (data) {
            alert("Server reachable: " + data);
        }
        sendJsonPostRequest(TEST_CONNECTION_URL, connection, onSuccess);
    }

    function addConnection() {
        var id = 'New Connection';
        var connection = {
            id: id,
            url: '',
            user: '',
            driver: ''
        };

        $tree.tree('appendNode', {
            id: id,
            label: id,
            connection: connection,
            newConnection: true
        });

        selectConnection(id);
    }

    function addExtractionUnit() {
        var root = $tree.tree('getSelectedNode');
        var id = 'NEW EXTRACTION UNIT';

        var extractionUnit = {
            connectionId: root.connection.id,
            sql: '',
            description: '',
            active: '',
            stopOnError: '',
            lookupId: '',
            frequencyInSeconds: '',
            insertBatchSize: '',
            label: id,
            newExtractionUnit: true
        };

        columnsLoaded = false;
        $('#save-eu').attr('disabled', 'disabled');

        $tree.tree('appendNode', extractionUnit, root);
        selectExtractionUnit(id);
    }

    function loadColumns() {
        var eu = getExtractionUnitFromForm();

        var onSuccess = function(response) {
            fillColumnList(response);
        }

        sendJsonPostRequest(GET_COLUMNS_URL, eu, onSuccess);
    }

    function loadColumnTypes() {
        var onSuccess = function(columnTypes) {
            $columnTypesSelect = $('<select></select>');
            for (var i = 0; i < columnTypes.length; i++) {   
                $columnTypesSelect.append('<option value="'+ columnTypes[i] + '">' + columnTypes[i] + '</option>');
            }
        }

        $.getJSON(GET_COLUMN_TYPES_URL, onSuccess);
    }

    function fillColumnList(columnsList) {
        var columnRows = '';
        var select;

        columnsLoaded = true;
        $('#save-eu').removeAttr('disabled');

        for (var i = 0; i < columnsList.length; i++) {
            columnRow = '<tr><td>' + columnsList[i].sourceColumn + '</td>';

            if (columnsList[i].pkmember) {
                columnRow += '<td><input type="checkbox" checked="checked"></td>';
            } else {
                columnRow += '<td><input type="checkbox"></td>';
            }

            if (columnsList[i].triggerUpdate) {
                columnRow += '<td><input type="checkbox" checked="checked"></td>';
            } else {
                columnRow += '<td><input type="checkbox"></td>';
            }

            columnRow += '<input type="hidden" value="'+ columnsList[i].javaClass +'">'
            columnRow += '<input type="hidden" value="'+ columnsList[i].length +'">'
            columnRow += '<td><select>' + $columnTypesSelect.html() + '</select></td></tr>';

            $('#eu-columns').append(columnRow);

            select = $('#eu-columns').find('select')[i];
            console.log($(select));
            $(select).val(columnsList[i].targetType);
        }

    }

    function start() {
        initTree();
        loadColumnTypes();

        $('#save-connection').click(function () {
            var selectedNode = $tree.tree('getSelectedNode');

            if (selectedNode.newConnection) {
                saveConnection();
            } else {
                updateConnection();
            }
        });

        $('#save-eu').click(function () {
            var selectedNode = $tree.tree('getSelectedNode');

            if (selectedNode.newExtractionUnit) {
                saveExtractionUnit();
            } else {
                updateExtractionUnit();
            }
        });

        $('#test-connection').click(function () {
            testConnection();
        });

        $('preview-eu').click(function () {
            console.log('preview clicked');
        });

        $('#add-connection').click(function () {
            addConnection();
        });

        $('#add-eu').click(function () {
            addExtractionUnit();
        });

        $('#eu-sql').blur(function () {
            if (!columnsLoaded) {
                loadColumns();
            }
        });
    }

    that.start = start;
    return that;
};

$(document).ready(function () {
    var admin = CRWebAdmin();
    admin.start();
});
