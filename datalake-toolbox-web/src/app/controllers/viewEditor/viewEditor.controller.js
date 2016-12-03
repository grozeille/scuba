require('./viewEditor.css');

module.exports = {
  controller: ViewEditorController,
  controllerAs: 'viewEditor',
  template: require('./viewEditor.html')
};


/** @ngInject */
function ViewEditorController($timeout, $log, $uibModal, $state, $stateParams, $scope, $rootScope, $window, preparationService, hiveService) {
  var vm = this;
  vm.maxRows = 10000;
  vm.selectedColumn = null;
  vm.selectedDatabase = null;
  vm.selectedTable = null;
  vm.selectedColumnIsCalculated = null;

  vm.name = "";
  vm.description = "";

  vm.renameField = null;
  vm.renameDescription = null;
  vm.changeType = null;

  vm.isLoading = false;
  vm.activeTab = 0;

  vm.tables = [];
  vm.calculatedColumns = [];
  vm.isColumnLinked = { };

  vm.name = preparationService.getName();
  vm.description = preparationService.getComment();

  vm.queryGroup = preparationService.getFilter();
  vm.queryFields = [ ];

  function htmlEntities(str) {
    return String(str).replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  function computed(group) {
    if (!group) return "";
    if(!group.rules) return "";

    for (var str = "(", i = 0; i < group.rules.length; i++) {
      i > 0 && (str += " <strong>" + group.operator + "</strong> ");

      if(group.rules[i].group){
        str += computed(group.rules[i].group);
      }
      else {
        if(group.rules[i].condition.localeCompare('IS NULL') == 0 || group.rules[i].condition.localeCompare('IS NOT NULL') == 0){
          str += group.rules[i].field.name + " " + htmlEntities(group.rules[i].condition);
        }
        else if(group.rules[i].condition.localeCompare('IN') == 0 || group.rules[i].condition.localeCompare('NOT IN') == 0){
          str += group.rules[i].field.name + " " + htmlEntities(group.rules[i].condition) + " [" + group.rules[i].data+"]";
        }
        else {
          str += group.rules[i].field.name + " " + htmlEntities(group.rules[i].condition) + " " + group.rules[i].data;
        }
      }
    }

    return str + ")";
  }

  vm.computedGroup = function(){
    return "<b>Filter:</b> "+computed(vm.queryGroup);
  }

  preparationService.subscribeOnChange($scope, function(){
    vm.refreshTables();
    $log.info('Refreshed after changes at: ' + new Date());
  });

  vm.save = function(){
    preparationService.setName(vm.name);
    preparationService.setComment(vm.description);
    preparationService.setFilter(vm.queryGroup);
    preparationService.saveView();
    $log.info('Save at: ' + new Date());

    var modalInstance = $uibModal.open({
      templateUrl: 'saved.html',
      controllerAs: "save",
      controller: function($uibModalInstance, viewName){
        var vm = this;
        vm.viewName = viewName;
        vm.ok = function(){
          $uibModalInstance.close();
        };
      },
      resolve: {
        viewName: function () {
          return vm.name;
        }
      }
    });

  };

  vm.refreshTables = function(){
    vm.selectedColumn = null;
    vm.selectedDatabase = null;
    vm.selectedTable = null;
    vm.selectedColumnIsCalculated = null;
    vm.queryFields = [];

    vm.tables = preparationService.getTables();
    vm.calculatedColumns = preparationService.getCalculatedColumns();
    var links = preparationService.getLinks();
    vm.isColumnLinked = { };
    for(var l = 0; l < links.length; l++){
      var link = links[l];
      vm.isColumnLinked[link.left.database+"."+link.left.table+"."+link.left.column] = true;
      //vm.isColumnLinked[link.right.database+"."+link.right.table+"."+link.right.column] = true;
    }

    var columnDefs = [];

    for(var t = 0; t < vm.tables.length; t++){
      var table = vm.tables[t];

      for(var c = 0; c < table.columns.length; c++){
        var column = table.columns[c];

        vm.queryFields.push({
          name: table.database+"."+table.table+"."+column.newName,
          groupName: table.database+"."+table.table,
          database: table.database,
          table: table.table,
          column: column.name
        });

        if(column.selected) {
          columnDefs.push({
            field: column.newName.toLowerCase(),
            enableHiding: false,
            minWidth: 70,
            width: 100,
            headerCellTemplate: 'app/viewEditor/header-cell-template.html',
            hive: {
              database: table.database,
              table: table.table,
              column: column
            }
          });
        }
      }
    }

    for(var c = 0; c < vm.calculatedColumns.length; c++){
      var column = vm.calculatedColumns[c];

      columnDefs.push({
        field: column.newName.toLowerCase(),
        enableHiding: false,
        minWidth: 70,
        width: 100,
        enableColumnResizing: true,
        headerCellTemplate: 'app/viewEditor/header-cell-template.html',
        hive: {
          database: "",
          table: "",
          column: column
        }
      });
    }



    vm.gridSampleOptions = {
      enableSorting: false,
      enableColumnMenus: false,
      enableColumnResizing: true,
      appScopeProvider: vm,
      columnDefs: columnDefs,
      data : [ ],
      onRegisterApi: function( gridApi ) {
        vm.gridSampleApi = gridApi;
      }
    };
  };

  vm.removeTable = function(table){
    var modalInstance = $uibModal.open({
      templateUrl: 'deleteTable.html',
      controllerAs: "deleteTable",
      controller: function($timeout, $log, $location, $filter, $uibModalInstance, $state, database, table, preparationService) {
        var vm = this;
        vm.table = preparationService.getTable(database, table);

        vm.ok = function(){
          $log.info('Modal OK at: ' + new Date());

          preparationService.removeTable(vm.table.database, vm.table.table);

          $uibModalInstance.close();
        };

        vm.cancel = function(){
          $log.info('Modal dismissed at: ' + new Date());
          $uibModalInstance.dismiss();
        };
      },
      resolve: {
        database: function () {
          return table.database;
        },
        table: function() {
          return table.table;
        }
      }
    });
  };

  vm.getData = function(){
    vm.isLoading = true;
    return preparationService.getData(vm.maxRows).then(function(data){
      if(data != null){
        vm.gridSampleOptions.data = data.data;
      }
      vm.isLoading = false;
    }).catch(function(error) {
      vm.isLoading = false;
    });
  };

  vm.cancelGetData = function(){
    preparationService.cancelGetData("user cancel");
  };

  vm.rename = function(){
    if(angular.isDefined(vm.selectedColumn)){
      vm.selectedColumn.newName = vm.renameField;
      vm.selectedColumn.newDescription = vm.renameDescription;
      vm.selectedColumn.newType = vm.changeType;

      vm.refreshTables();
    }
  };

  vm.makeTablePrimary = function(database, table){
    preparationService.makeTablePrimary(database, table);
  };

  vm.isColumnSelected = function(col) {
    return vm.selectedColumn === col.colDef.hive.column &&
      vm.selectedDatabase === col.colDef.hive.database &&
      vm.selectedTable === col.colDef.hive.table;
  };

  vm.selectColumn = function(col){
    // if it's the same, unselect it
    if( angular.isDefined(vm.selectedColumn) && vm.selectedColumn != null &&
        col.colDef.hive.column.name.localeCompare(vm.selectedColumn.name) == 0 &&
        col.colDef.hive.database.localeCompare(vm.selectedDatabase) == 0 &&
        col.colDef.hive.table.localeCompare(vm.selectedTable) == 0) {

      vm.unSelectColumn();
      return;
    };

    vm.selectedColumn = col.colDef.hive.column;
    vm.selectedDatabase = col.colDef.hive.database;
    vm.selectedTable = col.colDef.hive.table;
    vm.selectedColumnIsCalculated = col.colDef.hive.column.isCalculated;

    if(angular.isDefined(vm.selectedColumn)  && vm.selectedColumn != null){
      vm.renameField = vm.selectedColumn.newName;
      vm.renameDescription = vm.selectedColumn.newDescription;
      vm.changeType = vm.selectedColumn.newType;
    }

    if(vm.selectedColumnIsCalculated){
      vm.activeTab = 3;
    }
    else {
      vm.activeTab = 2;
    }

  };

  vm.unSelectColumn = function(){
    vm.selectedColumn = null;
    vm.selectedDatabase = null;
    vm.selectedTable = null;
    vm.selectedColumnIsCalculated = null;

    vm.activeTab = 0;
  };

  vm.onColumnSelectionChange = function(column) {
    preparationService.notifyOnChange();
  }


  vm.createCalculated = function(){
    if(angular.isDefined(vm.selectedColumn)  && vm.selectedColumn != null){
      var calculatedColumn = {
        name: "calculated_"+preparationService.getNextCalculatedColumnSequence(),
        newName: vm.selectedColumn.newName+" calculated",
        newDescription: "",
        formula: "`"+vm.selectedColumn.newName+"`",
        isCalculated: true
      };
      preparationService.addCalculatedColumn(calculatedColumn);


      // TODO select the new column
      //vm.gridSampleOptions.columnDefs
      //vm.selectColumn
    }
  };

  vm.removeCalculatedColumn = function($event, column) {

    $event.preventDefault();

    var modalInstance = $uibModal.open({
      templateUrl: 'deleteCalculated.html',
      controllerAs: "deleteCalculated",
      controller: function($timeout, $log, $location, $filter, $uibModalInstance, $state, columnName, preparationService) {
        var vm = this;
        vm.column = preparationService.getCalculatedColumn(columnName);

        vm.ok = function(){
          $log.info('Modal OK at: ' + new Date());

          preparationService.removeCalculatedColumn(vm.column.name);

          $uibModalInstance.close();
        };

        vm.cancel = function(){
          $log.info('Modal dismissed at: ' + new Date());
          $uibModalInstance.dismiss();
        };
      },
      resolve: {
        columnName: function() {
          return column.name;
        }
      }
    });
  };

  function activate(){

    vm.refreshTables();
  }

  activate();


};
