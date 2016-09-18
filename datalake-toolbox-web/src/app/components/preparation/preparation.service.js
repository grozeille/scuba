(function() {
  'use strict';

  angular
    .module('datalakeToolbox')
    .factory('preparationService', preparationService);

  /** @ngInject */
  function preparationService($log, $http, $location, $filter, $q, $rootScope, hiveService) {

    var vm = this;
    vm.apiHost = $location.protocol() +"://"+$location.host() +":"+$location.port()+"/api";
    //vm.apiHost = "http://localhost:8000/api";

    vm.id = "";
    vm.name = "";
    vm.comment = "";
    vm.tables = { };
    vm.calculatedColumns = [ ];
    vm.links = [ ];
    vm.filter = {"operator": "AND","rules": []};

    vm.cancelCurrentGetData = null;

    vm.getServiceData = function(response) {
      return response.data;
    };

    vm.catchServiceException = function(error) {
      $log.error('XHR Failed for getContributors.\n' + angular.toJson(error.data, true));
    };

    function getViews(){
      return $http.get(vm.apiHost + '/dataset')
              .then(vm.getServiceData)
              .catch(vm.catchServiceException);
    }

    function loadView(id){

      if(angular.isUndefined(id)){
        // load new view
        vm.id = "";
        vm.name = "";
        vm.comment = "";
        vm.tables = { };
        vm.calculatedColumns = [ ];
        vm.links = [ ];
        vm.filter = {"operator": "AND","rules": []};

        notifyOnChange();

        return $q.when(null);
      }
      else {

        vm.id = id;

        return $http.get(vm.apiHost + '/dataset/'+id)
          .then(vm.getServiceData)
          .then(function(data){
            var loadedView = data;
            vm.tables = [];
            for(var t=0; t < loadedView.tables.length; t++){
              var table = loadedView.tables[t];
              vm.tables[table.database+'.'+table.table] = table;
            }
            vm.calculatedColumns = loadedView.calculatedColumns;
            vm.links = loadedView.links;
            vm.comment = loadedView.comment;
            vm.name = loadedView.table;
            vm.filter = parseDataSetFilterGroup(loadedView.filter);

            notifyOnChange();

            return data;
          })
          .catch(vm.catchServiceException);

      }
    }

    function cloneView(id){

      return $http.get(vm.apiHost + '/dataset/'+id)
          .then(vm.getServiceData)
          .then(function(data){
            var loadedView = data;
            vm.tables = [];
            for(var t=0; t < loadedView.tables.length; t++){
              addTable(loadedView.tables[t]);
            }
            vm.calculatedColumns = loadedView.calculatedColumns;
            vm.links = loadedView.links;
            vm.comment = loadedView.comment;
            vm.filter = parseDataSetFilterGroup(loadedView.filter);
            vm.name = loadedView.table + " (cloned)";
            vm.id = "";

            notifyOnChange();

            return data;
          })
          .catch(vm.catchServiceException);
    }

    function parseDataSetFilterGroup(filter){
      var group = {
        operator: filter.operator,
        rules: []
      };

      if(filter.conditions){
        for(var c = 0; c < filter.conditions.length; c++){
          var rule = filter.conditions[c];

          var parsedRule = {
            condition : rule.condition,
            data: rule.data,
            field: {
              name: rule.database+"."+rule.table+"."+rule.column,
              groupName: rule.database+"."+rule.table,
              database: rule.database,
              table: rule.table,
              column: rule.column
            }
          };

          parsedRule.field.name = rule.database+"."+rule.table+"."+getColumn(rule.database, rule.table, rule.column).newName;

          group.rules.push(parsedRule);
        }
      }

      if(filter.groups){
          for(var g = 0; g < filter.groups.length; g++){
            group.rules.push({
              group: parseDataSetFilterGroup(filter.groups[g])
            });
          }
      }


      return group;
    }

    function buildDataSetFilterGroup(group){
      var filterGroup = group;

      var dataSetFilter = {
        operator: filterGroup.operator,
        conditions: [],
        groups: []
      };

      for(var r = 0; r < filterGroup.rules.length; r++){
        var rule = filterGroup.rules[r];
        if(angular.isDefined(rule.group)){
          dataSetFilter.groups.push(buildDataSetFilterGroup(rule.group));
        }
        else {
          dataSetFilter.conditions.push({
            condition: rule.condition,
            data: rule.data,
            database: rule.field.database,
            table: rule.field.table,
            column: rule.field.column,
          });
        }
      }

      return dataSetFilter;
    }

    function buildDataSetConf(){

      var dataSetConf = {
        id: vm.id,
        database: "app_aaa",
        table: vm.name,
        path: "/app/aaa/"+vm.name,
        comment: vm.comment,
        dataDomainOwner: "mathias.kluba@gmail.com",
        tags: [ "quotes", "yahoo", "referential"],
        format: "view",
        tables: getTables(),
        calculatedColumns: vm.calculatedColumns,
        links: vm.links,
        filter: buildDataSetFilterGroup(vm.filter)
      };

      return dataSetConf;
    }

    function saveView(){

      var dataSetConf = buildDataSetConf();

      var url = vm.apiHost+ '/dataset';
      if("".localeCompare(vm.id) != 0){
        url = url + '/' + vm.id;
      }

      return $http.put(url, dataSetConf)
                .then(vm.getServiceData)
                .then(function(data){
                  if(angular.isDefined(data.id)){
                    vm.id = data.id;
                  }

                  return data;
                })
                .catch(vm.catchServiceException);
    }

    function deleteView(id){
      return $http.delete(vm.apiHost + '/dataset/' + id)
                .then(vm.getServiceData)
                .then(function(data){
                  vm.id = data.id;
                  return data;
                })
                .catch(vm.catchServiceException);
    }

    function getName(){
      return vm.name;
    }

    function setName(name){
      vm.name = name;
    }

    function getComment(){
      return vm.comment;
    }

    function setComment(comment){
      vm.comment = comment;
    }

    function getFilter(){
        return vm.filter;
      }

    function setFilter(filter){
      vm.filter = filter;
    }

    function addTable(table){
      if(getTables().length == 0){
        table.primary = true;
      }
      else {
        table.primary = false;
      }

      vm.tables[table.database+'.'+table.table] = table;
      for(var i= 0; i < table.columns.length; i++)
      {
        var column = table.columns[i];
        column.selected = true;
        column.newName = column.name;
        column.newType = column.type;
        column.newDescription = column.description;
        column.isCalculated = false;
      }

      notifyOnChange();
    }

    function makeTablePrimary(database, table){
      for (var key in vm.tables) {
        vm.tables[key].primary = false;
      }
      vm.tables[database+"."+table].primary = true;

      notifyOnChange();
    }

    function getPrimaryTable(){
      for (var key in vm.tables) {
        if(vm.tables[key].primary){
          return vm.tables[key];
        }
      }

      return null;
    }

    function getTables(){
      var arrayValues = new Array();

      for (var key in vm.tables) {
        if(vm.tables[key].primary){
          arrayValues.unshift(vm.tables[key]);
        }
        else {
          arrayValues.push(vm.tables[key]);
        }
      }

      return arrayValues;
    }

    function getTable(database, table){
      var key = database+"."+table;

      return vm.tables[key];
    }

    function removeTable(database, table){
      var toDelete = vm.tables[database+'.'+table];
      delete vm.tables[database+'.'+table];
      if(getTables().length == 0){
        vm.calculatedColumns = [ ];
      }
      else if(toDelete.primary) {
        for (var key in vm.tables) {
          vm.tables[key].primary = true;
          break;
        }
      }

      var newLinks = [];
      // remove links related to this table
      for(var l = 0; l < vm.links.length; l++){
        var link = vm.links[l];

        if(!(link.left.database.localeCompare(database) == 0 && link.left.table.localeCompare(table) == 0)) {
          newLinks.push(link);
        }

      }
      vm.links = newLinks;

      notifyOnChange();
    }

    function addCalculatedColumn(calculatedColumn){
      vm.calculatedColumns.push(calculatedColumn);

      notifyOnChange();
    }

    function getCalculatedColumns(){
      return vm.calculatedColumns;
    }

    function removeCalculatedColumn(name){
      for(var i = 0; i < vm.calculatedColumns.length; i++){
        var calculatedColumn = vm.calculatedColumns[i];
        if(calculatedColumn.name.localeCompare(name) == 0){
           vm.calculatedColumns.splice(i, 1);
           break;
        }
      }

      notifyOnChange();
    }

    vm.calculatedColumnSequence = 1;

    function getNextCalculatedColumnSequence(){
      vm.calculatedColumnSequence++;
      return vm.calculatedColumnSequence;
    }

    function getCalculatedColumn(name){
      for(var i = 0; i < vm.calculatedColumns.length; i++){
        var calculatedColumn = vm.calculatedColumns[i];
        if(calculatedColumn.name.localeCompare(name) == 0){
           return calculatedColumn;
        }
      }

      return null;
    }

    function getColumn(database, table, name){
      var tableItem = getTable(database, table);

      if(angular.isUndefined(tableItem)){
        return null;
      }

      for(var i = 0; i < tableItem.columns.length; i++){
        var column = tableItem.columns[i];
        if(column.name.localeCompare(name) == 0){
           return column;
        }
      }

      return null;
    }

    function getLinks(){
      return vm.links;
    }

    function updateLinks(newLinks){
      vm.links = newLinks;
      notifyOnChange();
    }

    function getData(maxRows){

      if(vm.cancelCurrentGetData != null){
        vm.cancelCurrentGetData("new Get Data");
      }
      vm.cancelCurrentGetData = null;

      if(angular.isUndefined(maxRows)){
        maxRows = 10000;
      }

      if(Object.keys(vm.tables).length == 0){
        return $q.when([]);
      }

      var result = hiveService.getData(buildDataSetConf(), maxRows);

      vm.cancelCurrentGetData = result.cancel;
      return result.promise;
    }

    function cancelGetData(){
      if(vm.cancelCurrentGetData != null){
        vm.cancelCurrentGetData("user cancellation");
      }
      vm.cancelCurrentGetData = null;
    };


    function subscribeOnChange(scope, callback) {
        var handler = $rootScope.$on('onChange@preparationService', callback);
        scope.$on('$destroy', handler);
    }

    function notifyOnChange() {
        $rootScope.$emit('onChange@preparationService');
    }

    var service = {
      getViews: getViews,
      loadView: loadView,
      saveView: saveView,
      deleteView: deleteView,
      cloneView: cloneView,
      getName: getName,
      setName: setName,
      getComment: getComment,
      setComment: setComment,
      getFilter: getFilter,
      setFilter: setFilter,
      addTable: addTable,
      getTables: getTables,
      removeTable : removeTable,
      getTable: getTable,
      makeTablePrimary: makeTablePrimary,
      getPrimaryTable: getPrimaryTable,
      getColumn: getColumn,
      getData: getData,
      cancelGetData: cancelGetData,
      addCalculatedColumn: addCalculatedColumn,
      getCalculatedColumns: getCalculatedColumns,
      removeCalculatedColumn: removeCalculatedColumn,
      getCalculatedColumn: getCalculatedColumn,
      getNextCalculatedColumnSequence : getNextCalculatedColumnSequence,
      getLinks: getLinks,
      updateLinks: updateLinks,
      subscribeOnChange: subscribeOnChange,
      notifyOnChange: notifyOnChange
    };

    return service;

  }
})();
