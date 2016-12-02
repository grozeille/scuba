var angular = require('angular');

//require('todomvc-app-css/index.css');

/*var todos = require('./app/todos/todos');
var App = require('./app/containers/App');
var Header = require('./app/components/Header');
var MainSection = require('./app/components/MainSection');
var TodoTextInput = require('./app/components/TodoTextInput');
var TodoItem = require('./app/components/TodoItem');
var Footer = require('./app/components/Footer');
*/

/*var hiveService = require('./app/services/hive/hive.service.js')
var preparationService = require('./app/services/preparation/preparation.service.js')
var prepareTableService = require('./app/services/prepareTable/prepareTable.service.js')

var viewController = require('./app/controllers/view/view.controller.js')
var viewEditorController = require('./app/controllers/viewEditor/viewEditor.controller.js')
var linksController = require('./app/controllers/viewEditor/links/links.controller.js')
var catalogController = require('./app/controllers/catalog/catalog.controller.js')
var tableEditorController = require('./app/controllers/tableEditor/tableEditor.controller.js')
var chooseTableController = require('./app/controllers/chooseTable/chooseTable.controller.js')

var fileread = require('./app/components/fileread/fileread.directive.js')
var fillHeight = require('./app/components/fillHeight/fillHeight.directive.js')
var queryBuilder = require('./app/components/queryBuilder/queryBuilder.directive.js')*/

var datalakeToolboxModule = require('./app/index');

require('angular-ui-router');

var routesConfig = require('./routes');

require('bootstrap/dist/css/bootstrap.min.css');
require('./index.css');

angular
  .module('app', [datalakeToolboxModule,
    'ui.router'])
  .config(routesConfig)
  /*.service('HiveService', hiveService)
  .service('PreparationService', preparationService)
  .service('PrepareTableService', prepareTableService)
  .component('ViewController', viewController)
  .component('ViewEditorController', viewEditorController)
  .component('LinksController', linksController)
  .component('CatalogController', catalogController)
  .component('TableEditorController', tableEditorController)
  .component('ChooseTableController', chooseTableController)
  .component('fileread', fileread)
  .component('fillHeight', fillHeight)
  .component('queryBuilder', queryBuilder)*/;
