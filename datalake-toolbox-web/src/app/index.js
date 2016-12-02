var angular = require('angular');

var hiveService = require('./services/hive/hive.service');
var preparationService = require('./services/preparation/preparation.service');
var prepareTableService = require('./services/prepareTable/prepareTable.service');

var viewController = require('./controllers/view/view.controller');
var viewEditorController = require('./controllers/viewEditor/viewEditor.controller');
var viewEditorLinksController = require('./controllers/viewEditor/links/links.controller');
var catalogController = require('./controllers/catalog/catalog.controller');
var chooseTableController = require('./controllers/chooseTable/chooseTable.controller');
var tableEditorController = require('./controllers/tableEditor/tableEditor.controller');

var datalakeToolboxModule = 'datalakeToolbox';

module.exports = datalakeToolboxModule;


require('angular-ui-bootstrap');
require('angular-ui-grid');
//require('angular-ui-select');
require('ng-file-upload');
require('ng-tags-input');

require('angular-ui-router');
require('angular-ui-router-uib-modal');

angular
  .module(datalakeToolboxModule, [
    'ui.router',
    'ui.bootstrap',
    'ui.bootstrap.modal',
    'ui.router.modal',])
  .service('hiveService', hiveService)
  .service('preparationService', preparationService)
  .service('prepareTableService', prepareTableService)
  .component('viewControllerComponent', viewController)
  .component('viewEditorControllerComponent', viewEditorController)
  .component('viewEditorLinksControllerComponent', viewEditorLinksController)
  .component('chooseTableControllerComponent', chooseTableController)
  .component('catalogControllerComponent', catalogController)
  .component('tableEditorControllerComponent', tableEditorController);
