'use strict';

var angular = require('angular');

angular.module('datalakeToolbox')
  .component('datasetControllerComponent', require('./dataset/dataset.controller'))
  .component('datasetEditorControllerComponent', require('./datasetEditor/datasetEditor.controller'))
  .component('datasetEditorLinksControllerComponent', require('./datasetEditor/links/links.controller'))
  .component('chooseTableControllerComponent', require('./chooseTable/chooseTable.controller'))
  .component('catalogControllerComponent', require('./catalog/catalog.controller'))
  .component('tableEditorControllerComponent', require('./tableEditor/tableEditor.controller'));

