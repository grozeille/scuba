'use strict';

var angular = require('angular');

angular.module('datalakeToolbox')
  .component('datasetControllerComponent', require('./dataset/dataset.controller'))
  .component('datasetEditorControllerComponent', require('./datasetEditor/datasetEditor.controller'))
  .component('datasetEditorLinksControllerComponent', require('./datasetEditor/links/links.controller'))
  .component('chooseTableControllerComponent', require('./chooseTable/chooseTable.controller'))
  .component('catalogControllerComponent', require('./catalog/catalog.controller'))
  .component('customFileDataSetControllerComponent', require('./customFileDataSet/customFileDataSet.controller'))
  .component('adminControllerComponent', require('./admin/admin.controller'))
  .component('profileControllerComponent', require('./profile/profile.controller'))
  .component('setupControllerComponent', require('./setup/setup.controller'))
  .component('projectControllerComponent', require('./project/project.controller'));

