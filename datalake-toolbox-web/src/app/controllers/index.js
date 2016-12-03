'use strict';

var angular = require('angular');

angular.module('datalakeToolbox')
  .component('viewControllerComponent', require('./view/view.controller'))
  .component('viewEditorControllerComponent', require('./viewEditor/viewEditor.controller'))
  .component('viewEditorLinksControllerComponent', require('./viewEditor/links/links.controller'))
  .component('chooseTableControllerComponent', require('./chooseTable/chooseTable.controller'))
  .component('catalogControllerComponent', require('./catalog/catalog.controller'))
  .component('tableEditorControllerComponent', require('./tableEditor/tableEditor.controller'));

