var angular = require('angular');

var datalakeToolboxModule = require('./app/index');

require('angular-ui-router');

var routesConfig = require('./routes');

require('bootstrap/dist/css/bootstrap.min.css');
require('./index.css');

angular
  .module('app', [datalakeToolboxModule,
    'ui.router'])
  .config(routesConfig);
