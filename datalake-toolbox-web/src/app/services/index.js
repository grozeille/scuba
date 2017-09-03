'use strict';

var angular = require('angular');

angular.module('datalakeToolbox')
  .service('hiveService', require('./hive/hive.service'))
  .service('preparationService', require('./preparation/preparation.service'))
  .service('prepareTableService', require('./prepareTable/prepareTable.service'))
  .service('adminService', require('./admin/admin.service'));
