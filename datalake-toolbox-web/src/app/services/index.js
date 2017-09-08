'use strict';

var angular = require('angular');

angular.module('datalakeToolbox')
  .service('hiveService', require('./hive/hive.service'))
  .service('preparationService', require('./preparation/preparation.service'))
  .service('customFileDataSetService', require('./customFileDataSet/customFileDataSet.service'))
  .service('adminService', require('./admin/admin.service'))
  .service('projectService', require('./project/project.service'))
  .service('userService', require('./user/user.service'));
