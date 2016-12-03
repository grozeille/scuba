'use strict';

var angular = require('angular');

angular.module('datalakeToolbox')
  .directive('queryBuilder', require('./queryBuilder/queryBuilder.directive'));
