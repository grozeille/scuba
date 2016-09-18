(function() {
  'use strict';

  angular
    .module('datalakeToolbox')
    .config(config);

  /** @ngInject */
  function config($logProvider, $stateProvider, $urlRouterProvider, $locationProvider, $uiViewScrollProvider) {
    // Enable log
    $logProvider.debugEnabled(true);

    $uiViewScrollProvider.useAnchorScroll();

  }

})();
