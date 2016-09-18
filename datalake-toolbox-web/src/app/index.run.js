(function() {
  'use strict';

  angular
    .module('datalakeToolbox')
    .run(runBlock);

  /** @ngInject */
  function runBlock($log, $rootScope,$location,$stateParams, $anchorScroll) {

    $log.debug('runBlock end');

    $rootScope.$on('$stateChangeSuccess', function(event, toState){
      if($stateParams.scrollTo){
        $location.hash($stateParams.scrollTo);
        $anchorScroll();
      }
    });
  }

})();
